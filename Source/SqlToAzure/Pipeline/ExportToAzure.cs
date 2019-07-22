using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Dapper;
using Krowiorsch.Helper;
using Krowiorsch.Impl;
using Krowiorsch.Model;
using Krowiorsch.Pipeline.Transformers;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace Krowiorsch.Pipeline
{
    public class ExportToAzure
    {
        const int MaxAzureOperations = 100;                     // the limit of the azure tablebatchoperation
        const int SqlBatchSize = 500;

        readonly IRowKeyNormalizer _rowKeyNormalizer = new ReplaceCharacterRowKeyNormalizer();

        readonly IStateStore _stateStore;
        readonly Settings _settings;
        readonly CloudStorageAccount _storageAccount;

        ImportState _currentState;
        CloudTable _table;

        public ExportToAzure(Settings settings)
        {
            _settings = settings;
            _stateStore = new AzureBlobStateStore(settings.AzureConnection);
            _storageAccount = CloudStorageAccount.Parse(settings.AzureConnection);
        }

        public async Task InitializePipeline(string identifier, DateTime olderThan)
        {
            _currentState = await _stateStore.ByIdentifier(identifier) ?? new ImportState(identifier, olderThan);

            var client = _storageAccount.CreateCloudTableClient();
            _table = client.GetTableReference(_settings.AzureTableName);
            await _table.CreateIfNotExistsAsync();

            Serilog.Log.Information("Pipeline für Identifier: {identifier} gestartet", identifier);
        }

        public async Task Execute()
        {
            var duration = Stopwatch.StartNew();

            DynamicTableEntity[] databaseObjects; 
            long maxTimestamp;
            (databaseObjects, maxTimestamp) = await ReadSqlAndConvert(_currentState);
            while (databaseObjects.Any())
            {
                Transform(databaseObjects);

                await PushToAzure(databaseObjects);

                _currentState.LastProcessedPosition = maxTimestamp;

                await _stateStore.UpdateImportState(_currentState);

                Serilog.Log.Information("{count} Entries transferred (Duration: {duration} ms) - TS:{Timestamp}", 
                    databaseObjects.Length, 
                    duration.ElapsedMilliseconds, 
                    _currentState.LastProcessedPosition);

                duration.Restart();
                (databaseObjects, maxTimestamp) = await ReadSqlAndConvert(_currentState);
            }

        }

        void Transform(DynamicTableEntity[] entities)
        {
            var transformer = new PropertySizeTransformer();

            foreach (var entity in entities)
            {
                transformer.Transform(entity);
            }
        }

        async Task PushToAzure(DynamicTableEntity[] entities)
        {
            var operations = new TableBatchOperation();

            for (var i = 0; i < entities.Length; i++)
            {
                operations.Add(TableOperation.InsertOrReplace(entities[i]));

                if (i % MaxAzureOperations == MaxAzureOperations - 1)
                {
                    await _table.ExecuteBatchAsync(operations);
                    operations.Clear();
                }
            }

            if (operations.Any())
                await _table.ExecuteBatchAsync(operations);
        }

        async Task<(DynamicTableEntity[], long)> ReadSqlAndConvert(ImportState state)
        {
            using (var connection = new SqlConnection(_settings.SqlServerConnection))
            {
                var statement = SqlBuilder.BuildSelect(_settings.SqlTableName, _settings.TimestampColumn, SqlBatchSize);

                var resultsDatabase = (await connection.QueryAsync<dynamic>(statement, new { cursor = state.LastProcessedPosition }, commandTimeout: 600)).ToArray();

                var resultList = new List<DynamicTableEntity>();

                var maxTimestamp = 0L;

                foreach (var result in resultsDatabase)
                {
                    var resolved = (Dictionary<string, object>)DynamicHelper.ToDictionary(result);

                    var rowKey = _rowKeyNormalizer.ToRowKeyValue(resolved[_settings.IdColumn].ToString());

                    maxTimestamp = Math.Max(maxTimestamp, (long)resolved["TimestampAsLong"]);

                    var properties = resolved
                        .Where(t => !t.Key.Equals("TimestampAsLong", StringComparison.OrdinalIgnoreCase))           // virtual Column not needed
                        .ToDictionary(t => t.Key, t => EntityProperty.CreateEntityPropertyFromObject(t.Value));

                    var entity = new DynamicTableEntity("default", rowKey, "*", properties);
                    resultList.Add(entity);
                }

                return (resultList.ToArray(), maxTimestamp);
            }
        }
    }
}