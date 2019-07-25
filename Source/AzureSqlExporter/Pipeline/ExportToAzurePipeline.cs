using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Krowiorsch.AzureSqlExporter.Helper;
using Krowiorsch.AzureSqlExporter.Impl;
using Krowiorsch.AzureSqlExporter.Model;
using Krowiorsch.AzureSqlExporter.Pipeline.Transformers;
using Krowiorsch.Impl;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace Krowiorsch.AzureSqlExporter.Pipeline
{
    public class ExportToAzurePipeline
    {
        const int MaxAzureOperations = 100;                     // the limit of the azure tablebatchoperation
        const int SqlBatchSize = 500;

        readonly IRowKeyNormalizer _rowKeyNormalizer = new ReplaceCharacterRowKeyNormalizer();
        readonly string _azureTable;

        readonly IStateStore _stateStore;
        readonly CloudStorageAccount _storageAccount;

        readonly PipelineSettings _settings;

        ImportState _currentState;
        CloudTable _table;

        bool _isInitialized;

        public ExportToAzurePipeline(PipelineSettings settings)
        {
            _stateStore = new AzureBlobStateStore(settings.AzureConnection);
            _storageAccount = CloudStorageAccount.Parse(settings.AzureConnection);
            _azureTable = settings.AzureTableName;

            _settings = settings;
        }

        public async Task InitializePipeline()
        {
            _currentState = await _stateStore.ByIdentifier(_settings.Identifier) ?? new ImportState(_settings.Identifier);

            var client = _storageAccount.CreateCloudTableClient();
            _table = client.GetTableReference(_azureTable);
            await _table.CreateIfNotExistsAsync();

            Serilog.Log.Information("Pipeline for Identifier: {identifier} initialized", _settings.Identifier);

            _isInitialized = true;
        }

        public async Task Execute()
        {
            if (!_isInitialized)
                throw new InvalidOperationException("Pipeline not initialized yet (call InitializePipeline()");

            Serilog.Log.Information("Pipeline for Identifier: {identifier} started", _settings.Identifier);

            var duration = Stopwatch.StartNew();

            Dictionary<string, object>[] databaseObjects;
            long maxTimestamp;
            (databaseObjects, maxTimestamp) = await ReadSqlAndConvert(_currentState).ConfigureAwait(false);
            while (databaseObjects.Any())
            {
                var dynamicObjects = ConvertToDynamic(databaseObjects);         // zu tableentity

                Transform(dynamicObjects);

                await PushToAzure(dynamicObjects).ConfigureAwait(false);

                _currentState.LastProcessedPosition = maxTimestamp;

                await _stateStore.UpdateImportState(_currentState).ConfigureAwait(false);

                Serilog.Log.Information("{count} Entries transferred (Duration: {duration} ms) - TS:{Timestamp}",
                    databaseObjects.Length,
                    duration.ElapsedMilliseconds,
                    _currentState.LastProcessedPosition);

                duration.Restart();
                (databaseObjects, maxTimestamp) = await ReadSqlAndConvert(_currentState).ConfigureAwait(false);
            }

        }

        DynamicTableEntity[] ConvertToDynamic(Dictionary<string, object>[] databaseObjects)
        {
            var resultList = new List<DynamicTableEntity>();

            foreach (var result in databaseObjects)
            {
                var rowKey = _rowKeyNormalizer.ToRowKeyValue(result[_settings.IdColumn].ToString());

                var properties = result
                    .Where(t => !t.Key.Equals("TimestampAsLong", StringComparison.OrdinalIgnoreCase))           // virtual Column not needed
                    .ToDictionary(t => t.Key, t => EntityProperty.CreateEntityPropertyFromObject(t.Value));

                var entity = new DynamicTableEntity("default", rowKey, "*", properties);
                resultList.Add(entity);
            }

            return resultList.ToArray();
        }

        static void Transform(DynamicTableEntity[] entities)
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

        async Task<(Dictionary<string, object>[], long)> ReadSqlAndConvert(ImportState state)
        {
            var statement = SqlBuilder.BuildSelect(_settings.SqlTableName, _settings.TimestampColumn, SqlBatchSize);

            Serilog.Log.Verbose("Start Reading from SqlServer Statement:{statement}", statement);

            Dictionary<string, object>[] resultsDatabase;
            using (var connection = new SqlConnection(_settings.SqlServerConnection))
            {
                resultsDatabase = await connection.QueryAsync(statement, new Dictionary<string, object>
                {
                    {"cursor", state.LastProcessedPosition }
                });
            }

            var maxTimestamp = state.LastProcessedPosition;

            if (resultsDatabase.Any())
                maxTimestamp = resultsDatabase.Select(t => (long)t["TimestampAsLong"]).Max();

            Serilog.Log.Verbose("Result from SqlServer Count:{count} MaxTS:{maxTS}", resultsDatabase.Length, maxTimestamp);

            return (resultsDatabase, maxTimestamp);
        }
    }
}