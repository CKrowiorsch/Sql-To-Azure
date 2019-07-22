using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.Linq;
using System.Threading.Tasks;
using Dapper;
using Krowiorsch.Impl;
using Krowiorsch.Model;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using SqlToAzure;
using SqlToAzure.Helper;

namespace Krowiorsch.Pipeline
{
    public class ExportToAzure
    {
        const int MaxAzureOperations = 100;

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
        }

        public async Task Execute()
        {
            DynamicObjectTableEntity[] databaseObjects; //= await ReadSqlAndConvert(_currentState);
            while ((databaseObjects = await ReadSqlAndConvert(_currentState)).Any())
            {
                await PushToAzure(databaseObjects);

                var minTimestamp = databaseObjects.Min(t => t.ProcessCursorPoint);
                _currentState.LastProcessedTimestamp = minTimestamp;

                await _stateStore.UpdateImportState(_currentState);

                Serilog.Log.Information("{count} Einträge übertragen", databaseObjects.Length);
            }

        }

        async Task PushToAzure(DynamicObjectTableEntity[] entities)
        {
            var operations = new TableBatchOperation();

            for (int i = 0; i < entities.Length; i++)
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

        async Task<DynamicObjectTableEntity[]> ReadSqlAndConvert(ImportState state)
        {
            using (var connection = new SqlConnection(_settings.SqlServerConnection))
            {
                var statement = SqlBuilder.BuildSelect(_settings.SqlTableName, _settings.DateColumn, _settings.IdColumn, 500);

                var resultsDatabase = (await connection.QueryAsync<dynamic>(statement, new { Date = state.MaxDate, currentTimestamp = state.LastProcessedTimestamp ?? (DateTime)SqlDateTime.MinValue }, commandTimeout: 600)).ToArray();

                var resultList = new List<DynamicObjectTableEntity>();

                foreach (var result in resultsDatabase)
                {
                    var resolved = (Dictionary<string, object>)DynamicHelper.ToDictionary(result);

                    var rowKey = _rowKeyNormalizer.ToRowKeyValue(resolved[_settings.IdColumn].ToString());
                    var entity = new DynamicObjectTableEntity("default", rowKey);
                    entity.ProcessCursorPoint = (DateTime)resolved[_settings.DateColumn];

                    foreach (var entry in resolved)
                    {
                        if (entry.Key.Equals(_settings.IdColumn, StringComparison.OrdinalIgnoreCase))
                            continue;

                        entity.AddValue(entry.Key, entry.Value);
                    }

                    resultList.Add(entity);
                }

                return resultList.ToArray();
            }
        }
    }
}