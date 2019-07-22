﻿using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dapper;
using Krowiorsch.Impl;
using Krowiorsch.Model;
using Krowiorsch.Pipeline.Transformers;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using SqlToAzure;
using SqlToAzure.Helper;

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
        }

        public async Task Execute()
        {
            DynamicTableEntity[] databaseObjects; //= await ReadSqlAndConvert(_currentState);
            long maxTimestamp;
            (databaseObjects, maxTimestamp) = await ReadSqlAndConvert(_currentState);
            while (databaseObjects.Any())
            {
                var duration = Stopwatch.StartNew();
                Transform(databaseObjects);

                await PushToAzure(databaseObjects);

                _currentState.LastProcessedPosition = maxTimestamp;

                await _stateStore.UpdateImportState(_currentState);

                Serilog.Log.Information("{count} Einträge übertragen (Dauer: {duration} ms)", databaseObjects.Length, duration.ElapsedMilliseconds);

                (databaseObjects, maxTimestamp) = await ReadSqlAndConvert(_currentState);
            }

        }

        void Transform(DynamicTableEntity[] entities)
        {
            var transformer = new PropertySizeTransformer();

            foreach (var entity in entities)
            {
                transformer.Transform(entity);

                //for (int i = 0; i < entity.Properties.Count; i++)
                //{
                //    var prop = entity.Properties.ElementAt(i);
                //    if (prop.Value.PropertyType == EdmType.String && prop.Value.StringValue.Length > 32000)
                //    {
                //        entity.Properties[prop.Key] = new EntityProperty(prop.Value.StringValue.Substring(0, 31999));
                //    }
                //}
            }
        }

        async Task PushToAzure(DynamicTableEntity[] entities)
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

                    var t1 = resolved
                        .Where(t => !t.Key.Equals("TimestampAsLong", StringComparison.OrdinalIgnoreCase))
                        .ToDictionary(t => t.Key, t => EntityProperty.CreateEntityPropertyFromObject(t.Value));

                    var entity = new DynamicTableEntity("default", rowKey, "*", t1);
                    resultList.Add(entity);
                }

                return (resultList.ToArray(), maxTimestamp);
            }
        }
    }
}