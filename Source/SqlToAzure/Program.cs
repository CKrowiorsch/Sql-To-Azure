using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data.SqlClient;
using System.Dynamic;
using System.IO;
using System.Linq;
using Dapper;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using SqlToAzure.Helper;

namespace SqlToAzure
{
    class Program
    {
        static void Main(string[] args)
        {
            var filePath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "settings.json");

            IRowKeyNormalizer rowKeyNormalizer = new ReplaceCharacterRowKeyNormalizer();

            var settings = Settings.ReadFromFile(filePath);

            var olderThan = DateTime.Today.Subtract(TimeSpan.FromDays(720));

            Console.WriteLine("Hello World!");
            Console.WriteLine($"Azure: {settings.AzureConnection}");

            using (var connection = new SqlConnection(settings.SqlServerConnection))
            {
                var statement = SqlBuilder.BuildSelect(settings.SqlTableName, settings.DateColumn, settings.IdColumn, 10);

                dynamic[] resultsDatabase = connection.Query<dynamic>(statement, new { Date = olderThan }).ToArray();

                Console.Write($"Count:{resultsDatabase.Count()}");

                CloudStorageAccount storageAccount = CloudStorageAccount.Parse(settings.AzureConnection);
                var client = storageAccount.CreateCloudTableClient();
                var table = client.GetTableReference(settings.AzureTableName);
                table.CreateIfNotExistsAsync().Wait();

                TableBatchOperation operations = new TableBatchOperation();

                foreach (var result in resultsDatabase)
                {
                    var resolved = (Dictionary<string, object>)DynamicHelper.ToDictionary(result);

                    var rowKey = rowKeyNormalizer.ToRowKeyValue(resolved[settings.IdColumn].ToString());
                    var entity = new DynamicObjectTableEntity("default", rowKey);

                    foreach (var entry in resolved)
                    {
                        if (entry.Key.Equals(settings.IdColumn, StringComparison.OrdinalIgnoreCase))
                            continue;

                        entity.AddValue(entry.Key, entry.Value);
                    }

                    operations.Add(TableOperation.InsertOrReplace(entity));
                }

                table.ExecuteBatchAsync(operations).Wait();
            }
        }


    }
}
