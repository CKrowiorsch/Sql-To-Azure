using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data.SqlClient;
using System.Dynamic;
using System.IO;
using System.Linq;
using Dapper;
using Krowiorsch.Impl;
using Krowiorsch.Model;
using Krowiorsch.Pipeline;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using Serilog;
using Serilog.Sinks.SystemConsole.Themes;
using SqlToAzure.Helper;

namespace SqlToAzure
{
    class Program
    {
        static void Main(string[] args)
        {
            Log.Logger = new LoggerConfiguration()
                .WriteTo.Console(theme: AnsiConsoleTheme.Code)
                .CreateLogger();

            var filePath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "settings.json");

            IRowKeyNormalizer rowKeyNormalizer = new ReplaceCharacterRowKeyNormalizer();

            var settings = Settings.ReadFromFile(filePath);

            var olderThan = DateTime.Today.Subtract(TimeSpan.FromDays(720));

            Console.WriteLine("Hello World!");
            Console.WriteLine($"Azure: {settings.AzureConnection}");

            var pipeline = new ExportToAzure(settings);
            pipeline.InitializePipeline("analysetest", olderThan).Wait();
            pipeline.Execute().Wait();
        }
    }
}
