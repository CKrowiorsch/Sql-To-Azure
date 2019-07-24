using System;
using System.IO;
using Krowiorsch.AzureSqlExporter.Pipeline;
using Serilog;
using Serilog.Sinks.SystemConsole.Themes;

namespace Krowiorsch
{
    internal class Program
    {
        static void Main(string[] args)
        {
            Log.Logger = new LoggerConfiguration()
                .WriteTo.Console(theme: AnsiConsoleTheme.Code)
                .CreateLogger();

            var filePath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "settings.json");

            var settings = Settings.ReadFromFile(filePath);

            var olderThan = DateTime.Today.Subtract(TimeSpan.FromDays(720));

            Console.WriteLine($"Azure: {settings.AzureConnection}");

            var pipeline = new ExportToAzurePipeline(new PipelineSettings
            {
                AzureConnection = settings.AzureConnection,
                AzureTableName = settings.AzureTableName,
                SqlServerConnection = settings.SqlServerConnection,
                IdColumn = settings.IdColumn,
                SqlTableName = settings.SqlTableName,
                TimestampColumn = settings.TimestampColumn
            });
            pipeline.InitializePipeline(settings.AzureTableName, olderThan).Wait();
            pipeline.Execute().Wait();
        }
    }
}
