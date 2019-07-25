using System;
using System.IO;
using System.Reactive.Linq;
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

            Console.WriteLine($"Azure: {settings.AzureConnection}");

            var pipeline = new ExportToAzurePipeline(new PipelineSettings
            {
                Identifier = settings.Identifier,
                AzureConnection = settings.AzureConnection,
                AzureTableName = settings.AzureTableName,
                SqlServerConnection = settings.SqlServerConnection,
                IdColumn = settings.IdColumn,
                SqlTableName = settings.SqlTableName,
                TimestampColumn = settings.TimestampColumn
            });
            pipeline.InitializePipeline().Wait();

            var disposable = Observable.Interval(TimeSpan.FromSeconds(30))
                .Subscribe(_ => pipeline.Execute().Wait());

            Console.WriteLine("to cancel press enter");
            Console.ReadLine();

            disposable.Dispose();
        }
    }
}
