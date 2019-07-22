using System;
using System.IO;
using Krowiorsch.Pipeline;
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

            var pipeline = new ExportToAzure(settings);
            pipeline.InitializePipeline(settings.AzureTableName, olderThan).Wait();
            pipeline.Execute().Wait();
        }
    }
}
