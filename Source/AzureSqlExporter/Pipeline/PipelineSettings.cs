namespace Krowiorsch.AzureSqlExporter.Pipeline
{
    public class PipelineSettings
    {
        public string AzureConnection { get; set; }

        public string AzureTableName { get; set; }

        public string SqlServerConnection { get; set; }
        public string SqlTableName { get; set; }
        public string IdColumn { get; set; }

        public string TimestampColumn { get; set; } = "Timestamp";
    }
}