namespace Krowiorsch.AzureSqlExporter.Model
{
    public class ImportState
    {
        public ImportState(string identifier)
        {
            Identifier = identifier;
        }

        public string Identifier { get; set; }

        public long LastProcessedPosition { get; set; }
    }
}