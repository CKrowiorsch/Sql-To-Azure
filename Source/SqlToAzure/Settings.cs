using System.IO;
using Newtonsoft.Json;

namespace Krowiorsch
{
    public class Settings
    {
        public string Identifier { get; set; }
        public string AzureConnection { get; set; }

        public string AzureTableName { get; set; }

        public string SqlServerConnection { get; set; }
        public string SqlTableName { get; set; }
        public string IdColumn { get; set; }

        public string TimestampColumn { get; set; } = "Timestamp";


        public static Settings ReadFromFile(string fileInfo)
        {
            return JsonConvert.DeserializeObject<Settings>(File.ReadAllText(fileInfo));
        }

    }
}