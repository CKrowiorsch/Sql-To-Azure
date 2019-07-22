using System.Linq;
using Microsoft.WindowsAzure.Storage.Table;

namespace Krowiorsch.Pipeline.Transformers
{
    public class PropertySizeTransformer
    {
        const string PostfixString = "-->";         // indicate, that the data was shorten

        const int MaxLength = 32000;

        public void Transform(DynamicTableEntity entity)
        {
            for (var i = 0; i < entity.Properties.Count; i++)
            {
                var prop = entity.Properties.ElementAt(i);
                if (prop.Value.PropertyType == EdmType.String && prop.Value.StringValue.Length > 32000)
                {
                    entity.Properties[prop.Key] = new EntityProperty(ShortenString(prop.Value.StringValue));
                }
            }
        }

        string ShortenString(string input)
        {
            if (input.Length <= MaxLength)
                return input;

            return input.Substring(0, 32000) + PostfixString;
        }
    }
}