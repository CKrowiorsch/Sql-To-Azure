namespace SqlToAzure
{
    public interface IRowKeyNormalizer
    {
        string ToRowKeyValue(string originalValue);
        string FromRowKeyValue(string rowKey);

    }

    class Base64RowKeyNormalizer : IRowKeyNormalizer
    {
        public string ToRowKeyValue(string originalValue)
        {
            return Base64Encode(originalValue);
        }

        public string FromRowKeyValue(string rowKey)
        {
            return Base64Decode(rowKey);
        }

        static string Base64Encode(string plainText)
        {
            var plainTextBytes = System.Text.Encoding.UTF8.GetBytes(plainText);
            return System.Convert.ToBase64String(plainTextBytes);
        }

        static string Base64Decode(string plainText)
        {
            var bytes =  System.Convert.FromBase64String(plainText);
            return System.Text.Encoding.UTF8.GetString(bytes);
        }
    }

    class ReplaceCharacterRowKeyNormalizer : IRowKeyNormalizer
    {
        public string ToRowKeyValue(string originalValue)
        {
            return originalValue.Replace('/', '-');
        }

        public string FromRowKeyValue(string rowKey)
        {
            return rowKey.Replace('-', '/');
        }
    }

    
}