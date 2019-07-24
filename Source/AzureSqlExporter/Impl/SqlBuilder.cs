namespace Krowiorsch
{
    public static class SqlBuilder
    {
        public static string BuildSelect(string tableName, string timestampColumn = "Timestamp", int batchsize = 500)
        {
            return $"SELECT TOP {batchsize} CONVERT(bigint, {timestampColumn}) as TimestampAsLong,  * FROM {tableName} WHERE {timestampColumn} > Convert(timestamp,Convert(bigint, @cursor)) ORDER BY {timestampColumn} ASC";
        }
    }
}