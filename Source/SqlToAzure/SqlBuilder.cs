namespace SqlToAzure
{
    public static class SqlBuilder
    {
        public static string BuildSelect(string tableName, string dateColumn, string idColumn, int batchsize)
        {
            return $"SELECT TOP {batchsize} * FROM {tableName} WHERE {dateColumn} < @date ORDER BY {dateColumn} ASC";
        }
    }
}