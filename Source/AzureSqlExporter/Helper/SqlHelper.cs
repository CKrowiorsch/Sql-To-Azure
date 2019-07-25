using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;

namespace Krowiorsch.AzureSqlExporter.Helper
{
    public static class SqlHelper
    {
        static Dictionary<Type, SqlDbType> typeMap;

        // Create and populate the dictionary in the static constructor
        static SqlHelper()
        {
            typeMap = new Dictionary<Type, SqlDbType>();

            typeMap[typeof(string)]         = SqlDbType.NVarChar;
            typeMap[typeof(char[])]         = SqlDbType.NVarChar;
            typeMap[typeof(byte)]           = SqlDbType.TinyInt;
            typeMap[typeof(short)]          = SqlDbType.SmallInt;
            typeMap[typeof(int)]            = SqlDbType.Int;
            typeMap[typeof(long)]           = SqlDbType.BigInt;
            typeMap[typeof(byte[])]         = SqlDbType.Image;
            typeMap[typeof(bool)]           = SqlDbType.Bit;
            typeMap[typeof(DateTime)]       = SqlDbType.DateTime2;
            typeMap[typeof(DateTimeOffset)] = SqlDbType.DateTimeOffset;
            typeMap[typeof(decimal)]        = SqlDbType.Money;
            typeMap[typeof(float)]          = SqlDbType.Real;
            typeMap[typeof(double)]         = SqlDbType.Float;
            typeMap[typeof(TimeSpan)]       = SqlDbType.Time;
        }

        // Non-generic argument-based method
        public static SqlDbType GetDbType(Type giveType)
        {
            // Allow nullable types to be handled
            giveType = Nullable.GetUnderlyingType(giveType) ?? giveType;

            if (typeMap.ContainsKey(giveType))
            {
                return typeMap[giveType];
            }

            throw new ArgumentException($"{giveType.FullName} is not a supported .NET class");
        }

        // Generic version
        public static SqlDbType GetDbType<T>()
        {
            return GetDbType(typeof(T));
        }

        public static async Task<Dictionary<string, object>[]> QueryAsync(this SqlConnection connection, 
            string sqlStatement,
            Dictionary<string, object> parameters)
        {
            if (connection.State != ConnectionState.Open)
                await connection.OpenAsync();

            var results = new List<Dictionary<string, object>>();

            using (var command = connection.CreateCommand())
            {
                command.CommandText = sqlStatement;
                command.CommandTimeout = 600;

                foreach (var parameter in parameters)
                {
                    var p = command.Parameters.Add(parameter.Key, SqlHelper.GetDbType(parameter.Value.GetType()));
                    p.Value = parameter.Value;
                }

                using (var reader = await command.ExecuteReaderAsync())
                {
                    if (!reader.HasRows)
                        return new Dictionary<string, object>[0];

                    var columns = Enumerable.Range(0, reader.FieldCount)
                        .Select(reader.GetName)
                        .ToArray();

                    while (reader.Read())
                    {
                        results.Add(ReadRow(reader, columns));
                    }
                }
            }

            return results.ToArray();
        }

        static Dictionary<string, object> ReadRow(SqlDataReader reader, string[] columns)
        {
            var result = new Dictionary<string, object>();

            foreach (var column in columns)
            {
                result.Add(column, reader.GetValue(reader.GetOrdinal(column)));
            }

            return result;
        }
    }
}