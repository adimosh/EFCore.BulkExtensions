using System;
using System.Collections.Generic;
using System.Data;
using System.Linq.Expressions;
using System.Text;
using EFCore.BulkExtensions.SqlAdapters;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;

namespace EFCore.BulkExtensions.Adapters.SqlServer
{
    public class Dialect : IQueryBuilderSpecialization
    {
        private static readonly int SelectStatementLength = "SELECT".Length;

        public string DefaultSchema => "dbo";

        public List<object> ReloadSqlParameters(DbContext context, List<object> sqlParameters)
        {
            // if SqlServer, might need to convert
            // Microsoft.Data.SqlClient to System.Data.SqlClient
            var sqlParametersReloaded = new List<object>();
            var c = context.Database.GetDbConnection();
            foreach (var parameter in sqlParameters)
            {
                var sqlParameter = (IDbDataParameter)parameter;
                sqlParametersReloaded.Add(SqlClientHelper.CorrectParameterType(c, sqlParameter));
            }
            return sqlParametersReloaded;
        }

        public string GetBinaryExpressionAddOperation(BinaryExpression binaryExpression)
        {
            return "+";
        }

        public (string, string) GetBatchSqlReformatTableAliasAndTopStatement(string sqlQuery)
        {
            var isSqlServer = true;  // SqlServer : PostrgeSql;
            var escapeSymbolEnd = isSqlServer ? "]" : ".";
            var escapeSymbolStart = isSqlServer ? "[" : " "; // SqlServer : PostrgeSql;
            var tableAliasEnd = sqlQuery.Substring(SelectStatementLength, sqlQuery.IndexOf(escapeSymbolEnd, StringComparison.Ordinal) - SelectStatementLength); // " TOP(10) [table_alias" / " [table_alias" : " table_alias"
            var tableAliasStartIndex = tableAliasEnd.IndexOf(escapeSymbolStart, StringComparison.Ordinal);
            var tableAlias = tableAliasEnd.Substring(tableAliasStartIndex + escapeSymbolStart.Length); // "table_alias"
            var topStatement = tableAliasEnd.Substring(0, tableAliasStartIndex).TrimStart(); // "TOP(10) " / if TOP not present in query this will be a Substring(0,0) == ""
            return (tableAlias, topStatement);
        }

        public ExtractedTableAlias GetBatchSqlExtractTableAliasFromQuery(string fullQuery, string tableAlias,
            string tableAliasSuffixAs)
        {
            return new ExtractedTableAlias
            {
                TableAlias = tableAlias,
                TableAliasSuffixAs = tableAliasSuffixAs,
                Sql = fullQuery
            };
        }

        public IDbDataParameter CreateParameter()
        {
            return new SqlParameter();
        }

        public string WrapAliasName(string aliasName)
        {
            return $"[{aliasName}]";
        }

        public StringBuilder ReplaceAliasBeforeSuffix(StringBuilder columnNames, string aliasName)
        {
            return columnNames;
        }
    }
}
