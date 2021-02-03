﻿using System.Collections.Generic;
using System.Data;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using EFCore.BulkExtensions.SqlAdapters;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;

namespace EFCore.BulkExtensions.Adapters.Sqlite
{
    public class Dialect : IQueryBuilderSpecialization
    {
        public string DefaultSchema => string.Empty;

        public List<object> ReloadSqlParameters(DbContext context, List<object> sqlParameters)
        {
            var sqlParametersReloaded = new List<object>();
            foreach (var parameter in sqlParameters)
            {
                var sqlParameter = (IDbDataParameter) parameter;
                sqlParametersReloaded.Add(new SqliteParameter(sqlParameter.ParameterName, sqlParameter.Value));
            }

            return sqlParametersReloaded;
        }

        public string GetBinaryExpressionAddOperation(BinaryExpression binaryExpression)
        {
            return IsStringConcat(binaryExpression) ? "||" : "+";
        }

        public (string, string) GetBatchSqlReformatTableAliasAndTopStatement(string sqlQuery)
        {
            return (string.Empty, string.Empty);
        }

        public ExtractedTableAlias GetBatchSqlExtractTableAliasFromQuery(string fullQuery, string tableAlias,
            string tableAliasSuffixAs)
        {
            var result = new ExtractedTableAlias();
            var match = Regex.Match(fullQuery, @"FROM (""[^""]+"")( AS ""[^""]+"")");
            result.TableAlias = match.Groups[1].Value;
            result.TableAliasSuffixAs = match.Groups[2].Value;
            result.Sql = fullQuery.Substring(match.Index + match.Length);

            return result;
        }

        public IDbDataParameter CreateParameter()
        {
            return new SqliteParameter();
        }

        public string WrapAliasName(string aliasName)
        {
            return aliasName;
        }

        public StringBuilder ReplaceAliasBeforeSuffix(StringBuilder columnNames, string aliasName)
        {
            return columnNames.Replace(
                $"[{aliasName}].",
                "");
        }

        internal static bool IsStringConcat(BinaryExpression binaryExpression)
        {
            var methodProperty = binaryExpression.GetType().GetProperty("Method");
            if (methodProperty == null)
            {
                return false;
            }

            var method = methodProperty.GetValue(binaryExpression) as MethodInfo;
            if (method == null)
            {
                return false;
            }

            return method.DeclaringType == typeof(string) && method.Name == nameof(string.Concat);
        }
    }
}