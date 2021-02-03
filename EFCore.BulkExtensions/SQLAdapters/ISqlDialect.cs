using System.Collections.Generic;
using System.Data;
using System.Linq.Expressions;
using System.Text;
using Microsoft.EntityFrameworkCore;

namespace EFCore.BulkExtensions.SqlAdapters
{
    public class ExtractedTableAlias
    {
        public string TableAlias { get; set; }
        public string TableAliasSuffixAs { get; set; }
        public string Sql { get; set; }
    }
    public interface IQueryBuilderSpecialization
    {
        string DefaultSchema { get; }

        List<object> ReloadSqlParameters(DbContext context, List<object> sqlParameters);

        string GetBinaryExpressionAddOperation(BinaryExpression binaryExpression);

        (string, string) GetBatchSqlReformatTableAliasAndTopStatement(string sqlQuery);

        ExtractedTableAlias GetBatchSqlExtractTableAliasFromQuery(string fullQuery, string tableAlias,
            string tableAliasSuffixAs);

        IDbDataParameter CreateParameter();

        string WrapAliasName(string aliasName);

        StringBuilder ReplaceAliasBeforeSuffix(StringBuilder columnNames, string aliasName);
    }
}
