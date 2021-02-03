using EFCore.BulkExtensions.SqlAdapters;
using Xunit;

namespace EFCore.BulkExtensions.Tests
{
    public class BatchUtilTests
    {
        public BatchUtilTests()
        {
            SqlAdaptersMapping.TryRegisterMapping<Adapters.SqlServer.Adapter, Adapters.SqlServer.Dialect>("SqlServer");
            SqlAdaptersMapping.TryRegisterMapping<Adapters.Sqlite.Adapter, Adapters.Sqlite.Dialect>("Sqlite");
        }

        [Fact]
        public void GetBatchSql_UpdateSqlite_ReturnsExpectedValues()
        {
            ContextUtil.DbServer = "Sqlite";

            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                (string sql, string tableAlias, string tableAliasSufixAs, _, _, _)  = BatchUtil.GetBatchSql(context.Items, context, true);

                Assert.Equal("\"Item\"", tableAlias);
                Assert.Equal(" AS \"i\"", tableAliasSufixAs);
            }
        }
    }
}
