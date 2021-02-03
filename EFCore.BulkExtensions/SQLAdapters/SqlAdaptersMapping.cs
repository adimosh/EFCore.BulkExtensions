using System;
using System.Collections.Generic;
using Microsoft.EntityFrameworkCore;

namespace EFCore.BulkExtensions.SqlAdapters
{
    public static class SqlAdaptersMapping
    {
        private static readonly Dictionary<string, ISqlOperationsAdapter> SqlOperationAdapterMapping =
            new Dictionary<string, ISqlOperationsAdapter>();

        private static readonly Dictionary<string, IQueryBuilderSpecialization> SqlQueryBuilderSpecializationMapping =
            new Dictionary<string, IQueryBuilderSpecialization>();

        public static void RegisterMapping<TAdapter, TDialect>(string providerName)
            where TAdapter : ISqlOperationsAdapter, new()
            where TDialect : IQueryBuilderSpecialization, new()
        {
            // TODO: add overload
            if (string.IsNullOrWhiteSpace(providerName))
            {
                throw new ArgumentException(
                    "The argument must be a valid, non-empty provider name.",
                    nameof(providerName));
            }
            if (SqlOperationAdapterMapping.ContainsKey(providerName))
            {
                throw new InvalidOperationException("Provider mapping already registered for this provider.");
            }
            SqlOperationAdapterMapping.Add(providerName, new TAdapter());
            SqlQueryBuilderSpecializationMapping.Add(providerName, new TDialect());
        }

        public static ISqlOperationsAdapter CreateBulkOperationsAdapter(DbContext context)
        {
            var providerType = GetDatabaseType(context);
            return SqlOperationAdapterMapping[providerType];
        }

        public static IQueryBuilderSpecialization GetAdapterDialect(DbContext context)
        {
            var providerType = GetDatabaseType(context);
            return GetAdapterDialect(providerType);
        }

        public static IQueryBuilderSpecialization GetAdapterDialect(string providerType)
        {
            return SqlQueryBuilderSpecializationMapping[providerType];
        }

        public static string GetDatabaseType(DbContext context)
        {
            // TODO: This is a naive check, should probably replace with something better, that recognizes actual providers
            var providerName = context.Database.ProviderName;

            return providerName.Substring(context.Database.ProviderName.LastIndexOf('.') + 1);
        }
    }
}
