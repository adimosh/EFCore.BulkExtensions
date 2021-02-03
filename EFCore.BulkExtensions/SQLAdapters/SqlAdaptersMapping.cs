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
            if (string.IsNullOrWhiteSpace(providerName))
            {
                throw new ArgumentException(
                    "The argument must be a valid, non-empty provider name.",
                    nameof(providerName));
            }

            lock (SqlOperationAdapterMapping)
            {
                if (SqlOperationAdapterMapping.ContainsKey(providerName))
                {
                    throw new InvalidOperationException("Provider mapping already registered for this provider.");
                }

                SqlOperationAdapterMapping.Add(
                    providerName,
                    new TAdapter());
                SqlQueryBuilderSpecializationMapping.Add(
                    providerName,
                    new TDialect());
            }
        }

        public static bool TryRegisterMapping<TAdapter, TDialect>(string providerName)
            where TAdapter : ISqlOperationsAdapter, new()
            where TDialect : IQueryBuilderSpecialization, new()
        {
            if (string.IsNullOrWhiteSpace(providerName))
            {
                return false;
            }

            lock (SqlOperationAdapterMapping)
            {
                if (SqlOperationAdapterMapping.ContainsKey(providerName))
                {
                    return false;
                }

                SqlOperationAdapterMapping.Add(
                    providerName,
                    new TAdapter());
                SqlQueryBuilderSpecializationMapping.Add(
                    providerName,
                    new TDialect());
            }

            return true;
        }

        public static void RegisterMapping(string providerName, Type tAdapter, Type tDialect)
        {
            if (string.IsNullOrWhiteSpace(providerName))
            {
                throw new ArgumentException(
                    "The argument must be a valid, non-empty provider name.",
                    nameof(providerName));
            }

            if (tAdapter == null ||
                (tAdapter.IsGenericType && !tAdapter.IsConstructedGenericType) ||
                !typeof(ISqlOperationsAdapter).IsAssignableFrom(tAdapter) ||
                tAdapter.GetConstructor(Array.Empty<Type>()) == null)
            {
                throw new ArgumentException(
                    "The adapter type must not be null or a generic definition, must have an empty constructor and must inherit EFCore.BulkExtensions.SqlAdapters.ISqlOperationsAdapter",
                    nameof(tAdapter));
            }

            if (tDialect == null ||
                (tDialect.IsGenericType && !tDialect.IsConstructedGenericType) ||
                !typeof(IQueryBuilderSpecialization).IsAssignableFrom(tDialect) ||
                tDialect.GetConstructor(Array.Empty<Type>()) == null)
            {
                throw new ArgumentException(
                    "The dialect type must not be null or a generic definition, must have an empty constructor and must inherit EFCore.BulkExtensions.SqlAdapters.IQueryBuilderSpecialization",
                    nameof(tAdapter));
            }

            lock (SqlOperationAdapterMapping)
            {
                if (SqlOperationAdapterMapping.ContainsKey(providerName))
                {
                    throw new InvalidOperationException("Provider mapping already registered for this provider.");
                }

                SqlOperationAdapterMapping.Add(
                    providerName,
                    (ISqlOperationsAdapter)Activator.CreateInstance(tAdapter));
                SqlQueryBuilderSpecializationMapping.Add(
                    providerName,
                    (IQueryBuilderSpecialization)Activator.CreateInstance(tDialect));
            }
        }

        public static bool TryRegisterMapping(string providerName, Type tAdapter, Type tDialect)
        {
            if (string.IsNullOrWhiteSpace(providerName))
            {
                return false;
            }

            if (tAdapter == null ||
                (tAdapter.IsGenericType && !tAdapter.IsConstructedGenericType) ||
                !typeof(ISqlOperationsAdapter).IsAssignableFrom(tAdapter) ||
                tAdapter.GetConstructor(Array.Empty<Type>()) == null)
            {
                return false;
            }

            if (tDialect == null ||
                (tDialect.IsGenericType && !tDialect.IsConstructedGenericType) ||
                !typeof(IQueryBuilderSpecialization).IsAssignableFrom(tDialect) ||
                tDialect.GetConstructor(Array.Empty<Type>()) == null)
            {
                return false;
            }

            lock (SqlOperationAdapterMapping)
            {
                if (SqlOperationAdapterMapping.ContainsKey(providerName))
                {
                    return false;
                }

                SqlOperationAdapterMapping.Add(
                    providerName,
                    (ISqlOperationsAdapter)Activator.CreateInstance(tAdapter));
                SqlQueryBuilderSpecializationMapping.Add(
                    providerName,
                    (IQueryBuilderSpecialization)Activator.CreateInstance(tDialect));
            }

            return true;
        }

        public static void RegisterMappingInstances<TAdapter, TDialect>(string providerName, TAdapter adapterInstance, TDialect dialectInstance)
            where TAdapter : ISqlOperationsAdapter
            where TDialect : IQueryBuilderSpecialization
        {
            if (string.IsNullOrWhiteSpace(providerName))
            {
                throw new ArgumentException(
                    "The argument must be a valid, non-empty provider name.",
                    nameof(providerName));
            }

            if (adapterInstance == null)
            {
                throw new ArgumentNullException(nameof(adapterInstance));
            }

            if (dialectInstance == null)
            {
                throw new ArgumentNullException(nameof(dialectInstance));
            }

            lock (SqlOperationAdapterMapping)
            {
                if (SqlOperationAdapterMapping.ContainsKey(providerName))
                {
                    throw new InvalidOperationException("Provider mapping already registered for this provider.");
                }

                SqlOperationAdapterMapping.Add(
                    providerName,
                    adapterInstance);
                SqlQueryBuilderSpecializationMapping.Add(
                    providerName,
                    dialectInstance);
            }
        }

        public static bool TryRegisterMappingInstances<TAdapter, TDialect>(string providerName, TAdapter adapterInstance, TDialect dialectInstance)
            where TAdapter : ISqlOperationsAdapter
            where TDialect : IQueryBuilderSpecialization
        {
            if (string.IsNullOrWhiteSpace(providerName))
            {
                return false;
            }

            if (adapterInstance == null)
            {
                throw new ArgumentNullException(nameof(adapterInstance));
            }

            if (dialectInstance == null)
            {
                throw new ArgumentNullException(nameof(dialectInstance));
            }

            lock (SqlOperationAdapterMapping)
            {
                if (SqlOperationAdapterMapping.ContainsKey(providerName))
                {
                    return false;
                }

                SqlOperationAdapterMapping.Add(
                    providerName,
                    adapterInstance);
                SqlQueryBuilderSpecializationMapping.Add(
                    providerName,
                    dialectInstance);
            }

            return true;
        }

        public static ISqlOperationsAdapter CreateBulkOperationsAdapter(DbContext context)
        {
            var providerType = GetDatabaseType(context);
            lock (SqlOperationAdapterMapping)
            {
                return SqlOperationAdapterMapping[providerType];
            }
        }

        public static IQueryBuilderSpecialization GetAdapterDialect(DbContext context)
        {
            var providerType = GetDatabaseType(context);
                return GetAdapterDialect(providerType);
        }

        public static IQueryBuilderSpecialization GetAdapterDialect(string providerType)
        {
            lock (SqlOperationAdapterMapping)
            {
                return SqlQueryBuilderSpecializationMapping[providerType];
            }
        }

        public static string GetDatabaseType(DbContext context)
        {
            // TODO: This is a naive check, should probably replace with something better, that recognizes actual providers
            var providerName = context.Database.ProviderName;

            return providerName.Substring(context.Database.ProviderName.LastIndexOf('.') + 1);
        }
    }
}
