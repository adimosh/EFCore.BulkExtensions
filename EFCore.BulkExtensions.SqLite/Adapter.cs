﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using EFCore.BulkExtensions.SqlAdapters;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;

namespace EFCore.BulkExtensions.Adapters.Sqlite
{
    public class Adapter : ISqlOperationsAdapter
    {
        public void Register()
        {
            SqlAdaptersMapping.RegisterMapping<Adapter, Dialect>("Sqlite");
        }

        public void Insert<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, Action<decimal> progress)
        {
            var connection = OpenAndGetSqliteConnection(context, tableInfo.BulkConfig);
            bool doExplicitCommit = false;

            try
            {
                if (context.Database.CurrentTransaction == null)
                {
                    //context.Database.UseTransaction(connection.BeginTransaction());
                    doExplicitCommit = true;
                }
                var transaction = (SqliteTransaction)(context.Database.CurrentTransaction == null ?
                    connection.BeginTransaction() :
                    context.Database.CurrentTransaction.GetUnderlyingTransaction(tableInfo.BulkConfig));

                var command = GetSqliteCommand(context, type, entities, tableInfo, connection, transaction);

                type = tableInfo.HasAbstractList ? entities[0].GetType() : type;
                int rowsCopied = 0;
                foreach (var item in entities)
                {
                    LoadSqliteValues(tableInfo, item, command);
                    command.ExecuteNonQuery();
                    ProgressHelper.SetProgress(ref rowsCopied, entities.Count, tableInfo.BulkConfig, progress);
                }
                if (doExplicitCommit)
                {
                    transaction.Commit();
                }
            }
            finally
            {
                context.Database.CloseConnection();
            }
        }

        public async Task InsertAsync<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, Action<decimal> progress,
            CancellationToken cancellationToken)
        {
            var connection = await OpenAndGetSqliteConnectionAsync(context, tableInfo.BulkConfig, cancellationToken).ConfigureAwait(false);
            bool doExplicitCommit = false;

            try
            {
                if (context.Database.CurrentTransaction == null)
                {
                    //context.Database.UseTransaction(connection.BeginTransaction());
                    doExplicitCommit = true;
                }
                var transaction = (SqliteTransaction)(context.Database.CurrentTransaction == null ?
                    connection.BeginTransaction() :
                    context.Database.CurrentTransaction.GetUnderlyingTransaction(tableInfo.BulkConfig));

                var command = GetSqliteCommand(context, type, entities, tableInfo, connection, transaction);

                type = tableInfo.HasAbstractList ? entities[0].GetType() : type;
                int rowsCopied = 0;

                foreach (var item in entities)
                {
                    LoadSqliteValues(tableInfo, item, command);
                    await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                    ProgressHelper.SetProgress(ref rowsCopied, entities.Count, tableInfo.BulkConfig, progress);
                }
                if (doExplicitCommit)
                {
                    transaction.Commit();
                }
            }
            finally
            {
                await context.Database.CloseConnectionAsync().ConfigureAwait(false);
            }
        }

        public void Merge<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, OperationType operationType,
            Action<decimal> progress) where T : class
        {
            var connection = OpenAndGetSqliteConnection(context, tableInfo.BulkConfig);
            bool doExplicitCommit = false;

            try
            {
                if (context.Database.CurrentTransaction == null)
                {
                    //context.Database.UseTransaction(connection.BeginTransaction());
                    doExplicitCommit = true;
                }
                var transaction = (SqliteTransaction)(context.Database.CurrentTransaction == null ?
                                                      connection.BeginTransaction() :
                                                      context.Database.CurrentTransaction.GetUnderlyingTransaction(tableInfo.BulkConfig));

                var command = GetSqliteCommand(context, type, entities, tableInfo, connection, transaction);

                type = tableInfo.HasAbstractList ? entities[0].GetType() : type;
                int rowsCopied = 0;
                foreach (var item in entities)
                {
                    LoadSqliteValues(tableInfo, item, command);
                    command.ExecuteNonQuery();
                    ProgressHelper.SetProgress(ref rowsCopied, entities.Count, tableInfo.BulkConfig, progress);
                }

                if (operationType != OperationType.Delete && tableInfo.BulkConfig.SetOutputIdentity && tableInfo.IdentityColumnName != null)
                {
                    command.CommandText = SqlQueryBuilderSqlite.SelectLastInsertRowId();
                    long lastRowIdScalar = (long)command.ExecuteScalar();
                    string identityPropertyName = tableInfo.IdentityColumnName;
                    var identityPropertyInteger = false;
                    var identityPropertyUnsigned = false;
                    var identityPropertyByte = false;
                    var identityPropertyShort = false;

                    if (tableInfo.FastPropertyDict[identityPropertyName].Property.PropertyType == typeof(ulong))
                    {
                        identityPropertyUnsigned = true;
                    }
                    else if (tableInfo.FastPropertyDict[identityPropertyName].Property.PropertyType == typeof(uint))
                    {
                        identityPropertyInteger = true;
                        identityPropertyUnsigned = true;
                    }
                    else if (tableInfo.FastPropertyDict[identityPropertyName].Property.PropertyType == typeof(int))
                    {
                        identityPropertyInteger = true;
                    }
                    else if (tableInfo.FastPropertyDict[identityPropertyName].Property.PropertyType == typeof(ushort))
                    {
                        identityPropertyShort = true;
                        identityPropertyUnsigned = true;
                    }
                    else if (tableInfo.FastPropertyDict[identityPropertyName].Property.PropertyType == typeof(short))
                    {
                        identityPropertyShort = true;
                    }
                    else if (tableInfo.FastPropertyDict[identityPropertyName].Property.PropertyType == typeof(byte))
                    {
                        identityPropertyByte = true;
                        identityPropertyUnsigned = true;
                    }
                    else if (tableInfo.FastPropertyDict[identityPropertyName].Property.PropertyType == typeof(sbyte))
                    {
                        identityPropertyByte = true;
                    }

                    for (int i = entities.Count - 1; i >= 0; i--)
                    {
                        if (identityPropertyByte)
                        {
                            if (identityPropertyUnsigned)
                                tableInfo.FastPropertyDict[identityPropertyName].Set(entities[i], (byte)lastRowIdScalar);
                            else
                                tableInfo.FastPropertyDict[identityPropertyName].Set(entities[i], (sbyte)lastRowIdScalar);
                        }
                        else if (identityPropertyShort)
                        {
                            if (identityPropertyUnsigned)
                                tableInfo.FastPropertyDict[identityPropertyName].Set(entities[i], (ushort)lastRowIdScalar);
                            else
                                tableInfo.FastPropertyDict[identityPropertyName].Set(entities[i], (short)lastRowIdScalar);
                        }
                        else if (identityPropertyInteger)
                        {
                            if (identityPropertyUnsigned)
                                tableInfo.FastPropertyDict[identityPropertyName].Set(entities[i], (uint)lastRowIdScalar);
                            else
                                tableInfo.FastPropertyDict[identityPropertyName].Set(entities[i], (int)lastRowIdScalar);
                        }
                        else
                        {
                            if (identityPropertyUnsigned)
                                tableInfo.FastPropertyDict[identityPropertyName].Set(entities[i], (ulong)lastRowIdScalar);
                            else
                                tableInfo.FastPropertyDict[identityPropertyName].Set(entities[i], lastRowIdScalar);
                        }

                        lastRowIdScalar--;
                    }
                }
                if (doExplicitCommit)
                {
                    transaction.Commit();
                }
            }
            finally
            {
                context.Database.CloseConnection();
            }
        }

        public async Task MergeAsync<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, OperationType operationType,
            Action<decimal> progress, CancellationToken cancellationToken) where T : class
        {
            var connection = await OpenAndGetSqliteConnectionAsync(context, tableInfo.BulkConfig, cancellationToken).ConfigureAwait(false);
            bool doExplicitCommit = false;

            try
            {
                if (context.Database.CurrentTransaction == null)
                {
                    //context.Database.UseTransaction(connection.BeginTransaction());
                    doExplicitCommit = true;
                }
                var transaction = (SqliteTransaction)(context.Database.CurrentTransaction == null ?
                                                      connection.BeginTransaction() :
                                                      context.Database.CurrentTransaction.GetUnderlyingTransaction(tableInfo.BulkConfig));

                var command = GetSqliteCommand(context, type, entities, tableInfo, connection, transaction);

                type = tableInfo.HasAbstractList ? entities[0].GetType() : type;
                int rowsCopied = 0;

                foreach (var item in entities)
                {
                    LoadSqliteValues(tableInfo, item, command);
                    await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                    ProgressHelper.SetProgress(ref rowsCopied, entities.Count, tableInfo.BulkConfig, progress);
                }

                if (operationType != OperationType.Delete && tableInfo.BulkConfig.SetOutputIdentity && tableInfo.IdentityColumnName != null)
                {
                    command.CommandText = SqlQueryBuilderSqlite.SelectLastInsertRowId();
                    long lastRowIdScalar = (long)await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
                    string identityPropertyName = tableInfo.PropertyColumnNamesDict.SingleOrDefault(a => a.Value == tableInfo.IdentityColumnName).Key;

                    var identityPropertyInteger = false;
                    var identityPropertyUnsigned = false;
                    var identityPropertyByte = false;
                    var identityPropertyShort = false;

                    if (tableInfo.FastPropertyDict[identityPropertyName].Property.PropertyType == typeof(ulong))
                    {
                        identityPropertyUnsigned = true;
                    }
                    else if (tableInfo.FastPropertyDict[identityPropertyName].Property.PropertyType == typeof(uint))
                    {
                        identityPropertyInteger = true;
                        identityPropertyUnsigned = true;
                    }
                    else if (tableInfo.FastPropertyDict[identityPropertyName].Property.PropertyType == typeof(int))
                    {
                        identityPropertyInteger = true;
                    }
                    else if (tableInfo.FastPropertyDict[identityPropertyName].Property.PropertyType == typeof(ushort))
                    {
                        identityPropertyShort = true;
                        identityPropertyUnsigned = true;
                    }
                    else if (tableInfo.FastPropertyDict[identityPropertyName].Property.PropertyType == typeof(short))
                    {
                        identityPropertyShort = true;
                    }
                    else if (tableInfo.FastPropertyDict[identityPropertyName].Property.PropertyType == typeof(byte))
                    {
                        identityPropertyByte = true;
                        identityPropertyUnsigned = true;
                    }
                    else if (tableInfo.FastPropertyDict[identityPropertyName].Property.PropertyType == typeof(sbyte))
                    {
                        identityPropertyByte = true;
                    }

                    for (int i = entities.Count - 1; i >= 0; i--)
                    {
                        if (identityPropertyByte)
                        {
                            if (identityPropertyUnsigned)
                                tableInfo.FastPropertyDict[identityPropertyName].Set(entities[i], (byte)lastRowIdScalar);
                            else
                                tableInfo.FastPropertyDict[identityPropertyName].Set(entities[i], (sbyte)lastRowIdScalar);
                        }
                        else if (identityPropertyShort)
                        {
                            if (identityPropertyUnsigned)
                                tableInfo.FastPropertyDict[identityPropertyName].Set(entities[i], (ushort)lastRowIdScalar);
                            else
                                tableInfo.FastPropertyDict[identityPropertyName].Set(entities[i], (short)lastRowIdScalar);
                        }
                        else if (identityPropertyInteger)
                        {
                            if (identityPropertyUnsigned)
                                tableInfo.FastPropertyDict[identityPropertyName].Set(entities[i], (uint)lastRowIdScalar);
                            else
                                tableInfo.FastPropertyDict[identityPropertyName].Set(entities[i], (int)lastRowIdScalar);
                        }
                        else
                        {
                            if (identityPropertyUnsigned)
                                tableInfo.FastPropertyDict[identityPropertyName].Set(entities[i], (ulong)lastRowIdScalar);
                            else
                                tableInfo.FastPropertyDict[identityPropertyName].Set(entities[i], lastRowIdScalar);
                        }

                        lastRowIdScalar--;
                    }
                }
                if (doExplicitCommit)
                {
                    transaction.Commit();
                }
            }
            finally
            {
                await context.Database.CloseConnectionAsync().ConfigureAwait(false);
            }
        }

        public void Read<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, Action<decimal> progress) where T : class
        {
            throw new NotImplementedException();
        }

        public Task ReadAsync<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, Action<decimal> progress,
            CancellationToken cancellationToken) where T : class
        {
            throw new NotImplementedException();
        }

        public void Truncate(DbContext context, TableInfo tableInfo)
        {
            context.Database.ExecuteSqlRaw(SqlQueryBuilder.DeleteTable(tableInfo.FullTableName));
        }

        public async Task TruncateAsync(DbContext context, TableInfo tableInfo)
        {
            await context.Database.ExecuteSqlRawAsync(SqlQueryBuilder.DeleteTable(tableInfo.FullTableName));
        }

        #region SqliteData
        internal static SqliteCommand GetSqliteCommand<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, SqliteConnection connection, SqliteTransaction transaction)
        {
            SqliteCommand command = connection.CreateCommand();
            command.Transaction = transaction;

            OperationType operationType = tableInfo.BulkConfig.OperationType;

            if (operationType == OperationType.Insert)
            {
                command.CommandText = SqlQueryBuilderSqlite.InsertIntoTable(tableInfo, OperationType.Insert);
            }
            else if (operationType == OperationType.InsertOrUpdate)
            {
                command.CommandText = SqlQueryBuilderSqlite.InsertIntoTable(tableInfo, OperationType.InsertOrUpdate);
            }
            else if (operationType == OperationType.InsertOrUpdateDelete)
            {
                throw new NotSupportedException("Sqlite supports only UPSERT(analog for MERGE WHEN MATCHED) but does not have functionality to do: 'WHEN NOT MATCHED BY SOURCE THEN DELETE'" +
                                                "What can be done is to read all Data, find rows that are not in input List, then with those do the BulkDelete.");
            }
            else if (operationType == OperationType.Update)
            {
                command.CommandText = SqlQueryBuilderSqlite.UpdateSetTable(tableInfo);
            }
            else if (operationType == OperationType.Delete)
            {
                command.CommandText = SqlQueryBuilderSqlite.DeleteFromTable(tableInfo);
            }

            type = tableInfo.HasAbstractList ? entities[0].GetType() : type;
            var entityType = context.Model.FindEntityType(type);
            var entityPropertiesDict = entityType.GetProperties().Where(a => tableInfo.PropertyColumnNamesDict.ContainsKey(a.Name)).ToDictionary(a => a.Name, a => a);
            var properties = type.GetProperties();

            foreach (var property in properties)
            {
                if (entityPropertiesDict.ContainsKey(property.Name))
                {
                    var propertyEntityType = entityPropertiesDict[property.Name];
                    string columnName = propertyEntityType.GetColumnName();
                    var propertyType = Nullable.GetUnderlyingType(property.PropertyType) ?? property.PropertyType;

                    /*var sqliteType = SqliteType.Text; // "String" || "Decimal" || "DateTime"
                    if (propertyType.Name == "Int16" || propertyType.Name == "Int32" || propertyType.Name == "Int64")
                        sqliteType = SqliteType.Integer;
                    if (propertyType.Name == "Float" || propertyType.Name == "Double")
                        sqliteType = SqliteType.Real;
                    if (propertyType.Name == "Guid" )
                        sqliteType = SqliteType.Blob; */

                    var parameter = new SqliteParameter($"@{columnName}", propertyType); // ,sqliteType // ,null
                    command.Parameters.Add(parameter);
                }
            }

            var shadowProperties = tableInfo.ShadowProperties;
            foreach (var shadowProperty in shadowProperties)
            {
                var parameter = new SqliteParameter($"@{shadowProperty}", typeof(string));
                command.Parameters.Add(parameter);
            }

            command.Prepare(); // Not Required (check if same efficiency when removed)
            return command;
        }

        internal static void LoadSqliteValues<T>(TableInfo tableInfo, T entity, SqliteCommand command)
        {
            var PropertyColumnsDict = tableInfo.PropertyColumnNamesDict;
            foreach (var propertyColumn in PropertyColumnsDict)
            {
                object value;
                if (!tableInfo.ShadowProperties.Contains(propertyColumn.Key))
                {
                    if (propertyColumn.Key.Contains(".")) // ToDo: change IF clause to check for NavigationProperties, optimise, integrate with same code segment from LoadData method
                    {
                        var ownedPropertyNameList = propertyColumn.Key.Split('.');
                        var ownedPropertyName = ownedPropertyNameList[0];
                        var subPropertyName = ownedPropertyNameList[1];
                        var ownedFastProperty = tableInfo.FastPropertyDict[ownedPropertyName];
                        var ownedProperty = ownedFastProperty.Property;

                        var propertyType = Nullable.GetUnderlyingType(ownedProperty.GetType()) ?? ownedProperty.GetType();
                        if (!command.Parameters.Contains("@" + propertyColumn.Value))
                        {
                            var parameter = new SqliteParameter($"@{propertyColumn.Value}", propertyType);
                            command.Parameters.Add(parameter);
                        }

                        if (ownedProperty == null)
                        {
                            value = null;
                        }
                        else
                        {
                            var ownedPropertyValue = tableInfo.FastPropertyDict[ownedPropertyName].Get(entity);
                            var subPropertyFullName = $"{ownedPropertyName}_{subPropertyName}";
                            value = tableInfo.FastPropertyDict[subPropertyFullName].Get(ownedPropertyValue);
                        }
                    }
                    else
                    {
                        value = tableInfo.FastPropertyDict[propertyColumn.Key].Get(entity);
                    }
                }
                else // IsShadowProperty
                {
                    value = entity.GetType().Name;
                }

                if (tableInfo.ConvertibleProperties.ContainsKey(propertyColumn.Key) && value != DBNull.Value)
                {
                    value = tableInfo.ConvertibleProperties[propertyColumn.Key].ConvertToProvider.Invoke(value);
                }

                command.Parameters[$"@{propertyColumn.Value}"].Value = value ?? DBNull.Value;
            }
        }

        internal static async Task<SqliteConnection> OpenAndGetSqliteConnectionAsync(DbContext context, BulkConfig bulkConfig, CancellationToken cancellationToken)
        {
            await context.Database.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
            return (SqliteConnection)context.Database.GetDbConnection();
        }

        internal static SqliteConnection OpenAndGetSqliteConnection(DbContext context, BulkConfig bulkConfig)
        {
            context.Database.OpenConnection();

            return (SqliteConnection)context.Database.GetDbConnection();
        }
        #endregion
    }
}
