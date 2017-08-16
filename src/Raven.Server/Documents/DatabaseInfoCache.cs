﻿using System.Runtime.CompilerServices;
using Raven.Client.Documents.Indexes;
using Raven.Client.ServerWide.Operations;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Voron;
using Voron.Data.Tables;

namespace Raven.Server.Documents
{
    public class DatabaseInfoCache
    {

        protected readonly Logger Logger;

        private StorageEnvironment _environment;

        private TransactionContextPool _contextPool;

        private readonly TableSchema _databaseInfoSchema = new TableSchema();

        public DatabaseInfoCache()
        {
            Logger = LoggingSource.Instance.GetLogger<DatabaseInfoCache>("DatabaseInfoCache");
            _databaseInfoSchema.DefineKey(new TableSchema.SchemaIndexDef
            {
                StartIndex = 0,
                Count = 1
            });
        }

        public void Initialize(StorageEnvironment environment, TransactionContextPool contextPool)
        {
            _environment = environment;
            _contextPool = contextPool;

            using (contextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (var tx = _environment.WriteTransaction(context.PersistentContext))
            {
                _databaseInfoSchema.Create(tx, DatabaseInfoSchema.DatabaseInfoTree, 16);

                tx.Commit();
            }
        }

        public unsafe void InsertDatabaseInfo(DynamicJsonValue databaseInfo, string databaseName)
        {
            using (_contextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (var tx = context.OpenWriteTransaction())
            {
                var table = tx.InnerTransaction.OpenTable(_databaseInfoSchema, DatabaseInfoSchema.DatabaseInfoTree);


                using (var id = context.GetLazyString(databaseName.ToLowerInvariant()))
                {
                    var json = context.ReadObject(databaseInfo, "DatabaseInfo", BlittableJsonDocumentBuilder.UsageMode.ToDisk);
                    try
                    {
                        using (table.Allocate(out TableValueBuilder tvb))
                        {
                            tvb.Add(id.Buffer, id.Size);
                            tvb.Add(json.BasePointer, json.Size);

                            table.Set(tvb);
                        }
                    }
                    finally
                    {
                        json.Dispose(context);
                    }}
                tx.Commit();
            }
        }

        public unsafe bool TryWriteOfflineDatabaseStatusToRequest(TransactionOperationContext ctx, BlittableJsonTextWriter writer, string databaseName, bool disabled, IndexRunningStatus indexingStatus, NodesTopology topology)
        {
            using (_contextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (var tx = context.OpenReadTransaction())
            {
                var table = tx.InnerTransaction.OpenTable(_databaseInfoSchema, DatabaseInfoSchema.DatabaseInfoTree);

                TableValueReader infoTvr;
                using (Slice.From(tx.InnerTransaction.Allocator, databaseName.ToLowerInvariant(), out Slice databaseNameAsSlice))
                {
                    if (table.ReadByKey(databaseNameAsSlice, out infoTvr) == false)
                        return false;
                }
                //It seems like the database was shutdown rudely and never wrote it stats onto the disk
                if (infoTvr.Pointer == null)
                    return false;

                var databaseInfoJson = Read(ref infoTvr);
                try
                {
                    databaseInfoJson.Modifications = new DynamicJsonValue(databaseInfoJson)
                    {
                        [nameof(DatabaseInfo.Disabled)] = disabled,
                        [nameof(DatabaseInfo.IndexingStatus)] = indexingStatus.ToString(),
                        [nameof(DatabaseInfo.NodesTopology)] = topology.ToJson()
                    };

                    ctx.Write(writer, databaseInfoJson);
                    return true;
                }
                finally
                {
                    databaseInfoJson.Dispose(ctx);
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static unsafe BlittableJsonReaderObject Read(ref TableValueReader reader)
        {
            var ptr = reader.Read(DatabaseInfoSchema.DatabaseInfoTable.JsonIndex, out int size);
            return new BlittableJsonReaderObject(ptr, size);
        }

        /// <summary>
        /// This method deletes the database info from the cache when the databse is deleted.
        /// It assumes that the ctx already opened a write transaction.
        /// </summary>
        /// <param name="ctx">A context allocated outside the method with an open write transaction</param>
        /// <param name="databaseName">The database name as a slice</param>
        public void DeleteInternal(TransactionOperationContext ctx, Slice databaseName)
        {
            if (Logger.IsInfoEnabled)
                Logger.Info($"Deleteing database info for '{databaseName}'.");
            var table = ctx.Transaction.InnerTransaction.OpenTable(_databaseInfoSchema, DatabaseInfoSchema.DatabaseInfoTree);
            table.DeleteByKey(databaseName);
        }

        public void Delete(string databaseName)
        {
            using (_contextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            using (var tx = ctx.OpenWriteTransaction())
            using (Slice.From(ctx.Allocator, databaseName.ToLowerInvariant(), out Slice key))
            {
                DeleteInternal(ctx, key);
                tx.Commit();
            }
        }

        public static class DatabaseInfoSchema
        {
            public const string DatabaseInfoTree = "DatabaseInfo";

            public static class DatabaseInfoTable
            {
#pragma warning disable 169
                public const int IdIndex = 0;
                public const int JsonIndex = 1;
#pragma warning restore 169
            }
        }


    }
}
