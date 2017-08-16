﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions.Cluster;
using Raven.Client.Exceptions.Database;
using Raven.Client.Exceptions.Security;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Client.ServerWide.Tcp;
using Raven.Server.Commercial;
using Raven.Server.Json;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Commands.ConnectionStrings;
using Raven.Server.ServerWide.Commands.ETL;
using Raven.Server.ServerWide.Commands.Indexes;
using Raven.Server.ServerWide.Commands.PeriodicBackup;
using Raven.Server.ServerWide.Commands.Subscriptions;
using Raven.Server.ServerWide.Commands.Transformers;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Utils;
using Voron;
using Voron.Data;
using Voron.Data.Tables;
using Voron.Exceptions;
using Voron.Impl;

namespace Raven.Server.ServerWide
{
    public class ClusterStateMachine : RachisStateMachine
    {
        private const string LocalNodeStateTreeName = "LocalNodeState";
        private static readonly TableSchema ItemsSchema;
        private static readonly Slice EtagIndexName;
        private static readonly Slice Items;

        static ClusterStateMachine()
        {
            Slice.From(StorageEnvironment.LabelsContext, "Items", out Items);
            Slice.From(StorageEnvironment.LabelsContext, "EtagIndexName", out EtagIndexName);

            ItemsSchema = new TableSchema();

            // We use the follow format for the items data
            // { lowered key, key, data, etag }
            ItemsSchema.DefineKey(new TableSchema.SchemaIndexDef
            {
                StartIndex = 0,
                Count = 1
            });

            ItemsSchema.DefineFixedSizeIndex(new TableSchema.FixedSizeSchemaIndexDef
            {
                Name = EtagIndexName,
                IsGlobal = true,
                StartIndex = 3
            });
        }

        public event EventHandler<(string DatabaseName, long Index, string Type)> DatabaseChanged;

        public event EventHandler<(string DatabaseName, long Index, string Type)> DatabaseValueChanged;

        public event EventHandler<(long Index, string Type)> ValueChanged;

        protected override void Apply(TransactionOperationContext context, BlittableJsonReaderObject cmd, long index, Leader leader, ServerStore serverStore)
        {
            if (cmd.TryGet("Type", out string type) == false)
            {
                NotifyLeaderAboutError(index, leader, new CommandExecutionException("Cannot execute command, wrong format"));
                return;
            }


            try
            {
                string errorMessage;
                switch (type)
                {
                    //The reason we have a separate case for removing node from database is because we must 
                    //actually delete the database before we notify about changes to the record otherwise we 
                    //don't know that it was us who needed to delete the database.
                    case nameof(RemoveNodeFromDatabaseCommand):
                        RemoveNodeFromDatabase(context, cmd, index, leader);
                        break;

                    case nameof(DeleteValueCommand):
                    case nameof(DeactivateLicenseCommand):
                        DeleteValue(context, type, cmd, index, leader);
                        break;
                    case nameof(IncrementClusterIdentityCommand):
                        if (!ValidatePropertyExistence(cmd,
                            nameof(IncrementClusterIdentityCommand),
                            nameof(IncrementClusterIdentityCommand.Prefix),
                            out errorMessage))
                        {
                            NotifyLeaderAboutError(index, leader,
                                new InvalidDataException(errorMessage));
                            return;
                        }


                        var updatedDatabaseRecord = UpdateDatabase(context, type, cmd, index, leader);

                        cmd.TryGet(nameof(IncrementClusterIdentityCommand.Prefix), out string prefix);
                        Debug.Assert(prefix != null, "since we verified that the property exist, it must not be null");

                        leader?.SetStateOf(index, updatedDatabaseRecord.Identities[prefix]);
                        break;
                    case nameof(UpdateClusterIdentityCommand):
                        if (!ValidatePropertyExistence(cmd,
                            nameof(UpdateClusterIdentityCommand),
                            nameof(UpdateClusterIdentityCommand.Identities),
                            out errorMessage))
                        {
                            NotifyLeaderAboutError(index, leader,
                                new InvalidDataException(errorMessage));
                            return;
                        }
                        UpdateDatabase(context, type, cmd, index, leader);
                        break;
                    case nameof(PutIndexCommand):
                    case nameof(PutAutoIndexCommand):
                    case nameof(DeleteIndexCommand):
                    case nameof(SetIndexLockCommand):
                    case nameof(SetIndexPriorityCommand):
                    case nameof(PutTransformerCommand):
                    case nameof(SetTransformerLockCommand):
                    case nameof(DeleteTransformerCommand):
                    case nameof(RenameTransformerCommand):
                    case nameof(EditRevisionsConfigurationCommand):
                    case nameof(UpdatePeriodicBackupCommand):
                    case nameof(EditExpirationCommand):
                    case nameof(ModifyConflictSolverCommand):
                    case nameof(UpdateTopologyCommand):
                    case nameof(DeleteDatabaseCommand):
                    case nameof(ModifyCustomFunctionsCommand):
                    case nameof(UpdateExternalReplicationCommand):
                    case nameof(PromoteDatabaseNodeCommand):
                    case nameof(ToggleTaskStateCommand):
                    case nameof(AddRavenEtlCommand):
                    case nameof(AddSqlEtlCommand):
                    case nameof(UpdateRavenEtlCommand):
                    case nameof(UpdateSqlEtlCommand):
                    case nameof(DeleteOngoingTaskCommand):
                    case nameof(PutRavenConnectionString):
                    case nameof(PutSqlConnectionString):
                    case nameof(RemoveRavenConnectionString):
                    case nameof(RemoveSqlConnectionString):
                        UpdateDatabase(context, type, cmd, index, leader);
                        break;
                    case nameof(UpdatePeriodicBackupStatusCommand):
                    case nameof(AcknowledgeSubscriptionBatchCommand):
                    case nameof(PutSubscriptionCommand):
                    case nameof(DeleteSubscriptionCommand):
                    case nameof(UpdateEtlProcessStateCommand):
                    case nameof(ToggleSubscriptionStateCommand):
                        SetValueForTypedDatabaseCommand(context, type, cmd, index, leader);
                        break;
                    case nameof(PutLicenseCommand):
                        PutValue<License>(context, type, cmd, index, leader);
                        break;
                    case nameof(PutCertificateCommand):
                        PutValue<CertificateDefinition>(context, type, cmd, index, leader);
                        // Once the certificate is in the cluster, no need to keep it locally so we delete it.
                        if (cmd.TryGet(nameof(PutCertificateCommand.Name), out string key))
                            DeleteLocalState(context, key);
                        break;
                    case nameof(PutClientConfigurationCommand):
                        PutValue<ClientConfiguration>(context, type, cmd, index, leader);
                        break;
                    case nameof(AddDatabaseCommand):
                        AddDatabase(context, cmd, index, leader);
                        break;
                }
            }
            catch (Exception e)
            {
                NotifyLeaderAboutError(index, leader, new CommandExecutionException($"Cannot execute command of type {type}", e));
            }
        }

        protected static void NotifyLeaderAboutError(long index, Leader leader, Exception e)
        {
            // ReSharper disable once UseNullPropagation
            if (leader == null)
                return;

            leader.SetStateOf(index, tcs => { tcs.TrySetException(e); });
        }

        private static bool ValidatePropertyExistence(BlittableJsonReaderObject cmd, string propertyTypeName, string propertyName, out string errorMessage)
        {
            errorMessage = null;
            if (cmd.TryGet(propertyName, out object _) == false)
            {
                errorMessage = $"Expected to find {propertyTypeName}.{propertyName} property in the Raft command but didn't find it...";
                return false;
            }
            return true;
        }

        private void SetValueForTypedDatabaseCommand(TransactionOperationContext context, string type, BlittableJsonReaderObject cmd, long index, Leader leader)
        {
            UpdateValueForDatabaseCommand updateCommand = null;
            try
            {
                var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);

                updateCommand = (UpdateValueForDatabaseCommand)JsonDeserializationCluster.Commands[type](cmd);

                var record = ReadDatabase(context, updateCommand.DatabaseName);
                if (record == null)
                {
                    NotifyLeaderAboutError(index, leader,
                        new CommandExecutionException($"Cannot set typed value of type {type} for database {updateCommand.DatabaseName}, because does not exist"));
                    return;
                }

                try
                {
                    updateCommand.Execute(context, items, index, record, _parent.CurrentState == RachisConsensus.State.Passive);
                }
                catch (Exception e)
                {
                    NotifyLeaderAboutError(index, leader,
                        new CommandExecutionException($"Cannot set typed value of type {type} for database {updateCommand.DatabaseName}, because does not exist", e));
                }
            }
            finally
            {
                NotifyDatabaseValueChanged(context, updateCommand?.DatabaseName, index, type);
            }
        }

        private readonly RachisLogIndexNotifications _rachisLogIndexNotifications = new RachisLogIndexNotifications(CancellationToken.None);

        public async Task WaitForIndexNotification(long index)
        {
            await _rachisLogIndexNotifications.WaitForIndexNotification(index, _parent.OperationTimeout);
        }

        private unsafe void RemoveNodeFromDatabase(TransactionOperationContext context, BlittableJsonReaderObject cmd, long index, Leader leader)
        {
            var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);
            var remove = JsonDeserializationCluster.RemoveNodeFromDatabaseCommand(cmd);
            var databaseName = remove.DatabaseName;
            var databaseNameLowered = databaseName.ToLowerInvariant();
            using (Slice.From(context.Allocator, "db/" + databaseNameLowered, out Slice lowerKey))
            using (Slice.From(context.Allocator, "db/" + databaseName, out Slice key))
            {
                if (items.ReadByKey(lowerKey, out TableValueReader reader) == false)
                {
                    NotifyLeaderAboutError(index, leader, new InvalidOperationException($"The database {databaseName} does not exists"));
                    return;
                }
                var doc = new BlittableJsonReaderObject(reader.Read(2, out int size), size);

                var databaseRecord = JsonDeserializationCluster.DatabaseRecord(doc);

                if (doc.TryGet(nameof(DatabaseRecord.Topology), out BlittableJsonReaderObject _) == false)
                {
                    items.DeleteByKey(lowerKey);
                    NotifyDatabaseChanged(context, databaseName, index, nameof(RemoveNodeFromDatabaseCommand));
                    return;
                }
                remove.UpdateDatabaseRecord(databaseRecord, index);

                if (databaseRecord.Topology.AllNodes.Any() == false)
                {
                    // delete database record
                    items.DeleteByKey(lowerKey);

                    // delete all values linked to database record - for subscription, etl etc.
                    CleanupDatabaseRelatedValues(context, items, databaseName);

                    NotifyDatabaseChanged(context, databaseName, index, nameof(RemoveNodeFromDatabaseCommand));
                    return;
                }

                var updated = EntityToBlittable.ConvertEntityToBlittable(databaseRecord, DocumentConventions.Default, context);

                UpdateValue(index, items, lowerKey, key, updated);

                NotifyDatabaseChanged(context, databaseName, index, nameof(RemoveNodeFromDatabaseCommand));
            }
        }

        private static void CleanupDatabaseRelatedValues(TransactionOperationContext context, Table items, string dbNameLowered)
        {
            var dbValuesPrefix = Helpers.ClusterStateMachineValuesPrefix(dbNameLowered).ToLowerInvariant();
            using (Slice.From(context.Allocator, dbValuesPrefix, out Slice loweredKey))
            {
                items.DeleteByPrimaryKeyPrefix(loweredKey);
            }
        }

        internal static unsafe void UpdateValue(long index, Table items, Slice lowerKey, Slice key, BlittableJsonReaderObject updated)
        {
            using (items.Allocate(out TableValueBuilder builder))
            {
                builder.Add(lowerKey);
                builder.Add(key);
                builder.Add(updated.BasePointer, updated.Size);
                builder.Add(Bits.SwapBytes(index));

                items.Set(builder);
            }
        }

        private unsafe void AddDatabase(TransactionOperationContext context, BlittableJsonReaderObject cmd, long index, Leader leader)
        {
            var addDatabaseCommand = JsonDeserializationCluster.AddDatabaseCommand(cmd);
            try
            {
                var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);
                using (Slice.From(context.Allocator, "db/" + addDatabaseCommand.Name, out Slice valueName))
                using (Slice.From(context.Allocator, "db/" + addDatabaseCommand.Name.ToLowerInvariant(), out Slice valueNameLowered))
                {
                    var databaseRecordAsJson = EntityToBlittable.ConvertEntityToBlittable(addDatabaseCommand.Record, DocumentConventions.Default, context);
                    if (addDatabaseCommand.RaftCommandIndex != null)
                    {
                        if (items.ReadByKey(valueNameLowered, out TableValueReader reader) == false && addDatabaseCommand.RaftCommandIndex != 0)
                        {
                            NotifyLeaderAboutError(index, leader,
                                new ConcurrencyException("Concurrency violation, the database " + addDatabaseCommand.Name + " does not exists, but had a non zero etag"));
                            return;
                        }

                        var actualEtag = Bits.SwapBytes(*(long*)reader.Read(3, out int size));
                        Debug.Assert(size == sizeof(long));

                        if (actualEtag != addDatabaseCommand.RaftCommandIndex.Value)
                        {
                            NotifyLeaderAboutError(index, leader,
                                new ConcurrencyException("Concurrency violation, the database " + addDatabaseCommand.Name + " has etag " + actualEtag +
                                                         " but was expecting " + addDatabaseCommand.RaftCommandIndex));
                            return;
                        }
                    }

                    UpdateValue(index, items, valueNameLowered, valueName, databaseRecordAsJson);
                    SetDatabaseValues(addDatabaseCommand.DatabaseValues, context, index, items);
                }
            }
            finally
            {
                NotifyDatabaseChanged(context, addDatabaseCommand.Name, index, nameof(AddDatabaseCommand));
            }
        }

        private static void SetDatabaseValues(
            Dictionary<string, object> databaseValues,
            TransactionOperationContext context,
            long index,
            Table items)
        {
            if (databaseValues == null)
                return;

            foreach (var keyValue in databaseValues)
            {
                using (Slice.From(context.Allocator, keyValue.Key, out Slice databaseValueName))
                using (Slice.From(context.Allocator, keyValue.Key.ToLowerInvariant(), out Slice databaseValueNameLowered))
                {
                    var value = EntityToBlittable.ConvertEntityToBlittable(keyValue.Value, DocumentConventions.Default, context);
                    UpdateValue(index, items, databaseValueNameLowered, databaseValueName, value);
                }
            }
        }

        private void DeleteValue(TransactionOperationContext context, string type, BlittableJsonReaderObject cmd, long index, Leader leader)
        {
            try
            {
                var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);
                var delCmd = JsonDeserializationCluster.DeleteValueCommand(cmd);
                if (delCmd.Name.StartsWith("db/"))
                {
                    NotifyLeaderAboutError(index, leader,
                        new InvalidOperationException("Cannot delete " + delCmd.Name + " using DeleteValueCommand, only via dedicated database calls"));
                    return;
                }

                using (Slice.From(context.Allocator, delCmd.Name, out Slice _))
                using (Slice.From(context.Allocator, delCmd.Name.ToLowerInvariant(), out Slice keyNameLowered))
                {
                    items.DeleteByKey(keyNameLowered);
                }
            }
            finally
            {
                NotifyValueChanged(context, type, index);
            }
        }

        private void PutValue<T>(TransactionOperationContext context, string type, BlittableJsonReaderObject cmd, long index, Leader leader)
        {
            try
            {
                var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);
                var command = (PutValueCommand<T>)CommandBase.CreateFrom(cmd);
                if (command.Name.StartsWith(Constants.Documents.Prefix))
                {
                    NotifyLeaderAboutError(index, leader,
                        new InvalidOperationException("Cannot set " + command.Name + " using PutValueCommand, only via dedicated database calls"));
                    return;
                }

                using (Slice.From(context.Allocator, command.Name, out Slice valueName))
                using (Slice.From(context.Allocator, command.Name.ToLowerInvariant(), out Slice valueNameLowered))
                {
                    var rec = context.ReadObject(command.ValueToJson(), "inner-val");
                    UpdateValue(index, items, valueNameLowered, valueName, rec);
                }
            }
            finally
            {
                NotifyValueChanged(context, type, index);
            }
        }

        private void NotifyValueChanged(TransactionOperationContext context, string type, long index)
        {
            context.Transaction.InnerTransaction.LowLevelTransaction.OnDispose += transaction =>
            {
                if (transaction is LowLevelTransaction llt && llt.Committed)
                    TaskExecutor.Execute(_ =>
                    {
                        try
                        {
                            ValueChanged?.Invoke(this, (index, type));
                        }
                        finally
                        {
                            _rachisLogIndexNotifications.NotifyListenersAbout(index);
                        }
                    }, null);
            };
        }

        private void NotifyDatabaseChanged(TransactionOperationContext context, string databaseName, long index, string type)
        {
            context.Transaction.InnerTransaction.LowLevelTransaction.OnDispose += transaction =>
            {
                if (transaction is LowLevelTransaction llt && llt.Committed)
                    TaskExecutor.Execute(_ =>
                    {
                        try
                        {
                            DatabaseChanged?.Invoke(this, (databaseName, index, type));
                        }
                        finally
                        {
                            _rachisLogIndexNotifications.NotifyListenersAbout(index);
                        }
                    }, null);
            };
        }

        private void NotifyDatabaseValueChanged(TransactionOperationContext context, string databaseName, long index, string type)
        {
            context.Transaction.InnerTransaction.LowLevelTransaction.OnDispose += transaction =>
            {
                if (transaction is LowLevelTransaction llt && llt.Committed)
                    TaskExecutor.Execute(_ =>
                    {
                        try
                        {
                            DatabaseValueChanged?.Invoke(this, (databaseName, index, type));
                        }
                        finally
                        {
                            _rachisLogIndexNotifications.NotifyListenersAbout(index);
                        }
                    }, null);
            };
        }

        private static readonly StringSegment DatabaseName = new StringSegment("DatabaseName");

        private DatabaseRecord UpdateDatabase(TransactionOperationContext context, string type, BlittableJsonReaderObject cmd, long index, Leader leader)
        {
            if (cmd.TryGet(DatabaseName, out string databaseName) == false)
                throw new ArgumentException("Update database command must contain a DatabaseName property");

            DatabaseRecord databaseRecord;
            try
            {
                var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);
                var dbKey = "db/" + databaseName;

                using (Slice.From(context.Allocator, dbKey, out Slice valueName))
                using (Slice.From(context.Allocator, dbKey.ToLowerInvariant(), out Slice valueNameLowered))
                {
                    try
                    {
                        var databaseRecordJson = ReadInternal(context, out long etag, valueNameLowered);
                        var updateCommand = (UpdateDatabaseCommand)JsonDeserializationCluster.Commands[type](cmd);

                        if (databaseRecordJson == null)
                        {
                            if (updateCommand.ErrorOnDatabaseDoesNotExists)
                                NotifyLeaderAboutError(index, leader,
                                    DatabaseDoesNotExistException.CreateWithMessage(databaseName, $"Could not execute update command of type '{type}'."));
                            return null;
                        }

                        databaseRecord = JsonDeserializationCluster.DatabaseRecord(databaseRecordJson);

                        if (updateCommand.RaftCommandIndex != null && etag != updateCommand.RaftCommandIndex.Value)
                        {
                            NotifyLeaderAboutError(index, leader,
                                new ConcurrencyException(
                                    $"Concurrency violation at executing {type} command, the database {databaseRecord.DatabaseName} has etag {etag} but was expecting {updateCommand.RaftCommandIndex}"));
                            return null;
                        }

                        var relatedRecordIdToDelete = updateCommand.UpdateDatabaseRecord(databaseRecord, index);
                        if (relatedRecordIdToDelete != null)
                        {
                            var itemKey = relatedRecordIdToDelete;
                            using (Slice.From(context.Allocator, itemKey, out Slice _))
                            using (Slice.From(context.Allocator, itemKey.ToLowerInvariant(), out Slice valueNameToDeleteLowered))
                            {
                                items.DeleteByKey(valueNameToDeleteLowered);
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        NotifyLeaderAboutError(index, leader, new CommandExecutionException($"Cannot execute command of type {type} for database {databaseName}", e));
                        return null;
                    }

                    var updatedDatabaseBlittable = EntityToBlittable.ConvertEntityToBlittable(databaseRecord, DocumentConventions.Default, context);
                    UpdateValue(index, items, valueNameLowered, valueName, updatedDatabaseBlittable);
                }
            }
            finally
            {
                NotifyDatabaseChanged(context, databaseName, index, type);
            }

            return databaseRecord;
        }


        public override bool ShouldSnapshot(Slice slice, RootObjectType type)
        {
            return slice.Content.Match(Items.Content);
        }

        public override void Initialize(RachisConsensus parent, TransactionOperationContext context)
        {
            base.Initialize(parent, context);
            ItemsSchema.Create(context.Transaction.InnerTransaction, Items, 32);
            context.Transaction.InnerTransaction.CreateTree(LocalNodeStateTreeName);
        }

        public unsafe void PutLocalState(TransactionOperationContext context, string key, BlittableJsonReaderObject value)
        {
            var localState = context.Transaction.InnerTransaction.CreateTree(LocalNodeStateTreeName);
            using (localState.DirectAdd(key, value.Size, out var ptr))
            {
                value.CopyTo(ptr);
            }
        }

        public void DeleteLocalState(TransactionOperationContext context, string key)
        {
            var localState = context.Transaction.InnerTransaction.CreateTree(LocalNodeStateTreeName);
            localState.Delete(key);
        }

        public unsafe BlittableJsonReaderObject GetLocalState(TransactionOperationContext context, string key)
        {
            var localState = context.Transaction.InnerTransaction.ReadTree(LocalNodeStateTreeName);
            var read = localState.Read(key);
            if (read == null)
                return null;
            return new BlittableJsonReaderObject(read.Reader.Base, read.Reader.Length);
        }

        public IEnumerable<string> GetCertificateKeysFromLocalState(TransactionOperationContext context)
        {
            var tree = context.Transaction.InnerTransaction.ReadTree(LocalNodeStateTreeName);
            if (tree == null)
                yield break;

            using (var it = tree.Iterate(prefetch: false))
            {
                if (it.Seek(Slices.BeforeAllKeys) == false)
                    yield break;
                do
                {
                    yield return it.CurrentKey.ToString();
                } while (it.MoveNext());
            }
        }

        public IEnumerable<Tuple<string, BlittableJsonReaderObject>> ItemsStartingWith(TransactionOperationContext context, string prefix, int start, int take)
        {
            var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);

            var dbKey = prefix.ToLowerInvariant();
            using (Slice.From(context.Allocator, dbKey, out Slice loweredPrefix))
            {
                foreach (var result in items.SeekByPrimaryKeyPrefix(loweredPrefix, Slices.Empty, start))
                {
                    if (take-- <= 0)
                        yield break;

                    yield return GetCurrentItem(context, result.Value);
                }
            }
        }

        public IEnumerable<string> ItemKeysStartingWith(TransactionOperationContext context, string prefix, int start, int take)
        {
            var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);

            var dbKey = prefix.ToLowerInvariant();
            using (Slice.From(context.Allocator, dbKey, out Slice loweredPrefix))
            {
                foreach (var result in items.SeekByPrimaryKeyPrefix(loweredPrefix, Slices.Empty, start))
                {
                    if (take-- <= 0)
                        yield break;

                    yield return GetCurrentItemKey(result.Value);
                }
            }
        }

        public IEnumerable<string> GetDatabaseNames(TransactionOperationContext context, int start = 0, int take = int.MaxValue)
        {
            var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);

            const string dbKey = "db/";
            using (Slice.From(context.Allocator, dbKey, out Slice loweredPrefix))
            {
                foreach (var result in items.SeekByPrimaryKeyPrefix(loweredPrefix, Slices.Empty, 0))
                {
                    if (take-- <= 0)
                        yield break;

                    yield return GetCurrentItemKey(result.Value).Substring(3);
                }
            }
        }

        private static unsafe string GetCurrentItemKey(Table.TableValueHolder result)
        {
            return Encoding.UTF8.GetString(result.Reader.Read(1, out int size), size);
        }

        private static unsafe Tuple<string, BlittableJsonReaderObject> GetCurrentItem(JsonOperationContext context, Table.TableValueHolder result)
        {
            var ptr = result.Reader.Read(2, out int size);
            var doc = new BlittableJsonReaderObject(ptr, size);

            var key = Encoding.UTF8.GetString(result.Reader.Read(1, out size), size);

            return Tuple.Create(key, doc);
        }

        public DatabaseRecord ReadDatabase(TransactionOperationContext context, string name)
        {
            return ReadDatabase(context, name, out long _);
        }

        public DatabaseRecord ReadDatabase<T>(TransactionOperationContext<T> context, string name, out long etag)
            where T : RavenTransaction
        {
            var doc = Read(context, "db/" + name.ToLowerInvariant(), out etag);
            if (doc == null)
                return null;

            return JsonDeserializationCluster.DatabaseRecord(doc);
        }

        public BlittableJsonReaderObject Read<T>(TransactionOperationContext<T> context, string name)
            where T : RavenTransaction
        {
            return Read(context, name, out long _);
        }

        public BlittableJsonReaderObject Read<T>(TransactionOperationContext<T> context, string name, out long etag)
            where T : RavenTransaction
        {
            var dbKey = name.ToLowerInvariant();
            using (Slice.From(context.Allocator, dbKey, out Slice key))
            {
                return ReadInternal(context, out etag, key);
            }
        }

        private static unsafe BlittableJsonReaderObject ReadInternal<T>(TransactionOperationContext<T> context, out long etag, Slice key)
            where T : RavenTransaction
        {
            var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);
            if (items.ReadByKey(key, out TableValueReader reader) == false)
            {
                etag = 0;
                return null;
            }

            var ptr = reader.Read(2, out int size);
            var doc = new BlittableJsonReaderObject(ptr, size);

            etag = Bits.SwapBytes(*(long*)reader.Read(3, out size));
            Debug.Assert(size == sizeof(long));

            return doc;
        }

        public static IEnumerable<(Slice Key, BlittableJsonReaderObject Value)> ReadValuesStartingWith(
            TransactionOperationContext context, string startsWithKey)
        {
            var startsWithKeyLower = startsWithKey.ToLowerInvariant();
            using (Slice.From(context.Allocator, startsWithKeyLower, out Slice startsWithSlice))
            {
                var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);

                foreach (var holder in items.SeekByPrimaryKeyPrefix(startsWithSlice, Slices.Empty, 0))
                {
                    var reader = holder.Value.Reader;
                    var size = GetDataAndEtagTupleFromReader(context, reader, out BlittableJsonReaderObject doc, out long _);
                    Debug.Assert(size == sizeof(long));

                    yield return (holder.Key, doc);
                }
            }
        }

        private static unsafe int GetDataAndEtagTupleFromReader(JsonOperationContext context, TableValueReader reader, out BlittableJsonReaderObject doc,
            out long etag)
        {
            var ptr = reader.Read(2, out int size);
            doc = new BlittableJsonReaderObject(ptr, size);

            etag = Bits.SwapBytes(*(long*)reader.Read(3, out size));
            Debug.Assert(size == sizeof(long));
            return size;
        }

        public override async Task<Stream> ConnectToPeer(string url, X509Certificate2 certificate)
        {
            if (url == null)
                throw new ArgumentNullException(nameof(url));
            if (_parent == null)
                throw new InvalidOperationException("Cannot connect to peer without a parent");
            if (_parent.IsEncrypted && url.StartsWith("https:", StringComparison.OrdinalIgnoreCase) == false)
                throw new InvalidOperationException($"Failed to connect to node {url}. Connections from encrypted store must use HTTPS.");

            var info = await ReplicationUtils.GetTcpInfoAsync(url, "Rachis.Server", "Cluster", certificate);

            var tcpInfo = new Uri(info.Url);
            var tcpClient = new TcpClient();
            Stream stream = null;
            try
            {
                TcpUtils.SetTimeouts(tcpClient, _parent.TcpConnectionTimeout);
                await tcpClient.ConnectAsync(tcpInfo.Host, tcpInfo.Port);
                stream = await TcpUtils.WrapStreamWithSslAsync(tcpClient, info, _parent.ClusterCertificate);

                using (ContextPoolForReadOnlyOperations.AllocateOperationContext(out JsonOperationContext context))
                {
                    var msg = new DynamicJsonValue
                    {
                        [nameof(TcpConnectionHeaderMessage.DatabaseName)] = null,
                        [nameof(TcpConnectionHeaderMessage.Operation)] = TcpConnectionHeaderMessage.OperationTypes.Cluster,
                        [nameof(TcpConnectionHeaderMessage.OperationVersion)] = TcpConnectionHeaderMessage.TcpVersions[TcpConnectionHeaderMessage.OperationTypes.Cluster]
                    };
                    using (var writer = new BlittableJsonTextWriter(context, stream))
                    {
                        var msgJson = context.ReadObject(msg, "message");
                        context.Write(writer, msgJson);
                    }
                    var response = context.ReadForMemory(stream, "cluster-ConnectToPeer-header-response");
                    var reply = JsonDeserializationServer.TcpConnectionHeaderResponse(context, response);
                    if (reply.AuthorizationSuccessful == false)
                    {
                        throw new AuthorizationException("Unable to access " + url + " because " + reply.Message);
                    }
                }
                return stream;
            }
            catch (Exception)
            {
                stream?.Dispose();
                tcpClient.Dispose();
                throw;
            }
        }

        public override void OnSnapshotInstalled(TransactionOperationContext context, long lastIncludedIndex, ServerStore serverStore)
        {
            using (context.OpenWriteTransaction())
            {
                // Lets read all the certificate keys from the cluster, and delete the matching ones from the local state
                var clusterCertificateKeys = serverStore.Cluster.ItemKeysStartingWith(context, Constants.Certificates.Prefix, 0, int.MaxValue);

                foreach (var key in clusterCertificateKeys)
                {
                    var obj = GetLocalState(context, key);
                    try
                    {
                        DeleteLocalState(context, key);
                    }
                    finally
                    {
                        obj.Dispose(context);
                    }
                }
                //There is potentially a lot of work to be done here so we are responding to the change on a separate task.
                var onDatabaseChanged = DatabaseChanged;
                if (onDatabaseChanged != null)
                {
                    var listOfDatabaseName = GetDatabaseNames(context).ToList();
                    TaskExecutor.Execute(_ =>
                    {
                        foreach (var db in listOfDatabaseName)
                            onDatabaseChanged.Invoke(this, (db, lastIncludedIndex, "SnapshotInstalled"));
                    }, null);
                }

                context.Transaction.Commit();
            }

            _rachisLogIndexNotifications.NotifyListenersAbout(lastIncludedIndex);
        }
    }

    public class RachisLogIndexNotifications
    {
        public long LastModifiedIndex;
        private readonly AsyncManualResetEvent _notifiedListeners;

        public RachisLogIndexNotifications(CancellationToken token)
        {
            _notifiedListeners = new AsyncManualResetEvent(token);
        }

        public async Task WaitForIndexNotification(long index, TimeSpan? timeout = null)
        {
            while (true)
            {
                // first get the task, then wait on it
                var waitAsync = timeout.HasValue == false ? _notifiedListeners.WaitAsync() : _notifiedListeners.WaitAsync(timeout.Value);

                if (index <= Volatile.Read(ref LastModifiedIndex))
                    break;

                if (await waitAsync == false)
                {
                    if (index <= Volatile.Read(ref LastModifiedIndex))
                        break;
                    ThrowTimeoutException(timeout ?? TimeSpan.MaxValue, index, LastModifiedIndex);
                }
            }
        }

        private static void ThrowTimeoutException(TimeSpan value, long index, long lastModifiedIndex)
        {
            throw new TimeoutException($"Waited for {value} but didn't get index notification for {index}. " +
                                       $"Last commit index is: {lastModifiedIndex}.");
        }

        public void NotifyListenersAbout(long index)
        {
            var lastModified = LastModifiedIndex;
            while (index > lastModified)
            {
                lastModified = Interlocked.CompareExchange(ref LastModifiedIndex, index, lastModified);
            }
            _notifiedListeners.SetAndResetAtomically();
        }
    }
}
