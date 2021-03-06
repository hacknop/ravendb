﻿using System;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.ServerWide.Context;
using Tests.Infrastructure;
using Xunit;

namespace RachisTests
{

    public class Cluster : ClusterTestBase
    {
        private static async Task<int> GetMembersCount(IDocumentStore store, string databaseName)
        {
            var res = await store.Admin.Server.SendAsync(new GetDatabaseRecordOperation(databaseName));
            if (res == null)
            {
                return -1;
            }
            return res.Topology.Members.Count;
        }

        [NightlyBuildFact]
        public async Task CanCreateAddAndDeleteDatabaseFromNodes()
        {
            var clusterSize = 3;
            var leader = await CreateRaftClusterAndGetLeader(clusterSize);
            var replicationFactor = 2;
            var databaseName = "test";
            using (var store = new DocumentStore()
            {
                Urls = new[] { leader.WebUrl },
                Database = databaseName
            }.Initialize())
            {
                var doc = new DatabaseRecord(databaseName);
                var databaseResult = store.Admin.Server.Send(new CreateDatabaseOperation(doc, replicationFactor));

                int numberOfInstances = 0;
                await AssertNumberOfNodesContainingDatabase(databaseResult.RaftCommandIndex, databaseName, numberOfInstances, replicationFactor);
                databaseResult = store.Admin.Server.Send(new AddDatabaseNodeOperation(databaseName));
                Assert.Equal(databaseResult.Topology.AllNodes.Count(), ++replicationFactor);
                numberOfInstances = 0;
                await AssertNumberOfNodesContainingDatabase(databaseResult.RaftCommandIndex, databaseName, numberOfInstances, replicationFactor);
                while (replicationFactor > 0)
                {
                    var val = await WaitForValueAsync(async () => await GetMembersCount(store, databaseName), replicationFactor);
                    Assert.Equal(replicationFactor, val);
                    var res = await store.Admin.Server.SendAsync(new GetDatabaseRecordOperation(databaseName));

                    var serverTagToBeDeleted = res.Topology.Members[0];
                    replicationFactor--;
                    var deleteResult = store.Admin.Server.Send(new DeleteDatabasesOperation(databaseName, hardDelete: true, fromNode: serverTagToBeDeleted, timeToWaitForConfirmation: TimeSpan.FromSeconds(30)));
                    Assert.Empty(deleteResult.PendingDeletes);
                    await AssertNumberOfNodesContainingDatabase(deleteResult.RaftCommandIndex, databaseName, numberOfInstances, replicationFactor);
                }
                using (leader.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.OpenReadTransaction())
                {
                    Assert.Null(leader.ServerStore.Cluster.ReadDatabase(context, databaseName));
                }
            }
        }

        private async Task AssertNumberOfNodesContainingDatabase(long eTag, string databaseName, int numberOfInstances, int replicationFactor)
        {
            await Task.Delay(500);

            foreach (var server in Servers)
            {
                await server.ServerStore.Cluster.WaitForIndexNotification(eTag);
                try
                {
                    if (server.ServerStore.DatabasesLandlord.DatabasesCache.TryGetValue(databaseName, out var _))
                        numberOfInstances++;
                }
                catch
                {
                    // ignored
                }
            }
            Assert.True(numberOfInstances == replicationFactor, $"Expected replicationFactor={replicationFactor} but got {numberOfInstances}");
        }

    }
}
