using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations
{
    public class GetOngoingTaskInfoOperation : IServerOperation<OngoingTask>
    {
        private readonly string _database;
        private readonly long _taskId;
        private readonly OngoingTaskType _type;

        public GetOngoingTaskInfoOperation(string database, long taskId, OngoingTaskType type)
        {
            Helpers.AssertValidDatabaseName(database);
            _database = database;
            _taskId = taskId;
            _type = type;
        }

        public RavenCommand<OngoingTask> GetCommand(DocumentConventions conventions, JsonOperationContext ctx)
        {
            return new GetOngoingTaskInfoCommand(ctx, _database, _taskId, _type);
        }

        private class GetOngoingTaskInfoCommand : RavenCommand<OngoingTask>
        {
            private readonly string _databaseName;
            private readonly JsonOperationContext _ctx;
            private readonly long _taskId;
            private readonly OngoingTaskType _type;

            public GetOngoingTaskInfoCommand(
                JsonOperationContext ctx, 
                string database,
                long taskId,
                OngoingTaskType type
            )
            {
                _databaseName = database ?? throw new ArgumentNullException(nameof(database));
                _ctx = ctx;
                _taskId = taskId;
                _type = type;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{_databaseName}/task?key={_taskId}&type={_type}";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };

                return request;
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
                if (response != null)
                {
                    switch (_type)
                    {
                        case OngoingTaskType.Replication:
                            Result = JsonDeserializationClient.GetOngoingTaskReplicationResult(_ctx, response);
                            break;
                        case OngoingTaskType.RavenEtl:
                            Result = JsonDeserializationClient.GetOngoingTaskRavenEtlResult(_ctx, response);
                            break;
                        case OngoingTaskType.SqlEtl:
                            Result = JsonDeserializationClient.GetOngoingTaskSqlEtlResult(_ctx, response);
                            break;
                        case OngoingTaskType.Backup:
                            Result = JsonDeserializationClient.GetOngoingTaskBackupResult(_ctx, response);
                            break;
                        case OngoingTaskType.Subscription:
                            Result = JsonDeserializationClient.GetOngoingTaskSubscriptionResult(_ctx, response);
                            break;
                        default:
                            throw new ArgumentOutOfRangeException();
                    }
                }
            }

            public override bool IsReadRequest => false;
        }
    }

}
