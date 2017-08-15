using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Replication;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations
{
    public class GetReplicationPerformanceStatisticsOperation : IAdminOperation<ReplicationPerformance>
    {
        public RavenCommand<ReplicationPerformance> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GetReplicationPerformanceStatisticsCommand(context, conventions);
        }

        private class GetReplicationPerformanceStatisticsCommand : RavenCommand<ReplicationPerformance>
        {
            private readonly JsonOperationContext _ctx;
            private readonly DocumentConventions _conventions;

            public GetReplicationPerformanceStatisticsCommand(JsonOperationContext ctx, DocumentConventions conventions)
            {
                _ctx = ctx;
                _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
            }

            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/replication/performance";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = (ReplicationPerformance)EntityToBlittable.ConvertToEntity(_ctx,typeof(ReplicationPerformance), "replication/performance", response, _conventions);
            }
        }
    }
}
