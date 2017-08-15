﻿using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Http;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Indexes
{
    public class GetIndexingStatusOperation : IAdminOperation<IndexingStatus>
    {
        public RavenCommand<IndexingStatus> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GetIndexingStatusCommand(context);
        }

        private class GetIndexingStatusCommand : RavenCommand<IndexingStatus>
        {
            private readonly JsonOperationContext _ctx;

            public GetIndexingStatusCommand(JsonOperationContext ctx)
            {
                _ctx = ctx;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/indexes/status";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonDeserializationClient.IndexingStatus(_ctx, response);
            }

            public override bool IsReadRequest => true;
        }
    }
}
