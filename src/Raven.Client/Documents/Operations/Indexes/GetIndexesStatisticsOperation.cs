﻿using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Http;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Indexes
{
    public class GetIndexesStatisticsOperation : IAdminOperation<IndexStats[]>
    {
        public RavenCommand<IndexStats[]> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GetIndexesStatisticsCommand(context);
        }

        private class GetIndexesStatisticsCommand : RavenCommand<IndexStats[]>
        {
            private readonly JsonOperationContext _context;

            public GetIndexesStatisticsCommand(JsonOperationContext context)
            {
                _context = context;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/indexes/stats";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                var results = JsonDeserializationClient.GetIndexStatisticsResponse(_context, response).Results;
                Result = results;
            }

            public override bool IsReadRequest => true;
        }
    }
}
