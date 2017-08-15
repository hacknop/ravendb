﻿using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Http;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Indexes
{
    public class GetIndexStatisticsOperation : IAdminOperation<IndexStats>
    {
        private readonly string _indexName;

        public GetIndexStatisticsOperation(string indexName)
        {
            _indexName = indexName ?? throw new ArgumentNullException(nameof(indexName));
        }

        public RavenCommand<IndexStats> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GetIndexStatisticsCommand(context, _indexName);
        }

        private class GetIndexStatisticsCommand : RavenCommand<IndexStats>
        {
            private readonly JsonOperationContext _ctx;
            private readonly string _indexName;

            public GetIndexStatisticsCommand(JsonOperationContext ctx, string indexName)
            {
                _ctx = ctx;
                _indexName = indexName ?? throw new ArgumentNullException(nameof(indexName));
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/indexes/stats?name={Uri.EscapeDataString(_indexName)}";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                var results = JsonDeserializationClient.GetIndexStatisticsResponse(_ctx, response).Results;
                if (results.Length != 1)
                    ThrowInvalidResponse();

                Result = results[0];
            }

            public override bool IsReadRequest => true;
        }
    }
}
