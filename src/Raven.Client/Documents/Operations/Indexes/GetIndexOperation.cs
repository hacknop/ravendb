﻿using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Http;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Indexes
{
    public class GetIndexOperation : IAdminOperation<IndexDefinition>
    {
        private readonly string _indexName;

        public GetIndexOperation(string indexName)
        {
            _indexName = indexName ?? throw new ArgumentNullException(nameof(indexName));
        }

        public RavenCommand<IndexDefinition> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GetIndexCommand(context, _indexName);
        }

        private class GetIndexCommand : RavenCommand<IndexDefinition>
        {
            private readonly JsonOperationContext _ctx;
            private readonly string _indexName;

            public GetIndexCommand(JsonOperationContext ctx, string indexName)
            {
                _ctx = ctx;
                _indexName = indexName ?? throw new ArgumentNullException(nameof(indexName));
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/indexes?name={Uri.EscapeDataString(_indexName)}";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    return;

                Result = JsonDeserializationClient.GetIndexesResponse(_ctx, response).Results[0];
            }

            public override bool IsReadRequest => true;
        }
    }
}
