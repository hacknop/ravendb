using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Indexes
{
    public class CompactIndexOperation : IAdminOperation<OperationIdResult>
    {
        private readonly string _indexName;

        public CompactIndexOperation(string indexName)
        {
            _indexName = indexName ?? throw new ArgumentNullException(nameof(indexName));
        }

        public RavenCommand<OperationIdResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new CompactIndexCommand(context, _indexName);
        }

        private class CompactIndexCommand : RavenCommand<OperationIdResult>
        {
            private readonly JsonOperationContext _ctx;
            private readonly string _indexName;

            public CompactIndexCommand(JsonOperationContext ctx, string indexName)
            {
                _ctx = ctx;
                _indexName = indexName ?? throw new ArgumentNullException(nameof(indexName));
            }

            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/admin/indexes/compact?name={Uri.EscapeDataString(_indexName)}";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Post
                };
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonDeserializationClient.OperationIdResult(_ctx,response);
            }
        }
    }
}
