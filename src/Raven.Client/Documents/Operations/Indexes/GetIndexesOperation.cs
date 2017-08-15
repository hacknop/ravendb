using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Http;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Indexes
{
    public class GetIndexesOperation : IAdminOperation<IndexDefinition[]>
    {
        private readonly int _start;
        private readonly int _pageSize;

        public GetIndexesOperation(int start, int pageSize)
        {
            _start = start;
            _pageSize = pageSize;
        }

        public RavenCommand<IndexDefinition[]> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GetIndexesCommand(context, _start, _pageSize);
        }

        private class GetIndexesCommand : RavenCommand<IndexDefinition[]>
        {
            private readonly JsonOperationContext _ctx;
            private readonly int _start;
            private readonly int _pageSize;

            public GetIndexesCommand(JsonOperationContext ctx, int start, int pageSize)
            {
                _ctx = ctx;
                _start = start;
                _pageSize = pageSize;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/indexes?start={_start}&pageSize={_pageSize}";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonDeserializationClient.GetIndexesResponse(_ctx, response).Results;
            }

            public override bool IsReadRequest => true;
        }
    }
}
