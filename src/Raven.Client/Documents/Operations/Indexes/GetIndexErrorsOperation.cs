using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Http;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Indexes
{
    public class GetIndexErrorsOperation : IAdminOperation<IndexErrors[]>
    {
        private readonly string[] _indexNames;

        public GetIndexErrorsOperation()
        {
        }

        public GetIndexErrorsOperation(string[] indexNames)
        {
            _indexNames = indexNames;
        }

        public RavenCommand<IndexErrors[]> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GetIndexErrorsCommand(context, _indexNames);
        }

        private class GetIndexErrorsCommand : RavenCommand<IndexErrors[]>
        {
            private readonly JsonOperationContext _ctx;
            private readonly string[] _indexNames;

            public GetIndexErrorsCommand(JsonOperationContext ctx, string[] indexNames)
            {
                _ctx = ctx;
                _indexNames = indexNames;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/indexes/errors";
                if (_indexNames != null && _indexNames.Length > 0)
                {
                    url += "?";
                    foreach (var indexName in _indexNames)
                        url += $"&name={indexName}";
                }

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null ||
                    response.TryGet("Results", out BlittableJsonReaderArray results) == false)
                {
                    ThrowInvalidResponse();
                    return; // never hit
                }

                var indexErrors = new IndexErrors[results.Length];
                for (int i = 0; i < results.Length; i++)
                {
                    var obj = results.GetValueTokenTupleByIndex(_ctx, i).Value;
                    indexErrors[i] = JsonDeserializationClient.IndexErrors(_ctx, (BlittableJsonReaderObject)obj);
                }

                Result = indexErrors;
            }

            public override bool IsReadRequest => true;
        }
    }
}
