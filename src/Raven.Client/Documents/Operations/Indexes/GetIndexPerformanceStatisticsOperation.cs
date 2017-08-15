using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Indexes
{
    public class GetIndexPerformanceStatisticsOperation : IAdminOperation<IndexPerformanceStats[]>
    {
        private readonly string[] _indexNames;

        public GetIndexPerformanceStatisticsOperation()
        {
        }

        public GetIndexPerformanceStatisticsOperation(string[] indexNames)
        {
            _indexNames = indexNames ?? throw new ArgumentNullException(nameof(indexNames));
        }

        public RavenCommand<IndexPerformanceStats[]> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GetIndexPerformanceStatisticsCommand(context, conventions, _indexNames);
        }

        private class GetIndexPerformanceStatisticsCommand : RavenCommand<IndexPerformanceStats[]>
        {
            private readonly JsonOperationContext _ctx;
            private readonly DocumentConventions _conventions;
            private readonly string[] _indexNames;

            public GetIndexPerformanceStatisticsCommand(JsonOperationContext ctx, DocumentConventions conventions, string[] indexNames)
            {
                _ctx = ctx;
                _conventions = conventions;
                _indexNames = indexNames;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = GetUrl(node);

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

                var stats = new IndexPerformanceStats[results.Length];
                for (var i = 0; i < results.Length; i++)
                {
                    var item = results.GetValueTokenTupleByIndex(_ctx, i).Value;
                    stats[i] = (IndexPerformanceStats)_conventions.DeserializeEntityFromBlittable(_ctx, typeof(IndexPerformanceStats), (BlittableJsonReaderObject)item);
                }

                Result = stats;
            }

            private string GetUrl(ServerNode node)
            {
                var url = $"{node.Url}/databases/{node.Database}/indexes/performance";

                if (_indexNames == null)
                    return url;

                var first = true;
                foreach (var indexName in _indexNames)
                {
                    if (first)
                        url += $"?name={Uri.EscapeDataString(indexName)}";
                    else
                        url += $"&name={Uri.EscapeDataString(indexName)}";

                    first = false;
                }

                return url;
            }

            public override bool IsReadRequest => true;
        }
    }
}
