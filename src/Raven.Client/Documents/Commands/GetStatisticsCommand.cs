using System.Net.Http;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.Documents.Commands
{
    public class GetStatisticsCommand : RavenCommand<DatabaseStatistics>
    {
        private readonly JsonOperationContext _ctx;
        public readonly string DebugTag;

        public GetStatisticsCommand(JsonOperationContext ctx)
        {
            _ctx = ctx;
        }
        
        public GetStatisticsCommand(string debugTag)
        {
            DebugTag = debugTag;
        }
        
        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/stats";
            if (DebugTag != null)
                url += "?" + DebugTag;
            return new HttpRequestMessage
            {
                Method = HttpMethod.Get
            };
        }

        public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
        {
            Result = JsonDeserializationClient.GetStatisticsResult(_ctx, response);
        }

        public override bool IsReadRequest => true;
    }
}
