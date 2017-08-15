using System.Net.Http;
using Raven.Client.Http;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Commands
{
    public class NodeInfo
    {
        public string NodeTag;
        public string TopologyId;
        public string Certificate;
        public string ClusterStatus;
    }

    public class GetNodeInfoCommand : RavenCommand<NodeInfo>
    {
        private readonly JsonOperationContext _ctx;

        public GetNodeInfoCommand(JsonOperationContext ctx)
        {
            _ctx = ctx;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/cluster/node-info";

            return new HttpRequestMessage
            {
                Method = HttpMethod.Get
            };
        }

        public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
                return;

            Result = JsonDeserializationClient.NodeInfo(_ctx, response);
        }

        public override bool IsReadRequest => true;
    }
}
