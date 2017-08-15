﻿using System.Net.Http;
using Raven.Client.Http;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Commands
{
    public class GetClusterTopologyCommand : RavenCommand<ClusterTopologyResponse>
    {
        private readonly JsonOperationContext _ctx;

        public GetClusterTopologyCommand(JsonOperationContext ctx)
        {
            _ctx = ctx;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/cluster/topology";
           
            return new HttpRequestMessage
            {
                Method = HttpMethod.Get
            };
        }

        public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
                return;

            Result = JsonDeserializationClient.ClusterTopology(_ctx, response);
        }

        public override bool IsReadRequest => true;
    }
}
