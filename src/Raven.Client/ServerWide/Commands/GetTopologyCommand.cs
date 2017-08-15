﻿using System.Net.Http;
using Raven.Client.Http;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Commands
{
    public class GetTopologyCommand : RavenCommand<Topology>
    {
        private readonly JsonOperationContext _ctx;
        private readonly string _forcedUrl;

        public GetTopologyCommand(JsonOperationContext ctx, string forcedUrl = null)
        {
            _ctx = ctx;
            _forcedUrl = forcedUrl;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/topology?name={node.Database}";
            if (string.IsNullOrEmpty(_forcedUrl) == false)
            {
                url += $"&url={_forcedUrl}";
            }
            return new HttpRequestMessage
            {
                Method = HttpMethod.Get
            };
        }

        public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
                return;

            Result = JsonDeserializationClient.Topology(_ctx, response);
        }

        public override bool IsReadRequest => true;
    }
}
