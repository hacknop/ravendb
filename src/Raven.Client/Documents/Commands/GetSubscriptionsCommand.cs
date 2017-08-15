﻿using System.Net.Http;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Http;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.Documents.Commands
{
    public class GetSubscriptionsCommand : RavenCommand<SubscriptionState[]>
    {
        private readonly JsonOperationContext _ctx;
        private readonly int _start;
        private readonly int _pageSize;

        public GetSubscriptionsCommand(JsonOperationContext ctx, int start, int pageSize)
        {
            _ctx = ctx;
            _start = start;
            _pageSize = pageSize;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/subscriptions?start={_start}&pageSize={_pageSize}";

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Get
            };
            return request;
        }

        public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
            {
                Result = null;
                return;
            }

            Result = JsonDeserializationClient.GetSubscriptionsResult(_ctx, response).Results;
        }

        public override bool IsReadRequest => true;
    }
}
