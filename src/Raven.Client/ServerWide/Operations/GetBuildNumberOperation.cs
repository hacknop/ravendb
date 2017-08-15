﻿using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations
{
    public class GetBuildNumberOperation : IServerOperation<BuildNumber>
    {
        public RavenCommand<BuildNumber> GetCommand(DocumentConventions conventions, JsonOperationContext ctx)
        {
            return new GetBuildNumberCommand(ctx);
        }

        private class GetBuildNumberCommand : RavenCommand<BuildNumber>
        {
            private readonly JsonOperationContext _ctx;
            public override bool IsReadRequest => true;

            public GetBuildNumberCommand(JsonOperationContext ctx)
            {
                _ctx = ctx;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/build/version";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonDeserializationClient.BuildNumber(_ctx, response);
            }
        }
    }
}
