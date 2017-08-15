using System;
using System.Collections.Generic;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations
{
    public class GetIdentitiesCommand : RavenCommand<Dictionary<string, long>>
    {
        private readonly JsonOperationContext _ctx;

        private static readonly Func<JsonOperationContext,BlittableJsonReaderObject, IdentitiesResult> _deserializeIdentities = 
            JsonDeserializationBase.GenerateJsonDeserializationRoutine<IdentitiesResult>();

        // ReSharper disable once ClassNeverInstantiated.Local
        private class IdentitiesResult
        {
            public Dictionary<string, long> Identities { get; set; }
        }

        public override bool IsReadRequest => true;

        public GetIdentitiesCommand(JsonOperationContext ctx)
        {
            _ctx = ctx;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/debug/identities";
            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Get
            };

            return request;
        }

        public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
        {
            Result = _deserializeIdentities(_ctx, response).Identities;
        }
    }

    public class GetIdentitiesOperation : IAdminOperation<Dictionary<string,long>>
    {
        public RavenCommand<Dictionary<string, long>> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GetIdentitiesCommand(context);
        }
    }
}
