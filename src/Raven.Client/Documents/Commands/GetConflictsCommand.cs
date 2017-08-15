using System.Net.Http;
using Raven.Client.Http;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.Documents.Commands
{
    public class GetConflictsCommand : RavenCommand<GetConflictsResult>
    {
        private readonly JsonOperationContext _ctx;
        private readonly string _id;
        public override bool IsReadRequest => true;

        public GetConflictsCommand(JsonOperationContext ctx, string id)
        {
            _ctx = ctx;
            _id = id;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/replication/conflicts?docId={_id}";
            return new HttpRequestMessage
            {
                Method = HttpMethod.Get
            };
        }

        public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
                ThrowInvalidResponse();
            Result = JsonDeserializationClient.GetConflictsResult(_ctx, response);
        }
    }
}
