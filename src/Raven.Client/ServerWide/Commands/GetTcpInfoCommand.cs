using System.Net.Http;
using Raven.Client.Http;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Commands
{
    public class GetTcpInfoCommand : RavenCommand<TcpConnectionInfo>
    {
        private readonly string _tag;
        private readonly JsonOperationContext _ctx;
        private readonly string _dbName;

        public GetTcpInfoCommand(JsonOperationContext ctx, string tag)
        {
            _ctx = ctx;
            _tag = tag;
        }

        public GetTcpInfoCommand(JsonOperationContext ctx, string tag, string dbName = null) : this(ctx, tag)
        {
            _dbName = dbName;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            if (string.IsNullOrEmpty(_dbName))
            {
                url = $"{node.Url}/info/tcp?tag={_tag}";

            }
            else
            {
                url = $"{node.Url}/databases/{_dbName}/info/tcp?tag={_tag}";
                
            }
            RequestedNode = node;
            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Get
            };
            return request;
        }

        public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
                ThrowInvalidResponse();

            Result = JsonDeserializationClient.TcpConnectionInfo(_ctx, 
                response);
        }

        public ServerNode RequestedNode { get; private set; }

        public override bool IsReadRequest => true;   
    }
}
