using System;
using System.Net.Http;
using System.Text;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.Documents.Commands
{
    public class GetRevisionsBinEntryCommand : RavenCommand<BlittableArrayResult>
    {
        private readonly JsonOperationContext _ctx;
        private readonly long _etag;
        private readonly int? _pageSize;

        public GetRevisionsBinEntryCommand(JsonOperationContext ctx, long etag, int? pageSize)
        {
            _ctx = ctx;
            _etag = etag;
            _pageSize = pageSize;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Get
            };

            var pathBuilder = new StringBuilder(node.Url);
            pathBuilder.Append("/databases/")
                .Append(node.Database)
                .Append("/revisions/bin?&etag=")
                .Append(_etag);

            if (_pageSize.HasValue)
                pathBuilder.Append("&pageSize=").Append(_pageSize);

            url = pathBuilder.ToString();
            return request;
        }

        public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
                throw new InvalidOperationException();
            Result = JsonDeserializationClient.BlittableArrayResult(_ctx, response);
        }

        public override bool IsReadRequest => true;
    }
}
