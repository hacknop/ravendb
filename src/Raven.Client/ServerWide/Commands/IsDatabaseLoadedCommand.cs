﻿using System.Net.Http;
using Raven.Client.Http;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Commands
{   
    public class IsDatabaseLoadedCommand : RavenCommand<IsDatabaseLoadedCommand.CommandResult>
    {
        private readonly JsonOperationContext _ctx;

        public class CommandResult
        {
            public string DatabaseName;
            public bool IsLoaded;
        }

        public override bool IsReadRequest => true;

        public IsDatabaseLoadedCommand(JsonOperationContext ctx)
        {
            _ctx = ctx;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            //not sure if we need to escape database name here
            url = $"{node.Url}/debug/is-loaded?name={node.Database}";
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

            Result = JsonDeserializationClient.IsDatabaseLoadedCommandResult(_ctx, response);
        }
    }
}
