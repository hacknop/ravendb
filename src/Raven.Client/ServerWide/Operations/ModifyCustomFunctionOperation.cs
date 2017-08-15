﻿using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.ServerWide.Operations
{
    public class ModifyCustomFunctionsOperation : IServerOperation<ModifyCustomFunctionsResult>
    {
        private readonly string _database;
        private readonly string _functions;

        public ModifyCustomFunctionsOperation(string database, string functions)
        {
            _database = database;
            _functions = functions;
        }

        public RavenCommand<ModifyCustomFunctionsResult> GetCommand(DocumentConventions conventions, JsonOperationContext ctx)
        {
            return new ModifyCustomFunctionsCommand(ctx, _database, _functions);
        }
    }

    public class ModifyCustomFunctionsCommand : RavenCommand<ModifyCustomFunctionsResult>
    {
        private readonly JsonOperationContext _ctx;
        private readonly string _database;
        private readonly string _functions;
        public override bool IsReadRequest => false;

        public ModifyCustomFunctionsCommand(JsonOperationContext ctx, string database, string functions)
        {
            _ctx = ctx;
            _database = database;
            _functions = functions;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/admin/modify-custom-functions?name={_database}";
            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Post,
                Content = new BlittableJsonContent(stream =>
                {
                    var json = new DynamicJsonValue
                    {
                        ["Functions"] = _functions
                    };

                    ctx.Write(stream, ctx.ReadObject(json, "modify-custom-functions"));
                })
            };

            return request;
        }

        public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
                ThrowInvalidResponse();

            Result = JsonDeserializationClient.ModifyCustomFunctionResult(_ctx, response);
        }
    }
}
