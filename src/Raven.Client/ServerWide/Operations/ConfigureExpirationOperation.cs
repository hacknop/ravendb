﻿using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
using Raven.Client.ServerWide.Expiration;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations
{
    public class ConfigureExpirationOperation : IServerOperation<ConfigureExpirationOperationResult>
    {
        private readonly ExpirationConfiguration _configuration;
        private readonly string _databaseName;

        public ConfigureExpirationOperation(ExpirationConfiguration configuration, string databaseName)
        {
            _configuration = configuration;
            _databaseName = databaseName;
        }

        public RavenCommand<ConfigureExpirationOperationResult> GetCommand(DocumentConventions conventions, JsonOperationContext ctx)
        {
            return new ConfigureExpirationCommand(ctx, _configuration, _databaseName);
        }

        public class ConfigureExpirationCommand : RavenCommand<ConfigureExpirationOperationResult>
        {
            private readonly JsonOperationContext _ctx;
            private readonly ExpirationConfiguration _configuration;
            private readonly string _databaseName;

            public ConfigureExpirationCommand(JsonOperationContext ctx, ExpirationConfiguration configuration, string databaseName)
            {
                _ctx = ctx;
                _configuration = configuration;
                _databaseName = databaseName;
            }

            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/expiration/config?name={_databaseName}";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = new BlittableJsonContent(stream =>
                    {
                        var config = EntityToBlittable.ConvertEntityToBlittable(_configuration, DocumentConventions.Default, ctx);
                        ctx.Write(stream, config);
                    })
                };

                return request;
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonDeserializationClient.ConfigureExpirationOperationResult(_ctx, response);
            }
        }
    }
}
