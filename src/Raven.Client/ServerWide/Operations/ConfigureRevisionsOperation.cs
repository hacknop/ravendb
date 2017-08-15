﻿using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
using Raven.Client.ServerWide.Revisions;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations
{
    public class ConfigureRevisionsOperation : IServerOperation<ConfigureRevisionsOperationResult>
    {
        private readonly RevisionsConfiguration _configuration;
        private readonly string _databaseName;

        public ConfigureRevisionsOperation(RevisionsConfiguration configuration, string databaseName)
        {
            _configuration = configuration;
            _databaseName = databaseName;
        }
        public RavenCommand<ConfigureRevisionsOperationResult> GetCommand(DocumentConventions conventions, JsonOperationContext ctx)
        {
            return new ConfigureRevisionsCommand(ctx, _configuration, _databaseName);
        }
    }

    public class ConfigureRevisionsCommand : RavenCommand<ConfigureRevisionsOperationResult>
    {
        private readonly JsonOperationContext _ctx;
        private readonly RevisionsConfiguration _configuration;
        private readonly string _databaseName;

        public ConfigureRevisionsCommand(JsonOperationContext ctx, RevisionsConfiguration configuration, string databaseName)
        {
            _ctx = ctx;
            _configuration = configuration;
            _databaseName = databaseName;
        }

        public override bool IsReadRequest => false;

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/admin/revisions/config?name={_databaseName}";

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

            Result = JsonDeserializationClient.ConfigureRevisionsOperationResult(_ctx, response);
        }
    }

    public class ConfigureRevisionsOperationResult
    {
        public long? ETag { get; set; }
    }
}
