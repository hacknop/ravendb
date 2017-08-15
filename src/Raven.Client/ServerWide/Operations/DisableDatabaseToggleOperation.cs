﻿using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations
{
    public class DisableDatabaseToggleOperation : IServerOperation<DisableDatabaseToggleResult>
    {
        private readonly string _databaseName;
        private readonly bool _ifDisableRequest;

        public DisableDatabaseToggleOperation(string databaseName, bool ifDisableRequest)
        {
            _databaseName = databaseName ?? throw new ArgumentNullException(nameof(databaseName));
            _ifDisableRequest = ifDisableRequest;
        }

        public RavenCommand<DisableDatabaseToggleResult> GetCommand(DocumentConventions conventions, JsonOperationContext ctx)
        {
            return new DisableDatabaseToggleCommand(ctx, _databaseName, _ifDisableRequest);
        }

        public class DisableDatabaseToggleCommand : RavenCommand<DisableDatabaseToggleResult>
        {
            private readonly JsonOperationContext _ctx;
            private readonly string _databaseName;
            private readonly bool _ifDisableRequest;

            public DisableDatabaseToggleCommand(JsonOperationContext ctx, string databaseName, bool ifDisableRequest)
            {
                _ctx = ctx;
                _databaseName = databaseName;
                _ifDisableRequest = ifDisableRequest;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                var toggle = _ifDisableRequest ? "disable" : "enable";
                url = $"{node.Url}/admin/databases/{toggle}?name={_databaseName}";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Post
                };
            }
        
            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null ||
                    response.TryGet("Status", out BlittableJsonReaderArray databases) == false)
                {
                    ThrowInvalidResponse();
                    return; // never hit
                }

                var resultObject = databases.GetValueTokenTupleByIndex(_ctx, 0).Value as BlittableJsonReaderObject;
                Result = JsonDeserializationClient.DisableResourceToggleResult(_ctx, resultObject);
            }

            public override bool IsReadRequest => false;
        }
    }
}
