﻿using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations
{
    public class AddDatabaseNodeOperation : IServerOperation<DatabasePutResult>
    {
        private readonly string _databaseName;
        private readonly string _node;

        public AddDatabaseNodeOperation(string databaseName, string node = null)
        {
            Helpers.AssertValidDatabaseName(databaseName);
            _databaseName = databaseName;
            _node = node;
        }

        public RavenCommand<DatabasePutResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new AddDatabaseNodeCommand(context, _databaseName, _node);
        }

        private class AddDatabaseNodeCommand : RavenCommand<DatabasePutResult>
        {
            private readonly JsonOperationContext _ctx;
            private readonly string _databaseName;
            private readonly string _node;

            public AddDatabaseNodeCommand(JsonOperationContext ctx, string databaseName, string node)
            {
                if (string.IsNullOrEmpty(databaseName))
                    throw new ArgumentNullException(databaseName);

                _ctx = ctx;
                _databaseName = databaseName;
                _node = node;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/databases/node?name={_databaseName}";
                if (string.IsNullOrEmpty(_node) == false)
                {
                    url += $"&node={_node}";
                }

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Put
                };

                return request;
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonDeserializationClient.DatabasePutResult(_ctx, response);
            }

            public override bool IsReadRequest => false;
        }
    }
}
