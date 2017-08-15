﻿using System;
using System.Linq;
using System.Linq.Expressions;
using System.Net.Http;
using System.Text;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Session;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations
{
    public class PatchByQueryOperation<TEntity, TIndexCreator> : PatchByQueryOperation<TEntity>
        where TIndexCreator : AbstractIndexCreationTask, new()
    {
        public PatchByQueryOperation(Expression<Func<TEntity, bool>> expression, PatchRequest patch, QueryOperationOptions options = null)
            : base(new TIndexCreator().IndexName, expression, patch, options)
        {
        }
    }

    public class PatchByQueryOperation<TEntity> : PatchByQueryOperation
    {
        private readonly string _indexName;
        private readonly Expression<Func<TEntity, bool>> _expression;

        public PatchByQueryOperation(string indexName, Expression<Func<TEntity, bool>> expression, PatchRequest patch, QueryOperationOptions options = null)
            : base(DummyQuery, patch, options)
        {
            _indexName = indexName ?? throw new ArgumentNullException(nameof(indexName));
            _expression = expression ?? throw new ArgumentNullException(nameof(expression));
        }

        public override RavenCommand<OperationIdResult> GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
        {
            if (_queryToUpdate == DummyQuery)
            {
                using (var session = store.OpenSession())
                {
                    var query = session.Query<TEntity>(_indexName).Where(_expression);

                    var inspector = (IRavenQueryInspector)query;

                    _queryToUpdate = inspector.GetIndexQuery(isAsync: false);
                }
            }

            return base.GetCommand(store, conventions, context, cache);
        }
    }

    public class PatchByQueryOperation : IOperation<OperationIdResult>
    {
        protected static IndexQuery DummyQuery = new IndexQuery();

        protected IndexQuery _queryToUpdate;
        private readonly PatchRequest _patch;
        private readonly QueryOperationOptions _options;

        public PatchByQueryOperation(IndexQuery queryToUpdate, PatchRequest patch, QueryOperationOptions options = null)
        {
            _queryToUpdate = queryToUpdate ?? throw new ArgumentNullException(nameof(queryToUpdate));
            _patch = patch ?? throw new ArgumentNullException(nameof(patch));
            _options = options;
        }

        public virtual RavenCommand<OperationIdResult> GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
        {
            return new PatchByIndexCommand(conventions, context, _queryToUpdate, _patch, _options);
        }

        private class PatchByIndexCommand : RavenCommand<OperationIdResult>
        {
            private readonly JsonOperationContext _context;
            private readonly DocumentConventions _conventions;
            private readonly IndexQuery _queryToUpdate;
            private readonly BlittableJsonReaderObject _patch;
            private readonly QueryOperationOptions _options;

            public PatchByIndexCommand(DocumentConventions conventions, JsonOperationContext context, IndexQuery queryToUpdate, PatchRequest patch, QueryOperationOptions options = null)
            {
                if (patch == null)
                    throw new ArgumentNullException(nameof(patch));
                _context = context;

                _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
                if (context == null)
                    throw new ArgumentNullException(nameof(context));
                _queryToUpdate = queryToUpdate ?? throw new ArgumentNullException(nameof(queryToUpdate));
                _patch = EntityToBlittable.ConvertEntityToBlittable(patch, conventions, context);
                _options = options ?? new QueryOperationOptions();
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                var path = new StringBuilder(node.Url)
                    .Append("/databases/")
                    .Append(node.Database)
                    .Append("/queries")
                    .Append("?allowStale=")
                    .Append(_options.AllowStale)
                    .Append("&maxOpsPerSec=")
                    .Append(_options.MaxOpsPerSecond)
                    .Append("&details=")
                    .Append(_options.RetrieveDetails);

                if (_options.StaleTimeout != null)
                {
                    path
                        .Append("&staleTimeout=")
                        .Append(_options.StaleTimeout.Value);
                }

                var request = new HttpRequestMessage
                {
                    Method = HttpMethods.Patch,
                    Content = new BlittableJsonContent(stream =>
                        {
                            using (var writer = new BlittableJsonTextWriter(ctx, stream))
                            {
                                writer.WriteStartObject();

                                writer.WritePropertyName("Query");
                                writer.WriteIndexQuery(_conventions, ctx, _queryToUpdate);
                                writer.WriteComma();

                                writer.WritePropertyName("Patch");
                                writer.WriteObject(_patch);

                                writer.WriteEndObject();
                            }
                        }
                    )
                };

                url = path.ToString();
                return request;
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonDeserializationClient.OperationIdResult(_context, response);
            }

            public override bool IsReadRequest => false;
        }
    }
}
