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
using Sparrow.Json;

namespace Raven.Client.Documents.Operations
{
    public class DeleteByIndexOperation<TEntity, TIndexCreator> : DeleteByIndexOperation<TEntity>
        where TIndexCreator : AbstractIndexCreationTask, new()
    {
        public DeleteByIndexOperation(Expression<Func<TEntity, bool>> expression, QueryOperationOptions options = null)
            : base(new TIndexCreator().IndexName, expression, options)
        {
        }
    }

    public class DeleteByIndexOperation<TEntity> : DeleteByIndexOperation
    {
        private readonly string _indexName;
        private readonly Expression<Func<TEntity, bool>> _expression;

        public DeleteByIndexOperation(string indexName, Expression<Func<TEntity, bool>> expression, QueryOperationOptions options = null)
            : base( DummyQuery, options)
        {
            _indexName = indexName ?? throw new ArgumentNullException(nameof(indexName));
            _expression = expression ?? throw new ArgumentNullException(nameof(expression));
        }

        public override RavenCommand<OperationIdResult> GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
        {
            if (_queryToDelete == DummyQuery)
            {
                using (var session = store.OpenSession())
                {
                    var query = session
                        .Query<TEntity>(_indexName)
                        .Where(_expression);

                    var inspector = (IRavenQueryInspector)query;

                    _queryToDelete = inspector.GetIndexQuery(isAsync: false);
                }
            }

            return base.GetCommand(store, conventions, context, cache);
        }
    }

    public class DeleteByIndexOperation : IOperation<OperationIdResult>
    {
        protected static IndexQuery DummyQuery = new IndexQuery();

        protected IndexQuery _queryToDelete;
        private readonly QueryOperationOptions _options;

        public DeleteByIndexOperation(IndexQuery queryToDelete, QueryOperationOptions options = null)
        {
            _queryToDelete = queryToDelete ?? throw new ArgumentNullException(nameof(queryToDelete));
            _options = options;
        }

        public virtual RavenCommand<OperationIdResult> GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
        {
            return new DeleteByIndexCommand(context, conventions, _queryToDelete, _options);
        }

        private class DeleteByIndexCommand : RavenCommand<OperationIdResult>
        {
            private readonly JsonOperationContext _ctx;
            private readonly DocumentConventions _conventions;
            private readonly IndexQuery _queryToDelete;
            private readonly QueryOperationOptions _options;

            public DeleteByIndexCommand(JsonOperationContext ctx, DocumentConventions conventions, IndexQuery queryToDelete, QueryOperationOptions options = null)
            {
                _ctx = ctx;
                _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
                _queryToDelete = queryToDelete ?? throw new ArgumentNullException(nameof(queryToDelete));
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
                    Method = HttpMethod.Delete,
                    Content = new BlittableJsonContent(stream =>
                        {
                            using (var writer = new BlittableJsonTextWriter(ctx, stream))
                            {
                                writer.WriteIndexQuery(_conventions, ctx, _queryToDelete);
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

                Result = JsonDeserializationClient.OperationIdResult(_ctx, response);
            }

            public override bool IsReadRequest => false;
        }
    }
}
