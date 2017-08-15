﻿using System;
using System.Net.Http;
using System.Text;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Queries.Facets;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.Documents.Commands
{
    public class GetFacetsCommand : RavenCommand<FacetedQueryResult>
    {
        private readonly JsonOperationContext _context;
        private readonly DocumentConventions _conventions;
        private readonly FacetQuery _query;

        public GetFacetsCommand(DocumentConventions conventions, JsonOperationContext context, FacetQuery query)
        {
            _context = context;
            _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
            _query = query ?? throw new ArgumentNullException(nameof(query));

            if (_query.WaitForNonStaleResultsTimeout.HasValue && _query.WaitForNonStaleResultsTimeout != TimeSpan.MaxValue)
                Timeout = _query.WaitForNonStaleResultsTimeout.Value.Add(TimeSpan.FromSeconds(10)); // giving the server an opportunity to finish the response
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            if (string.IsNullOrWhiteSpace(_query.FacetSetupDoc) == false && _query.Facets != null && _query.Facets.Count > 0)
                throw new InvalidOperationException($"You cannot specify both '{nameof(FacetQuery.FacetSetupDoc)}' and '{nameof(FacetQuery.Facets)}'.");

            var path = new StringBuilder(node.Url)
                .Append("/databases/")
                .Append(node.Database)
                .Append("/queries?op=facets&query-hash=")
                .Append(_query.GetQueryHash(ctx));

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Post,
                Content = new BlittableJsonContent(stream =>
                {
                    using (var writer = new BlittableJsonTextWriter(ctx, stream))
                    {
                        writer.WriteFacetQuery(_conventions, ctx, _query);
                    }
                })
            };

            url = path.ToString();
            return request;
        }

        public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
            {
                Result = null;
                return;
            }

            Result = JsonDeserializationClient.FacetedQueryResult(_context, response);
        }

        public override bool IsReadRequest => true;
    }
}
