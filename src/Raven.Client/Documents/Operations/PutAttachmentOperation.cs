﻿using System;
using System.IO;
using System.Net.Http;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json.Converters;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations
{
    public class PutAttachmentOperation : IOperation<AttachmentDetails>
    {
        private readonly string _documentId;
        private readonly string _name;
        private readonly Stream _stream;
        private readonly string _contentType;
        private readonly string _changeVector;

        public PutAttachmentOperation(string documentId, string name, Stream stream, string contentType = null, string changeVector = null)
        {
            _documentId = documentId;
            _name = name;
            _stream = stream;
            _contentType = contentType;
            _changeVector = changeVector;
        }

        public RavenCommand<AttachmentDetails> GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
        {
            return new PutAttachmentCommand(context, _documentId, _name, _stream, _contentType, _changeVector);
        }

        private class PutAttachmentCommand : RavenCommand<AttachmentDetails>
        {
            private readonly JsonOperationContext _ctx;
            private readonly string _documentId;
            private readonly string _name;
            private readonly Stream _stream;
            private readonly string _contentType;
            private readonly string _changeVector;

            public PutAttachmentCommand(
                JsonOperationContext ctx,
                string documentId,
                string name, 
                Stream stream, 
                string contentType, 
                string changeVector)
            {
                if (string.IsNullOrWhiteSpace(documentId))
                    throw new ArgumentNullException(nameof(documentId));
                if (string.IsNullOrWhiteSpace(name))
                    throw new ArgumentNullException(nameof(name));

                _ctx = ctx;
                _documentId = documentId;
                _name = name;
                _stream = stream;
                _contentType = contentType;
                _changeVector = changeVector;

                PutAttachmentCommandHelper.ValidateStream(stream);
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                PutAttachmentCommandHelper.PrepareStream(_stream);

                url = $"{node.Url}/databases/{node.Database}/attachments?id={Uri.EscapeDataString(_documentId)}&name={Uri.EscapeDataString(_name)}";
                if (string.IsNullOrWhiteSpace(_contentType) == false)
                    url += $"&contentType={Uri.EscapeDataString(_contentType)}";
                var request = new HttpRequestMessage
                {
                    Method = HttpMethods.Put,
                    Content = new AttachmentStreamContent(_stream, CancellationToken)
                };

                AddChangeVectorIfNotNull(_changeVector, request);

                return request;
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
                Result = JsonDeserializationClient.AttachmentDetails(_ctx, response);
            }

            public override bool IsReadRequest => false;
        }
    }
}
