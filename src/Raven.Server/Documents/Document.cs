﻿using System;
using System.Globalization;
using Raven.Client;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents
{
    public class Document
    {
        public static readonly Document ExplicitNull = new Document();

        private ulong? _hash;
        private bool _metadataEnsured;

        public long Etag;
        public LazyStringValue Id;
        public LazyStringValue LowerId;
        public long StorageId;
        public BlittableJsonReaderObject Data;
        public float? IndexScore;
        public string ChangeVector;
        public DateTime LastModified;
        public DocumentFlags Flags;
        public NonPersistentDocumentFlags NonPersistentFlags;
        public short TransactionMarker;

        public unsafe ulong DataHash
        {
            get
            {
                if (_hash.HasValue == false)
                    _hash = Hashing.XXHash64.Calculate(Data.BasePointer, (ulong)Data.Size);

                return _hash.Value;
            }
        }

        public void EnsureMetadata(DocumentsOperationContext context = null)
        {
            if (_metadataEnsured)
                return;

            _metadataEnsured = true;
            DynamicJsonValue mutatedMetadata;
            if (Data.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata))
            {
                if (metadata.Modifications == null)
                    metadata.Modifications = new DynamicJsonValue(metadata);

                mutatedMetadata = metadata.Modifications;
            }
            else
            {
                Data.Modifications = new DynamicJsonValue(Data)
                {
                    [Constants.Documents.Metadata.Key] = mutatedMetadata = new DynamicJsonValue()
                };
            }
            mutatedMetadata[Constants.Documents.Metadata.Id] = Id;
            if (ChangeVector != null)
                mutatedMetadata[Constants.Documents.Metadata.ChangeVector] = ChangeVector;
            if (Flags != DocumentFlags.None)
                mutatedMetadata[Constants.Documents.Metadata.Flags] = Flags.ToString();
            if (IndexScore.HasValue)
                mutatedMetadata[Constants.Documents.Metadata.IndexScore] = IndexScore;

            _hash = null;
        }

        public bool Expired(DateTime currentDate)
        {
            if (Data.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata) == false || 
                metadata.TryGet(Constants.Documents.Expiration.ExpirationDate, out string expirationDate) == false)
                return false;

            var expirationDateTime = DateTime.ParseExact(expirationDate, new[] {"o", "r"}, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);
            return expirationDateTime < currentDate;
        }
    }
}
