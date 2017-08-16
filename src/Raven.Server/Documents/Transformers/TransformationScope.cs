﻿using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Parsing;
using Raven.Server.Documents.Includes;
using Raven.Server.Utils;
using Sparrow.Json;

namespace Raven.Server.Documents.Transformers
{
    public class TransformationScope : IDisposable
    {
        private readonly TransformerBase _transformer;
        private readonly DocumentsOperationContext _context;
        private readonly bool _nested;

        public TransformationScope(TransformerBase transformer, BlittableJsonReaderObject transformerParameters, IncludeDocumentsCommand include, DocumentsStorage documentsStorage, TransformerStore transformerStore, DocumentsOperationContext context, bool nested)
        {
            _transformer = transformer;
            _context = context;
            _nested = nested;
            if (_nested == false)
            {
                Debug.Assert(CurrentTransformationScope.Current == null);
                CurrentTransformationScope.Current = new CurrentTransformationScope(transformerParameters, include, documentsStorage, transformerStore, context);
            }
            else
                Debug.Assert(CurrentTransformationScope.Current != null);
        }

        public bool HasLoadedAnyDocument => LoadedDocumentChangeVectors != null && LoadedDocumentChangeVectors.Count > 0;

        public List<string> LoadedDocumentChangeVectors => CurrentTransformationScope.Current.LoadedDocumentChangeVectors;

        public void Dispose()
        {
            if (_nested == false)
                CurrentTransformationScope.Current = null;
        }

        public IEnumerable<dynamic> Transform(IEnumerable<dynamic> items)
        {
            foreach (var item in _transformer.TransformResults(items))
            {
                yield return item;
            }
        }

        public IEnumerable<Document> Transform(IEnumerable<Document> documents)
        {
            if (_transformer.HasGroupBy == false)
            {
                var docsEnumerator = new TransformedDocsEnumerator(documents, _transformer.TransformResults);

                while (docsEnumerator.MoveNext(out IEnumerable transformedResults))
                {
                    if (docsEnumerator.Current == null)
                    {
                        yield return Document.ExplicitNull;
                        continue;
                    }

                    try
                    {
                        var values = new DynamicJsonArray();
                        var result = new DynamicJsonValue
                        {
                            ["$values"] = values
                        };

                        foreach (var transformedResult in transformedResults)
                        {
                            var value = TypeConverter.ToBlittableSupportedType(transformedResult);
                            values.Add(value);
                        }

                        var document = new Document
                        {
                            Id = docsEnumerator.Current.Id,
                            Data = _context.ReadObject(result, docsEnumerator.Current.Id ?? string.Empty),
                            Etag = docsEnumerator.Current.Etag,
                            StorageId = docsEnumerator.Current.StorageId
                        };

                        yield return document;
                    }
                    finally
                    {
                        docsEnumerator.Current.Data?.Dispose(_context);
                    }
                }
            }
            else
            {
                var groupByEnumerationWrapper = new GroupByTransformationWrapper(_context, documents);

                var values = new DynamicJsonArray();
                var result = new DynamicJsonValue
                {
                    ["$values"] = values
                };

                foreach (var transformedResult in _transformer.TransformResults(groupByEnumerationWrapper))
                {
                    if (transformedResult == null)
                    {
                        yield return Document.ExplicitNull;
                        continue;
                    }

                    var value = TypeConverter.ToBlittableSupportedType(transformedResult);
                    values.Add(value);
                }

                var document = new Document
                {
                    Data = _context.ReadObject(result, string.Empty)
                };

                yield return document;
            }
        }

        private class GroupByTransformationWrapper : IEnumerable<DynamicBlittableJson>
        {
            private readonly Enumerator _enumerator = new Enumerator();

            public GroupByTransformationWrapper(JsonOperationContext ctx, IEnumerable<Document> docs)
            {
                _enumerator.Initialize(ctx, docs.GetEnumerator());
            }

            public IEnumerator<DynamicBlittableJson> GetEnumerator()
            {
                return _enumerator;
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }

            private class Enumerator : IEnumerator<DynamicBlittableJson>
            {
                private IEnumerator<Document> _items;
                private JsonOperationContext _ctx;

                public void Initialize(JsonOperationContext ctx,IEnumerator<Document> items)
                {
                    _items = items;
                    _ctx = ctx;
                }

                public bool MoveNext()
                {
                    if (_items.MoveNext() == false)
                        return false;

                    Current = new DynamicBlittableJson(_ctx, _items.Current); // we have to create new instance to properly GroupBy

                    CurrentTransformationScope.Current.Source = Current;
                    return true;
                }

                public void Reset()
                {
                    throw new NotImplementedException();
                }

                public DynamicBlittableJson Current { get; private set; }

                object IEnumerator.Current => Current;

                public void Dispose()
                {

                }
            }
        }
    }
}
