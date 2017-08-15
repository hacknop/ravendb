﻿using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Session;
using Raven.Client.Extensions;
using Sparrow.Json;

namespace Raven.Client.Documents.Transformers
{
    internal static class TransformerHelper
    {
        public static Dictionary<string, T> ParseResultsForLoadOperation<T>(InMemoryDocumentSessionOperations session, GetDocumentResult transformedResult, List<string> ids = null)
        {
            var result = new Dictionary<string, T>(StringComparer.OrdinalIgnoreCase);
            var resultType = typeof(T);
            var allowMultiple = resultType.IsArray;

            for (var i = 0; i < transformedResult.Results.Length; i++)
            {
                var item = transformedResult.Results.GetValueTokenTupleByIndex(session.Context, i).Value as BlittableJsonReaderObject;
                if (item == null)
                {
                    if (ids != null)
                        result[ids[i]] = default(T);

                    continue;
                }

                var metadata = item.GetMetadata();
                var id = metadata.GetId();

                result[id] = ParseSingleResult<T>(item, session, allowMultiple, resultType);
            }

            return result;
        }

        public static IEnumerable<T> ParseResultsForQueryOperation<T>(InMemoryDocumentSessionOperations session, QueryResultBase<BlittableJsonReaderArray> transformedResult)
        {
            for (var i = 0; i < transformedResult.Results.Length; i++)
            {
                var item = transformedResult.Results.GetValueTokenTupleByIndex(session.Context, i).Value as BlittableJsonReaderObject;
                if (item == null)
                    continue;

                BlittableJsonReaderArray values;
                if (item.TryGet(Constants.Json.Fields.Values, out values) == false)
                    throw new InvalidOperationException("Transformed document must have a $values property");

                foreach (var parsedItem in ParseValuesFromBlittableArray(typeof(T), session, values).Cast<T>())
                    yield return parsedItem;
            }
        }

        public static IEnumerable<T> ParseResultsForStreamOperation<T>(InMemoryDocumentSessionOperations session, BlittableJsonReaderArray array)
        {
            return ParseValuesFromBlittableArray(typeof(T), session, array)
                .Cast<T>();
        }

        private static T ParseSingleResult<T>(BlittableJsonReaderObject item, InMemoryDocumentSessionOperations session, bool allowMultiple, Type resultType)
        {
            if (item == null)
                return default(T);

            BlittableJsonReaderArray values;
            if (item.TryGet(Constants.Json.Fields.Values, out values) == false)
                throw new InvalidOperationException("Transformed document must have a $values property");

            if (allowMultiple == false && values.Length > 1)
                throw new InvalidOperationException(
                    string.Format("An operation was attempted with transformer and more than one item was returned per entity - please use {0}[] as the projection type instead of {0}", typeof(T).Name));

            if (allowMultiple)
            {
                var elementType = typeof(T).GetElementType();

                var array = ParseValuesFromBlittableArray(elementType, session, values).ToArray();
                var newArray = Array.CreateInstance(elementType, array.Length);
                Array.Copy(array, newArray, array.Length);

                return (T)(object)newArray;
            }

            return ParseValuesFromBlittableArray(resultType, session, values)
                .Cast<T>()
                .FirstOrDefault();
        }

        private static IEnumerable<object> ParseValuesFromBlittableArray(Type type, InMemoryDocumentSessionOperations session, BlittableJsonReaderArray array)
        {
            for (var i = 0; i < array.Length; i++)
            {
                var val = array.GetValueTokenTupleByIndex(session.Context, i);

                switch (val.Type)
                {
                    case BlittableJsonToken.StartArray:
                        foreach (var inner in ParseValuesFromBlittableArray(type, session, val.Value as BlittableJsonReaderArray))
                            yield return inner;
                        break;
                    case BlittableJsonToken.StartObject:
                        yield return session.DeserializeFromTransformer(type, null, val.Value as BlittableJsonReaderObject);
                        break;
                    case BlittableJsonToken.String:
                        var lazyString = val.Value as LazyStringValue;
                        if (lazyString != null)
                            yield return lazyString.ToString();
                        break;
                    case BlittableJsonToken.CompressedString:
                        var lazyCompressedString = val.Value as LazyCompressedStringValue;
                        if (lazyCompressedString != null)
                            yield return lazyCompressedString.ToString();
                        break;
                    default:
                        // TODO, check if other types need special handling as well
                        yield return val.Value;
                        break;
                }
            }
        }
    }
}
