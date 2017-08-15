﻿using System;
using System.Collections;
using System.Collections.Generic;

namespace Sparrow.Json
{
    public class BlittableJsonTraverser
    {
        public static BlittableJsonTraverser Default = new BlittableJsonTraverser();
        public static BlittableJsonTraverser FlatMapReduceResults = new BlittableJsonTraverser(new char[] { }); // map-reduce results have always a flat structure, let's ignore separators

        private const char PropertySeparator = '.';
        private const char CollectionSeparatorStart = '[';
        public static readonly char[] CollectionSeparator = { CollectionSeparatorStart, ']', PropertySeparator };

        public static readonly char[] PropertySeparators =
        {
            PropertySeparator
        };

        private readonly char[] _separators =
        {
            PropertySeparator,
            CollectionSeparatorStart
        };

        private BlittableJsonTraverser(char[] nonDefaultSeparators = null)
        {
            if (nonDefaultSeparators != null)
                _separators = nonDefaultSeparators;
        }

        public object Read(JsonOperationContext ctx, BlittableJsonReaderObject docReader, StringSegment path)
        {
            object result;
            StringSegment leftPath;
            if (TryRead(ctx, docReader, path, out result, out leftPath) == false)
                throw new InvalidOperationException($"Invalid path: {path}.");

            return result;
        }

        public bool TryRead(JsonOperationContext ctx, BlittableJsonReaderObject docReader, StringSegment path, out object result, out StringSegment leftPath)
        {
            var indexOfFirstSeparator = path.IndexOfAny(_separators, 0);
            object reader;

            //if not found -> indexOfFirstSeparator == -1 -> take whole includePath as segment
            if (docReader.TryGetMember(path.SubSegment(0, indexOfFirstSeparator), out reader) == false)
            {
                leftPath = path;
                result = null;
                return false;
            }

            if (indexOfFirstSeparator == -1)
            {
                leftPath = path;
                result = reader;
                return true;
            }
            

            switch (path[indexOfFirstSeparator])
            {
                case PropertySeparator:
                    var pathSegment = path.SubSegment(indexOfFirstSeparator + 1);

                    var propertyInnerObject = reader as BlittableJsonReaderObject;
                    if (propertyInnerObject != null)
                    {
                        if (TryRead(ctx, propertyInnerObject, pathSegment, out result, out leftPath))
                            return true;

                        if (result == null)
                            result = reader;

                        return false;
                    }

                    leftPath = pathSegment;
                    result = reader;
                    return false;
                case CollectionSeparatorStart:
                    leftPath = path.SubSegment(indexOfFirstSeparator + CollectionSeparator.Length);

                    var collectionInnerArray = reader as BlittableJsonReaderArray;
                    if (collectionInnerArray != null)
                    {
                        result = ReadArray(ctx, collectionInnerArray, leftPath);
                        return true;
                    }
                    result = reader;
                    return false;
                default:
                    throw new NotSupportedException($"Unhandled separator character: {path[indexOfFirstSeparator]}");
            }
        }

        private IEnumerable<object> ReadArray(JsonOperationContext ctx,BlittableJsonReaderArray array, StringSegment pathSegment)
        {
            // ReSharper disable once ForCanBeConvertedToForeach
            for (int i = 0; i < array.Length; i++)
            {
                var item = array.GetValueTokenTupleByIndex(ctx, i).Value;
                var arrayObject = item as BlittableJsonReaderObject;
                if (arrayObject != null)
                {
                    object result;
                    StringSegment leftPath;
                    if (TryRead(ctx, arrayObject, pathSegment, out result, out leftPath))
                    {
                        var enumerable = result as IEnumerable;

                        if (enumerable != null)
                        {
                            foreach (var nestedItem in enumerable)
                            {
                                yield return nestedItem;
                            }
                        }
                        else
                        {
                            yield return result;
                        }
                    }
                }
                else
                {
                    var arrayReader = item as BlittableJsonReaderArray;
                    if (arrayReader != null)
                    {
                        var indexOfFirstSeparatorInSubIndex = pathSegment.IndexOfAny(_separators, 0);

                        string subSegment;

                        switch (pathSegment[indexOfFirstSeparatorInSubIndex])
                        {
                            case PropertySeparator:
                                subSegment = pathSegment.SubSegment(indexOfFirstSeparatorInSubIndex + 1);
                                break;
                            case CollectionSeparatorStart:
                                subSegment = pathSegment.SubSegment(indexOfFirstSeparatorInSubIndex + 3);
                                break;
                            default:
                                throw new NotSupportedException($"Unhandled separator character: {pathSegment[indexOfFirstSeparatorInSubIndex]}");
                        }

                        foreach (var nestedItem in ReadArray(ctx, arrayReader, subSegment))
                        {
                            yield return nestedItem;
                        }
                    }
                    else
                    {
                        yield return item;
                    }
                }
            }
        }
    }
}
