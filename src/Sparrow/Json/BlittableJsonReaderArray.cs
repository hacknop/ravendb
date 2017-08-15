using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Sparrow.Collections;
using Sparrow.Json.Parsing;

namespace Sparrow.Json
{
    public unsafe class BlittableJsonReaderArray : BlittableJsonReaderBase
    {
        private readonly int _count;
        private readonly byte* _metadataPtr;
        private readonly byte* _dataStart;
        private readonly long _currentOffsetSize;
        private FastDictionary<int, (object Value, BlittableJsonToken Type), NumericEqualityComparer> _cache;

        public DynamicJsonArray Modifications;

        public BlittableJsonReaderObject Parent => _parent;
      
        public BlittableJsonReaderArray(int pos, BlittableJsonReaderObject parent, BlittableJsonToken type)
        {
            _parent = parent;

            byte arraySizeOffset;
            _count = parent.ReadVariableSizeInt(pos, out arraySizeOffset);

            _dataStart = parent.BasePointer + pos;
            _metadataPtr = _dataStart + arraySizeOffset;

            // analyze main object type and it's offset and propertyIds flags
            _currentOffsetSize = ProcessTokenOffsetFlags(type);
        }

        public int Length => _count;

        public BlittableJsonToken GetArrayType()
        {
            var blittableJsonToken = (BlittableJsonToken)(*(_metadataPtr + _currentOffsetSize)) & TypesMask;
            Debug.Assert(blittableJsonToken != 0);
            return blittableJsonToken;
        }

        public T GetByIndex<T>(JsonOperationContext ctx,int index)
        {
            var obj = GetValueTokenTupleByIndex(ctx, index).Value;
            T result;
            BlittableJsonReaderObject.ConvertType(obj, out result);
            return result;
        }

        public string GetStringByIndex(JsonOperationContext ctx, int index)
        {
            var obj = GetValueTokenTupleByIndex(ctx, index).Value;
            if (obj == null)
                return null;

            var lazyStringValue = obj as LazyStringValue;
            if (lazyStringValue != (LazyStringValue)null)
                return (string)lazyStringValue;
            var lazyCompressedStringValue = obj as LazyCompressedStringValue;
            if (lazyCompressedStringValue != null)
                return lazyCompressedStringValue;
            string result;
            BlittableJsonReaderObject.ConvertType(obj, out result);
            return result;

        }

        public void AddItemsToStream<T>(JsonOperationContext ctx, ManualBlittableJsonDocumentBuilder<T> writer) where T : struct, IUnmanagedWriteBuffer
        {
            for (var i = 0; i < _count; i++)
            {
                var tuple = GetValueTokenTupleByIndex(ctx, i);
                writer.WriteValue(tuple.Item2, tuple.Item1);
            }
        }

        public (object Value, BlittableJsonToken Type) GetValueTokenTupleByIndex(JsonOperationContext ctx, int index)
        {
            // try get value from cache, works only with Blittable types, other objects are not stored for now
            (object Value, BlittableJsonToken Type) result;
            if (NoCache == false && _cache != null && _cache.TryGetValue(index, out result))
                return result;

            if (index >= _count || index < 0)
                throw new IndexOutOfRangeException($"Cannot access index {index} when our size is {_count}");

            var itemMetadataStartPtr = _metadataPtr + index * (_currentOffsetSize + 1);
            var offset = ReadNumber(itemMetadataStartPtr, _currentOffsetSize);
            var token = *(itemMetadataStartPtr + _currentOffsetSize);
            result = (
                _parent.GetObject(ctx, (BlittableJsonToken)token,(int)(_dataStart - _parent.BasePointer - offset)), 
                (BlittableJsonToken)token & TypesMask
                );

            var blittableJsonReaderBase = result.Item1 as BlittableJsonReaderBase;
            if (blittableJsonReaderBase  != null)
            {
                blittableJsonReaderBase.NoCache = NoCache;
                if (NoCache == false)
                {
                    if (_cache == null)
                    {
                        _cache = new FastDictionary<int, (object Value, BlittableJsonToken Type), NumericEqualityComparer>(NumericEqualityComparer.Instance);
                    }
                    _cache[index] = result;
                }
            }
            return result;
        }

        public IEnumerable<object> GetItems(JsonOperationContext ctx)
        {
            for (int i = 0; i < _count; i++)
                yield return GetValueTokenTupleByIndex(ctx, i).Value;
        }
    }
}
