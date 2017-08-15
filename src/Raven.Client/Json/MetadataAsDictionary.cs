﻿using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Raven.Client.Documents.Session;
using Sparrow.Json;

namespace Raven.Client.Json
{
    internal class MetadataAsDictionary : IMetadataDictionary
    {
        private IDictionary<string, object> _metadata;
        private readonly JsonOperationContext _ctx;
        private readonly BlittableJsonReaderObject _source;

        public MetadataAsDictionary(JsonOperationContext ctx, BlittableJsonReaderObject metadata)
        {
            _ctx = ctx;
            _source = metadata;
        }

        public MetadataAsDictionary(JsonOperationContext ctx, Dictionary<string, object> metadata)
        {
            _ctx = ctx;
            _metadata = metadata;
        }

        private void Init()
        {
            _metadata = new Dictionary<string, object>();
            var indexes = _source.GetPropertiesByInsertionOrder();
            foreach (var index in indexes)
            {
                var propDetails = new BlittableJsonReaderObject.PropertyDetails();
                _source.GetPropertyByIndex(_ctx, index, ref propDetails);
                _metadata[propDetails.Name] = ConvertValue(propDetails.Value);
            }
        }

        private object ConvertValue(object value)
        {
            if (value == null)
                return null;

            if (value is LazyStringValue || value is LazyCompressedStringValue)
                return value.ToString();

            if (value is long)
                return (long)value;

            if (value is bool)
                return (bool)value;

            var doubleValue = value as LazyNumberValue;
            if (doubleValue != null)
                return (double)doubleValue;

            var obj = value as BlittableJsonReaderObject;
            if (obj != null)
                return new MetadataAsDictionary(_ctx, obj);

            var array = value as BlittableJsonReaderArray;
            if (array != null)
            {
                var result = new object[array.Length];
                for (int i = 0; i < array.Length; i++)
                {
                    result[i] = ConvertValue(array.GetValueTokenTupleByIndex(_ctx, i).Value);
                }
                return result;
            }

            throw new NotImplementedException("Implement support for numbers and more");
        }

        public object this[string key]
        {
            get
            {
                if (_metadata != null)
                    return _metadata[key];
                object value;
                if (_source.TryGetMember(key, out value))
                    return ConvertValue(value);

                throw new KeyNotFoundException(key + " is not in the metadata");
            }

            set
            {
                if (_metadata == null)
                    Init();
                Debug.Assert(_metadata != null);
                _metadata[key] = value;
            }
        }

        public bool Changed => _metadata != null;

        public int Count => _metadata?.Count ?? _source.GetPropertiesByInsertionOrder().Length;

        public bool IsReadOnly => _metadata != null && _metadata.IsReadOnly;

        public ICollection<string> Keys => _metadata != null ? _metadata.Keys : _source.GetPropertyNames(_ctx);

        public ICollection<object> Values
        {
            get
            {
                if (_metadata != null)
                    return _metadata.Values;
                var values = new List<object>();
                foreach (var prop in _source.GetPropertiesByInsertionOrder())
                {
                    var propDetails = new BlittableJsonReaderObject.PropertyDetails();
                    _source.GetPropertyByIndex(_ctx, prop, ref propDetails);
                    values.Add(ConvertValue(propDetails));
                }
                return values;
            }
        }

        public void Add(KeyValuePair<string, object> item)
        {
            if (_metadata == null)
                Init();
            Debug.Assert(_metadata != null);
            _metadata.Add(item.Key, item.Value);
        }

        public void Add(string key, object value)
        {
            if (_metadata == null)
                Init();

            Debug.Assert(_metadata != null);
            _metadata.Add(key, value);
        }

        public void Clear()
        {
            if (_metadata == null)
                Init();
            Debug.Assert(_metadata != null);
            _metadata.Clear();
        }

        public bool Contains(KeyValuePair<string, object> item)
        {
            if (_metadata != null)
                return _metadata.Contains(item);

            object value;
            return _source.TryGetMember(item.Key, out value) && (value.ToString().Equals(item.Value));
        }

        public bool ContainsKey(string key)
        {
            if (_metadata != null)
                return _metadata.ContainsKey(key);

            return _source.TryGetMember(key, out _);
        }

        public void CopyTo(KeyValuePair<string, object>[] array, int arrayIndex)
        {
            if (_metadata == null)
                Init();
            Debug.Assert(_metadata != null);
            _metadata.CopyTo(array, arrayIndex);
        }

        public IEnumerator<KeyValuePair<string, object>> GetEnumerator()
        {
            if (_metadata == null)
                Init();
            Debug.Assert(_metadata != null);
            return _metadata.GetEnumerator();
        }

        public bool Remove(KeyValuePair<string, object> item)
        {
            if (_metadata == null)
                Init();
            Debug.Assert(_metadata != null);
            return _metadata.Remove(item);
        }

        public bool Remove(string key)
        {
            if (_metadata == null)
                Init();
            Debug.Assert(_metadata != null);
            return _metadata.Remove(key);
        }

        public bool TryGetValue(string key, out object value)
        {
            if (_metadata != null)
                return _metadata.TryGetValue(key, out value);

            object val;
            if (_source.TryGetMember(key, out val))
            {
                value = ConvertValue(val);
                return true;
            }
            value = default(object);
            return false;
        }

        public bool TryGetValue(string key, out string value)
        {
            object obj;
            var result = TryGetValue(key, out obj);
            value = (string)obj;
            return result;
        }

        public string GetString(string key)
        {
            var obj = this[key];
            return (string)obj;
        }

        public long GetNumber(string key)
        {
            var obj = this[key];
            return (long)obj;
        }

        public bool GetBoolean(string key)
        {
            var obj = this[key];
            return (bool)obj;
        }

        public double GetDouble(string key)
        {
            var obj = this[key];
            return (double)obj;
        }

        public IMetadataDictionary GetObject(string key)
        {
            var obj = this[key];
            return (IMetadataDictionary)obj;
        }

        public IMetadataDictionary[] GetObjects(string key)
        {
            var obj = (object[])this[key];
            return obj.Cast<IMetadataDictionary>().ToArray();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            if (_metadata == null)
                Init();
            Debug.Assert(_metadata != null);
            return _metadata.GetEnumerator();
        }
    }
}
