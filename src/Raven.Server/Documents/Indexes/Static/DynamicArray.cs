﻿using System;
using System.Collections;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using Raven.Server.Utils;
using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.Static
{
    public class DynamicArray : DynamicObject, IEnumerable<object>
    {
        private readonly JsonOperationContext _ctx;
        private readonly IEnumerable<object> _inner;

        public DynamicArray(JsonOperationContext ctx, IEnumerable inner)
            : this(ctx, inner.Cast<object>())
        {
        }

        public DynamicArray(JsonOperationContext ctx,IEnumerable<object> inner)
        {
            _ctx = ctx;
            _inner = inner;
        }

        public int Length => _inner.Count();

        public int Count => _inner.Count();


        public dynamic Get(params int[] indexes)
        {
            if (indexes == null)
                return DynamicNullObject.Null;

            dynamic val = this;
            foreach (int index in indexes)
            {
                val = val[index];
            }
            return val;
        }

        public override bool TryGetMember(GetMemberBinder binder, out object result)
        {
            const string lengthName = "Length";
            const string countName = "Count";

            result = null;
            if (string.CompareOrdinal(binder.Name, lengthName) == 0 ||
                string.CompareOrdinal(binder.Name, countName) == 0)
            {
                result = Length;
                return true;
            }

            return false;
        }

        public override bool TryGetIndex(GetIndexBinder binder, object[] indexes, out object result)
        {
            if (indexes == null)
            {
                result = DynamicNullObject.Null;
                return true;
            }
            if (indexes.Length != 1)
            {
                var ints = new int[indexes.Length];
                for (int j = 0; j < indexes.Length; j++)
                {
                    if (indexes[j] is int num)
                        ints[j] = num;
                }
                result = Get(ints);
                return true;
            }

            var i = (int)indexes[0];
            var resultObject = _inner.ElementAt(i);

            result = TypeConverter.ToDynamicType(_ctx, resultObject);
            return true;
        }

        public override bool TryConvert(ConvertBinder binder, out object result)
        {
            if (binder.ReturnType.IsArray)
            {
                var elementType = binder.ReturnType.GetElementType();
                var count = Count;
                var array = Array.CreateInstance(elementType, count);

                for (var i = 0; i < count; i++)
                {
                    var item = _inner.ElementAt(i);
                    if (elementType == typeof(string) && (item is LazyStringValue || item is LazyCompressedStringValue))
                        array.SetValue(item.ToString(), i);
                    else
                        array.SetValue(Convert.ChangeType(item, elementType), i);
                }

                result = array;

                return true;
            }

            return base.TryConvert(binder, out result);
        }

        IEnumerator<object> IEnumerable<object>.GetEnumerator()
        {
            return GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public DynamicArrayIterator GetEnumerator()
        {
            return new DynamicArrayIterator(_ctx, _inner);
        }

        public bool Contains(object item)
        {
            return Enumerable.Contains(this, item);
        }

        public int Sum(Func<dynamic, int> selector)
        {
            return Enumerable.Sum(this, selector);
        }

        public int? Sum(Func<dynamic, int?> selector)
        {
            return Enumerable.Sum(this, selector) ?? DynamicNullObject.Null;
        }

        public long Sum(Func<dynamic, long> selector)
        {
            return Enumerable.Sum(this, selector);
        }

        public long? Sum(Func<dynamic, long?> selector)
        {
            return Enumerable.Sum(this, selector) ?? DynamicNullObject.Null;
        }

        public float Sum(Func<dynamic, float> selector)
        {
            return Enumerable.Sum(this, selector);
        }

        public float? Sum(Func<dynamic, float?> selector)
        {
            return Enumerable.Sum(this, selector) ?? DynamicNullObject.Null;
        }

        public double Sum(Func<dynamic, double> selector)
        {
            return Enumerable.Sum(this, selector);
        }

        public double? Sum(Func<dynamic, double?> selector)
        {
            return Enumerable.Sum(this, selector) ?? DynamicNullObject.Null;
        }

        public decimal Sum(Func<dynamic, decimal> selector)
        {
            return Enumerable.Sum(this, selector);
        }

        public decimal? Sum(Func<dynamic, decimal?> selector)
        {
            return Enumerable.Sum(this, selector) ?? DynamicNullObject.Null;
        }

        public dynamic Min()
        {
            return Enumerable.Min(this) ?? DynamicNullObject.Null;
        }

        public dynamic Min<TResult>(Func<dynamic, TResult> selector)
        {
            var result = Enumerable.Min(this, selector);

            if (result == null)
                return DynamicNullObject.Null;

            return result;
        }

        public dynamic Max()
        {
            return Enumerable.Max(this) ?? DynamicNullObject.Null;
        }

        public dynamic Max<TResult>(Func<dynamic, TResult> selector)
        {
            var result = Enumerable.Max(this, selector);

            if (result == null)
                return DynamicNullObject.Null;

            return result;
        }

        public double Average(Func<dynamic, int> selector)
        {
            return Enumerable.Average(this, selector);
        }

        public double? Average(Func<dynamic, int?> selector)
        {
            return Enumerable.Average(this, selector) ?? DynamicNullObject.Null;
        }

        public double Average(Func<dynamic, long> selector)
        {
            return Enumerable.Average(this, selector);
        }

        public double? Average(Func<dynamic, long?> selector)
        {
            return Enumerable.Average(this, selector) ?? DynamicNullObject.Null;
        }

        public float Average(Func<dynamic, float> selector)
        {
            return Enumerable.Average(this, selector);
        }

        public float? Average(Func<dynamic, float?> selector)
        {
            return Enumerable.Average(this, selector) ?? DynamicNullObject.Null;
        }

        public double Average(Func<dynamic, double> selector)
        {
            return Enumerable.Average(this, selector);
        }

        public double? Average(Func<dynamic, double?> selector)
        {
            return Enumerable.Average(this, selector) ?? DynamicNullObject.Null;
        }

        public decimal Average(Func<dynamic, decimal> selector)
        {
            return Enumerable.Average(this, selector);
        }

        public decimal? Average(Func<dynamic, decimal?> selector)
        {
            return Enumerable.Average(this, selector) ?? DynamicNullObject.Null;
        }

        public IEnumerable<dynamic> OrderBy(Func<dynamic, dynamic> comparable)
        {
            return new DynamicArray(_ctx, Enumerable.OrderBy(this, comparable));
        }

        public IEnumerable<dynamic> OrderByDescending(Func<dynamic, dynamic> comparable)
        {
            return new DynamicArray(_ctx, Enumerable.OrderByDescending(this, comparable));
        }

        public dynamic GroupBy(Func<dynamic, dynamic> keySelector)
        {
            return new DynamicArray(_ctx, Enumerable.GroupBy(this, keySelector).Select(x => new DynamicGrouping(_ctx, x)));
        }

        public dynamic GroupBy(Func<dynamic, dynamic> keySelector, Func<dynamic, dynamic> selector)
        {
            return new DynamicArray(_ctx, Enumerable.GroupBy(this, keySelector, selector).Select(x => new DynamicGrouping(_ctx, x)));
        }

        public dynamic Last()
        {
            return Enumerable.Last(this);
        }

        public dynamic LastOrDefault()
        {
            return Enumerable.LastOrDefault(this) ?? DynamicNullObject.Null;
        }

        public dynamic Last(Func<dynamic, bool> predicate)
        {
            return Enumerable.Last(this, predicate);
        }

        public dynamic LastOrDefault(Func<dynamic, bool> predicate)
        {
            return Enumerable.LastOrDefault(this, predicate) ?? DynamicNullObject.Null;
        }

        public dynamic IndexOf(dynamic item)
        {
            var items = Enumerable.ToList(this);
            return items.IndexOf(item);
        }

        public dynamic IndexOf(dynamic item, int index)
        {
            var items = Enumerable.ToList(this);
            return items.IndexOf(item, index);
        }

        public dynamic IndexOf(dynamic item, int index, int count)
        {
            var items = Enumerable.ToList(this);
            return items.IndexOf(item, index, count);
        }

        public dynamic LastIndexOf(dynamic item)
        {
            var items = Enumerable.ToList(this);
            return items.LastIndexOf(item);
        }

        public dynamic LastIndexOf(dynamic item, int index)
        {
            var items = Enumerable.ToList(this);
            return items.LastIndexOf(item, index);
        }

        public dynamic LastIndexOf(dynamic item, int index, int count)
        {
            var items = Enumerable.ToList(this);
            return items.LastIndexOf(item, index, count);
        }

        public IEnumerable<dynamic> Take(int count)
        {
            return new DynamicArray(_ctx, Enumerable.Take(this, count));
        }

        public IEnumerable<dynamic> Skip(int count)
        {
            return new DynamicArray(_ctx, Enumerable.Skip(this, count));
        }

        public IEnumerable<object> Select(Func<object, object> func)
        {
            return new DynamicArray(_ctx, Enumerable.Select(this, func));
        }

        public IEnumerable<object> Select(Func<IGrouping<object, object>, object> func)
        {
            return new DynamicArray(_ctx, Enumerable.Select(this, o => func((IGrouping<object, object>)o)));
        }

        public IEnumerable<object> Select(Func<object, int, object> func)
        {
            return new DynamicArray(_ctx, Enumerable.Select(this, func));
        }

        public IEnumerable<object> SelectMany(Func<object, IEnumerable<object>> func)
        {
            return new DynamicArray(_ctx, Enumerable.SelectMany(this, func));
        }

        public IEnumerable<object> SelectMany(Func<object, IEnumerable<object>> func, Func<object, object, object> selector)
        {
            return new DynamicArray(_ctx, Enumerable.SelectMany(this, func, selector));
        }

        public IEnumerable<object> SelectMany(Func<object, int, IEnumerable<object>> func)
        {
            return new DynamicArray(_ctx, Enumerable.SelectMany(this, func));
        }

        public IEnumerable<object> Where(Func<object, bool> func)
        {
            return new DynamicArray(_ctx, Enumerable.Where(this, func));
        }

        public IEnumerable<object> Where(Func<object, int, bool> func)
        {
            return new DynamicArray(_ctx, Enumerable.Where(this, func));
        }

        public IEnumerable<object> Distinct()
        {
            return new DynamicArray(_ctx, Enumerable.Distinct(this));
        }

        public dynamic DefaultIfEmpty(object defaultValue = null)
        {
            return Enumerable.DefaultIfEmpty(this, defaultValue ?? DynamicNullObject.Null);
        }

        public IEnumerable<dynamic> Except(IEnumerable<dynamic> except)
        {
            return new DynamicArray(_ctx, Enumerable.Except(this, except));
        }

        public IEnumerable<dynamic> Reverse()
        {
            return new DynamicArray(_ctx, Enumerable.Reverse(this));
        }

        public bool SequenceEqual(IEnumerable<dynamic> second)
        {
            return Enumerable.SequenceEqual(this, second);
        }

        public IEnumerable<dynamic> AsEnumerable()
        {
            return this;
        }

        public dynamic[] ToArray()
        {
            return Enumerable.ToArray(this);
        }

        public List<dynamic> ToList()
        {
            return Enumerable.ToList(this);
        }

        public Dictionary<TKey, dynamic> ToDictionary<TKey>(Func<dynamic, TKey> keySelector, Func<dynamic, dynamic> elementSelector = null)
        {
            if (elementSelector == null)
                return Enumerable.ToDictionary(this, keySelector);

            return Enumerable.ToDictionary(this, keySelector, elementSelector);
        }

        public ILookup<TKey, dynamic> ToLookup<TKey>(Func<dynamic, TKey> keySelector, Func<dynamic, dynamic> elementSelector = null)
        {
            if (elementSelector == null)
                return Enumerable.ToLookup(this, keySelector);

            return Enumerable.ToLookup(this, keySelector, elementSelector);
        }

        public IEnumerable<dynamic> OfType<T>()
        {
            return new DynamicArray(_ctx, Enumerable.OfType<T>(this));
        }

        public IEnumerable<dynamic> Cast<T>()
        {
            return new DynamicArray(_ctx, Enumerable.Cast<T>(this));
        }

        public dynamic ElementAt(int index)
        {
            return Enumerable.ElementAt(this, index);
        }

        public dynamic ElementAtOrDefault(int index)
        {
            return Enumerable.ElementAtOrDefault(this, index) ?? DynamicNullObject.Null;
        }

        public long LongCount()
        {
            return Enumerable.LongCount(this);
        }

        public dynamic Aggregate(Func<dynamic, dynamic, dynamic> func)
        {
            return Enumerable.Aggregate(this, func);
        }

        public dynamic Aggregate(dynamic seed, Func<dynamic, dynamic, dynamic> func)
        {
            return Enumerable.Aggregate(this, (object)seed, func);
        }

        public dynamic Aggregate(dynamic seed, Func<dynamic, dynamic, dynamic> func, Func<dynamic, dynamic> resultSelector)
        {
            return Enumerable.Aggregate(this, (object)seed, func, resultSelector);
        }

        public IEnumerable<dynamic> TakeWhile(Func<dynamic, bool> predicate)
        {
            return new DynamicArray(_ctx, Enumerable.TakeWhile(this, predicate));
        }

        public IEnumerable<dynamic> TakeWhile(Func<dynamic, int, bool> predicate)
        {
            return new DynamicArray(_ctx, Enumerable.TakeWhile(this, predicate));
        }

        public IEnumerable<dynamic> SkipWhile(Func<dynamic, bool> predicate)
        {
            return new DynamicArray(_ctx, Enumerable.SkipWhile(this, predicate));
        }

        public IEnumerable<dynamic> SkipWhile(Func<dynamic, int, bool> predicate)
        {
            return new DynamicArray(_ctx, Enumerable.SkipWhile(this, predicate));
        }

        public IEnumerable<dynamic> Join(IEnumerable<dynamic> items, Func<dynamic, dynamic> outerKeySelector, Func<dynamic, dynamic> innerKeySelector,
                                            Func<dynamic, dynamic, dynamic> resultSelector)
        {
            return new DynamicArray(_ctx, Enumerable.Join(this, items, outerKeySelector, innerKeySelector, resultSelector));
        }

        public IEnumerable<dynamic> GroupJoin(IEnumerable<dynamic> items, Func<dynamic, dynamic> outerKeySelector, Func<dynamic, dynamic> innerKeySelector,
                                            Func<dynamic, dynamic, dynamic> resultSelector)
        {
            return new DynamicArray(_ctx, Enumerable.GroupJoin(this, items, outerKeySelector, innerKeySelector, resultSelector));
        }

        public IEnumerable<dynamic> Concat(IEnumerable second)
        {
            return new DynamicArray(_ctx, Enumerable.Concat(this, second.Cast<object>()));
        }

        public IEnumerable<dynamic> Zip(IEnumerable second, Func<dynamic, dynamic, dynamic> resultSelector)
        {
            return new DynamicArray(_ctx, Enumerable.Zip(this, second.Cast<object>(), resultSelector));
        }

        public IEnumerable<dynamic> Union(IEnumerable second)
        {
            return new DynamicArray(_ctx, Enumerable.Union(this, second.Cast<object>()));
        }

        public IEnumerable<dynamic> Intersect(IEnumerable second)
        {
            return new DynamicArray(_ctx, Enumerable.Intersect(this, second.Cast<object>()));
        }

        public struct DynamicArrayIterator : IEnumerator<object>
        {
            private readonly JsonOperationContext _ctx;
            private readonly IEnumerator<object> _inner;

            public DynamicArrayIterator(JsonOperationContext ctx, IEnumerable<object> items)
            {
                _ctx = ctx;
                _inner = items.GetEnumerator();
                Current = null;
            }

            public bool MoveNext()
            {
                if (_inner.MoveNext() == false)
                    return false;


                Current = TypeConverter.ToDynamicType(_ctx, _inner.Current);
                return true;
            }

            public void Reset()
            {
                throw new NotImplementedException();
            }

            public object Current { get; private set; }

            object IEnumerator.Current => Current;

            public void Dispose()
            {
            }
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
                return false;

            if (ReferenceEquals(this, obj))
                return true;

            var array = obj as DynamicArray;

            if (array != null)
                return Equals(_inner, array._inner);

            return Equals(_inner, obj);
        }

        public override int GetHashCode()
        {
            return _inner?.GetHashCode() ?? 0;
        }

        public class DynamicGrouping : DynamicArray, IGrouping<object, object>
        {
            private readonly IGrouping<dynamic, dynamic> _grouping;

            public DynamicGrouping(JsonOperationContext ctx, IGrouping<dynamic, dynamic> grouping)
                : base(ctx, grouping)
            {
                _grouping = grouping;
            }

            public dynamic Key => _grouping.Key;

            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }
        }
    }
}
