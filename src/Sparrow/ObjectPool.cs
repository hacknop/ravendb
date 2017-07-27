
// Copyright (c) Microsoft.  All Rights Reserved.  Licensed under the Apache License, Version 2.0.  See License.txt in the Roslyn project root for license information.

// define TRACE_LEAKS to get additional diagnostics that can lead to the leak sources. note: it will
// make everything about 2-3x slower
// 
//#define TRACE_LEAKS

// define DETECT_LEAKS to detect possible leaks
//#if DEBUG
//    #define DETECT_LEAKS  //for now always enable DETECT_LEAKS in debug.
//#endif

using System.Diagnostics.Contracts;
using System.Reflection;
using System.Runtime.InteropServices;
using Sparrow.Binary;
using Sparrow.Utils;

namespace Sparrow
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Runtime.CompilerServices;
    using System.Threading;

#if DETECT_LEAKS
    using System.Runtime.CompilerServices;
#endif

    /// <summary>
    /// Generic implementation of object pooling pattern with predefined pool size limit. The main
    /// purpose is that limited number of frequently used objects can be kept in the pool for
    /// further recycling.
    /// 
    /// Notes: 
    /// 1) it is not the goal to keep all returned objects. Pool is not meant for storage. If there
    ///    is no space in the pool, extra returned objects will be dropped.
    /// 
    /// 2) it is implied that if object was obtained from a pool, the caller will return it back in
    ///    a relatively short time. Keeping checked out objects for long durations is ok, but 
    ///    reduces usefulness of pooling. Just new up your own.
    /// 
    /// Not returning objects to the pool in not detrimental to the pool's work, but is a bad practice. 
    /// Rationale: 
    ///    If there is no intent for reusing the object, do not use pool - just use "new". 
    /// </summary>
    public class ObjectPool<T> : ObjectPool<T, NoResetSupport<T>, NonThreadAwareBehavior>
        where T : class
    {
        public ObjectPool(Factory factory) : base(factory) { }

        public ObjectPool(Factory factory, int size) : base(factory, size) { }
    }

    public class ObjectPool<T, TResetBehavior> : ObjectPool<T, TResetBehavior, NonThreadAwareBehavior>
        where T : class
        where TResetBehavior : struct, IResetSupport<T>
    {
        public ObjectPool(Factory factory) : base(factory) { }

        public ObjectPool(Factory factory, int size) : base(factory, size) { }
    }

    /// <summary>
    /// Generic implementation of object pooling pattern with predefined pool size limit. The main
    /// purpose is that limited number of frequently used objects can be kept in the pool for
    /// further recycling.
    /// 
    /// Notes: 
    /// 1) it is not the goal to keep all returned objects. Pool is not meant for storage. If there
    ///    is no space in the pool, extra returned objects will be dropped.
    /// 
    /// 2) it is implied that if object was obtained from a pool, the caller will return it back in
    ///    a relatively short time. Keeping checked out objects for long durations is ok, but 
    ///    reduces usefulness of pooling. Just new up your own.
    /// 
    /// Not returning objects to the pool in not detrimental to the pool's work, but is a bad practice. 
    /// Rationale: 
    ///    If there is no intent for reusing the object, do not use pool - just use "new". 
    /// </summary>
    public class ObjectPool<T, TResetBehavior, TProcessAwareBehavior>
        where T : class
        where TResetBehavior : struct, IResetSupport<T>
        where TProcessAwareBehavior : struct, IProcessAwareBehavior
    {
        private static readonly TResetBehavior Behavior = new TResetBehavior();

        private struct Element
        {
            internal T Value;
        }

        [StructLayout(LayoutKind.Sequential, Size = 128)]
        private struct CacheAwareElement
        {
            public readonly long _pad1, _pad2, _pad3;
            public T Value;
        }

        /// <remarks>
        /// Not using System.Func{T} because this file is linked into the (debugger) Formatter,
        /// which does not have that type (since it compiles against .NET 2.0).
        /// </remarks>
        public delegate T Factory();

        public const int Buckets = 16;

        // Storage for the pool objects. The first item is stored in a dedicated field because we
        // expect to be able to satisfy most requests from it.
        private readonly CacheAwareElement[] _firstItems;
        private readonly int _bucketsMask;
        private readonly Element[] _items;
        private readonly int _itemsMask;
        private readonly int _cacheLineOffset;

        // factory is stored for the lifetime of the pool. We will call this only when pool needs to
        // expand. compared to "new T()", Func gives more flexibility to implementers and faster
        // than "new T()".
        private readonly Factory _factory;

#if DETECT_LEAKS
    private static readonly ConditionalWeakTable<T, LeakTracker> leakTrackers = new ConditionalWeakTable<T, LeakTracker>();

    private class LeakTracker : IDisposable
    {
        private volatile bool disposed;

#if TRACE_LEAKS
        internal volatile object Trace = null;
#endif

        public void Dispose()
        {
            disposed = true;
            GC.SuppressFinalize(this);
        }

        private string GetTrace()
        {
#if TRACE_LEAKS
            return Trace == null ? "" : Trace.ToString();
#else
            return "Leak tracing information is disabled. Define TRACE_LEAKS on ObjectPool`1.cs to get more info \n";
#endif
        }

        ~LeakTracker()
        {
            if (!this.disposed && !Environment.HasShutdownStarted)
            {
                var trace = GetTrace();

                // If you are seeing this message it means that object has been allocated from the pool 
                // and has not been returned back. This is not critical, but turns pool into rather 
                // inefficient kind of "new".
                Debug.WriteLine(string.Format("TRACEOBJECTPOOLLEAKS_BEGIN\nPool detected potential leaking of {0}. \n Location of the leak: \n {1} TRACEOBJECTPOOLLEAKS_END", typeof(T), GetTrace()));
            }
        }
    }
#endif

        public ObjectPool(Factory factory)
            : this(factory, ProcessorInfo.ProcessorCount * 2)
        { }

        public ObjectPool(Factory factory, int size)
        {
            Debug.Assert(size >= 1);
            _factory = factory;

            int bucketsSize = 1;
            if (typeof(TProcessAwareBehavior) == typeof(ThreadAwareBehavior))
            {
                // PERF: We will always have power of two pools to make operations a lot faster. 
                size = Bits.NextPowerOf2(size);
                size = Math.Max(16, size); 

                bucketsSize = Buckets;
                _cacheLineOffset = size / Buckets;
            }

            _items = new Element[size];
            _itemsMask = size - 1;
            _bucketsMask = bucketsSize - 1;            
            _firstItems = new CacheAwareElement[bucketsSize];
        }

        private T CreateInstance()
        {
            var inst = _factory();
            return inst;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ObjectPoolContext<T, TResetBehavior, TProcessAwareBehavior> AllocateInContext()
        {
            return new ObjectPoolContext<T, TResetBehavior, TProcessAwareBehavior>(this, Allocate());
        }

        /// <summary>
        /// Produces an instance.
        /// </summary>
        /// <remarks>
        /// Search strategy is a simple linear probing which is chosen for it cache-friendliness.
        /// Note that Free will try to store recycled objects close to the start thus statistically 
        /// reducing how far we will typically search.
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public T Allocate()
        {
            int threadIndex = 0;
            if (typeof(TProcessAwareBehavior) == typeof(ThreadAwareBehavior))
                threadIndex = Environment.CurrentManagedThreadId & _bucketsMask;

            // PERF: Examine the first element. If that fails, AllocateSlow will look at the remaining elements.
            // Note that the initial read is optimistically not synchronized. That is intentional. 
            // We will interlock only when we have a candidate. in a worst case we may miss some
            // recently returned objects. Not a big deal.

            ref var firstItem = ref _firstItems[threadIndex];

            T inst = firstItem.Value;
            if (inst == null || inst != Interlocked.CompareExchange(ref firstItem.Value, null, inst))
            {
                if (typeof(TProcessAwareBehavior) == typeof(ThreadAwareBehavior))
                    inst = AllocateSlow(threadIndex);
                else
                    inst = AllocateSlow();
            }

#if DETECT_LEAKS
        var tracker = new LeakTracker();
        leakTrackers.Add(inst, tracker);

#if TRACE_LEAKS
        var frame = CaptureStackTrace();
        tracker.Trace = frame;
#endif
#endif
            return inst;
        }

        private T AllocateSlow(int threadIndex)
        {
            var items = _items;

            int offset = _cacheLineOffset * threadIndex;
            for (int i = 0; i < items.Length; i++)
            {
                ref var item = ref items[(i + offset) & _itemsMask];

                // Note that the initial read is optimistically not synchronized. That is intentional. 
                // We will interlock only when we have a candidate. in a worst case we may miss some
                // recently returned objects. Not a big deal.

                T inst = item.Value;
                if (inst != null)
                {
                    if (inst == Interlocked.CompareExchange(ref item.Value, null, inst))
                        return inst;
                }
            }

            return CreateInstance();
        }

        private T AllocateSlow()
        {
            var items = _items;

            for (int i = 0; i < items.Length; i++)
            {
                // Note that the initial read is optimistically not synchronized. That is intentional. 
                // We will interlock only when we have a candidate. in a worst case we may miss some
                // recently returned objects. Not a big deal.
                T inst = items[i].Value;
                if (inst != null)
                {
                    if (inst == Interlocked.CompareExchange(ref items[i].Value, null, inst))
                    {
                        return inst;
                    }
                }
            }

            return CreateInstance();
        }

        /// <summary>
        /// Returns objects to the pool.
        /// </summary>
        /// <remarks>
        /// Search strategy is a simple linear probing which is chosen for it cache-friendliness.
        /// Note that Free will try to store recycled objects close to the start thus statistically 
        /// reducing how far we will typically search in Allocate.
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Free(T obj)
        {
            Validate(obj);
            ForgetTrackedObject(obj);

            Behavior.Reset(obj);

            int threadIndex = 0;
            if (typeof(TProcessAwareBehavior) == typeof(ThreadAwareBehavior))
                threadIndex = Environment.CurrentManagedThreadId & _bucketsMask;

            ref var firstItem = ref _firstItems[threadIndex];
            
            if (firstItem.Value == null)
            {
                // Intentionally not using interlocked here. 
                // In a worst case scenario two objects may be stored into same slot.
                // It is very unlikely to happen and will only mean that one of the objects will get collected.
                firstItem.Value = obj;
                return;
            }

            if (typeof(TProcessAwareBehavior) == typeof(ThreadAwareBehavior))
                FreeSlow(obj, threadIndex);
            else
                FreeSlow(obj);
        }

        private void FreeSlow(T obj)
        {
            var items = _items;
            for (int i = 0; i < items.Length; i++)
            {
                ref var item = ref items[i];
                
                if (item.Value == null)
                {
                    // Intentionally not using interlocked here. 
                    // In a worst case scenario two objects may be stored into same slot.
                    // It is very unlikely to happen and will only mean that one of the objects will get collected.
                    item.Value = obj;
                    break;
                }
            }
        }

        private void FreeSlow(T obj, int threadIndex)
        {
            var items = _items;
            
            int offset = _cacheLineOffset * threadIndex;
            for (int i = 0; i < items.Length; i++)
            {
                ref var item = ref items[(i + offset) & _itemsMask];

                if (item.Value == null)
                {
                    // Intentionally not using interlocked here. 
                    // In a worst case scenario two objects may be stored into same slot.
                    // It is very unlikely to happen and will only mean that one of the objects will get collected.
                    item.Value = obj;
                    break;
                }
            }
        }

        /// <summary>
        /// Removes an object from leak tracking.  
        /// 
        /// This is called when an object is returned to the pool.  It may also be explicitly 
        /// called if an object allocated from the pool is intentionally not being returned
        /// to the pool.  This can be of use with pooled arrays if the consumer wants to 
        /// return a larger array to the pool than was originally allocated.
        /// </summary>
        [Conditional("DEBUG")]
        public void ForgetTrackedObject(T old, T replacement = null)
        {
#if DETECT_LEAKS
        LeakTracker tracker;
        if (leakTrackers.TryGetValue(old, out tracker))
        {
            tracker.Dispose();
            leakTrackers.Remove(old);
        }
        else
        {
            var trace = CaptureStackTrace();
            Debug.WriteLine(string.Format("TRACEOBJECTPOOLLEAKS_BEGIN\nObject of type {0} was freed, but was not from pool. \n Callstack: \n {1} TRACEOBJECTPOOLLEAKS_END", typeof(T), trace));
        }

        if (replacement != null)
        {
            tracker = new LeakTracker();
            leakTrackers.Add(replacement, tracker);
        }
#endif
        }

#if DETECT_LEAKS
    private static Lazy<Type> _stackTraceType = new Lazy<Type>(() => Type.GetType("System.Diagnostics.StackTrace"));

    private static object CaptureStackTrace()
    {
        return Activator.CreateInstance(_stackTraceType.Value);
    }
#endif

        [Conditional("DEBUG")]
        private void Validate(object obj)
        {
            Debug.Assert(obj != null, "freeing null?");

            var items = _items;
            for (int i = 0; i < items.Length; i++)
            {
                var value = items[i].Value;
                if (value == null)
                {
                    return;
                }

                Debug.Assert(value != obj, "freeing twice?");
            }
        }
    }


    public interface IProcessAwareBehavior
    { }
    
    public struct ThreadAwareBehavior : IProcessAwareBehavior { }
    public struct NonThreadAwareBehavior : IProcessAwareBehavior { }

    public interface IResetSupport<in T> where T : class
    {
        void Reset(T value);
    }

    public struct NoResetSupport<T> : IResetSupport<T> where T : class
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Reset(T value) {}
    }

    public struct DictionaryResetBehavior<T1, T2> : IResetSupport<Dictionary<T1, T2>>
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void IResetSupport<Dictionary<T1, T2>>.Reset(Dictionary<T1, T2> value)
        {
            value.Clear();
        }
    }

    public struct HashSetResetBehavior<T1> : IResetSupport<HashSet<T1>>
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void IResetSupport<HashSet<T1>>.Reset(HashSet<T1> value)
        {
            value.Clear();
        }
    }

    public struct ListResetBehavior<T1> : IResetSupport<List<T1>>
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void IResetSupport<List<T1>>.Reset(List<T1> value)
        {
            value.Clear();
        }
    }

    public struct ObjectPoolContext<T, TR> : IDisposable
        where T : class
        where TR : struct, IResetSupport<T>
    {
        private readonly ObjectPool<T, TR, NonThreadAwareBehavior> _owner;
        public readonly T Value;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ObjectPoolContext(ObjectPool<T, TR, NonThreadAwareBehavior> owner, T value)
        {
            this._owner = owner;
            this.Value = value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Dispose()
        {
            this._owner.Free(Value);
        }
    }

    public struct ObjectPoolContext<T, TR, TPA> : IDisposable
        where T : class
        where TR : struct, IResetSupport<T>
        where TPA : struct, IProcessAwareBehavior
    {
        private readonly ObjectPool<T, TR, TPA> _owner;
        public readonly T Value;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ObjectPoolContext(ObjectPool<T, TR, TPA> owner, T value)
        {
            this._owner = owner;
            this.Value = value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Dispose()
        {
            this._owner.Free(Value);
        }
    }
}
