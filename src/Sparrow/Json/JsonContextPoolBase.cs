﻿using System;
using System.Threading;
using Sparrow.LowMemory;
using Sparrow.Utils;

namespace Sparrow.Json
{
    public abstract class JsonContextPoolBase<T> : ILowMemoryHandler, IDisposable
        where T : JsonOperationContext
    {
        /// <summary>
        /// This is thread static value because we usually have great similiarity in the operations per threads.
        /// Indexing thread will adjust their contexts to their needs, and request processing threads will tend to
        /// average to the same overall type of contexts
        /// </summary>
        private readonly ThreadLocal<ContextStack> _contextPool;
        private readonly NativeMemoryCleaner<ContextStack, T> _nativeMemoryCleaner;
        private bool _disposed;
        protected LowMemoryFlag LowMemoryFlag = new LowMemoryFlag();

        private readonly CancellationTokenSource _cts = new CancellationTokenSource();

        private class ContextStack : StackHeader<T>, IDisposable
        {
            ~ContextStack()
            {
                if (Environment.HasShutdownStarted)
                    return; // let the OS clean this up

                try
                {
                    Dispose();
                }
                catch (ObjectDisposedException)
                {
                }
            }

            public void Dispose()
            {
                GC.SuppressFinalize(this);

                var current = Head;
                while (current != null)
                {
                    var ctx = current.Value;
                    current = current.Next;
                    if (ctx == null)
                        continue;
                    if (Interlocked.CompareExchange(ref ctx.InUse, 1, 0) != 0)
                        continue;
                    ctx.Dispose();
                }
            }
        }

        protected JsonContextPoolBase()
        {
            _contextPool = new ThreadLocal<ContextStack>(() => new ContextStack(), trackAllValues: true);
            _nativeMemoryCleaner = new NativeMemoryCleaner<ContextStack, T>(_contextPool, LowMemoryFlag, TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1));
            LowMemoryNotification.Instance?.RegisterLowMemoryHandler(this);
        }

        public IDisposable AllocateOperationContext(out JsonOperationContext context)
        {
            T ctx;
            var disposable = AllocateOperationContext(out ctx);
            context = ctx;

            return disposable;
        }

        public void Clean()
        {
            // we are expecting to be called here when there is no
            // more work to be done, and we want to release resources
            // to the system

            var stack = _contextPool.Value;
            var current = Interlocked.Exchange(ref stack.Head, null);
            while (current != null)
            {
                current.Value?.Dispose();
                current = current.Next;
            }
        }

        public IDisposable AllocateOperationContext(out T context)
        {
            _cts.Token.ThrowIfCancellationRequested();
            ContextStack currentThread = _contextPool.Value;
            if (TryReuseExistingContextFrom(currentThread, out context, out IDisposable returnContext))
                return returnContext;

            // no choice, got to create it
            context = CreateContext();
            return new ReturnRequestContext
            {
                Parent = currentThread,
                Context = context,
            };
        }

        private bool TryReuseExistingContextFrom(ContextStack stack, out T context, out IDisposable disposable)
        {
            while (true)
            {
                var current = stack.Head;
                if (current == null)
                    break;
                if (Interlocked.CompareExchange(ref stack.Head, current.Next, current) != current)
                    continue;
                context = current.Value;
                if (context == null)
                    continue;
                if (Interlocked.CompareExchange(ref context.InUse, 1, 0) != 0)
                    continue;
                context.Renew();
                disposable = new ReturnRequestContext
                {
                    Parent = stack,
                    Context = context
                };
                return true;
            }

            context = default(T);
            disposable = null;
            return false;
        }

        protected abstract T CreateContext();

        private class ReturnRequestContext : IDisposable
        {
            public T Context;
            public ContextStack Parent;

            public void Dispose()
            {
                Context.Reset();
                Interlocked.Exchange(ref Context.InUse, 0);
                Context.InPoolSince = DateTime.UtcNow;

                while (true)
                {
                    var current = Parent.Head;
                    var newHead = new StackNode<T> { Value = Context, Next = current };
                    if (Interlocked.CompareExchange(ref Parent.Head, newHead, current) == current)
                        return;
                }
            }
        }

     

        public void Dispose()
        {
            if (_disposed)
                return;
            lock (this)
            {
                if (_disposed)
                    return;
                _cts.Cancel();
                _disposed = true;
                _nativeMemoryCleaner.Dispose();
                foreach (var stack in _contextPool.Values)
                {
                    stack.Dispose();
                }
                _contextPool.Dispose();
            }
        }

        public void LowMemory()
        {
            if (Interlocked.CompareExchange(ref LowMemoryFlag.LowMemoryState, 1, 0) != 0)
                return;
            _nativeMemoryCleaner.CleanNativeMemory(null);
        }

        public void LowMemoryOver()
        {
            Interlocked.CompareExchange(ref LowMemoryFlag.LowMemoryState, 0, 1);
        }
    }
}