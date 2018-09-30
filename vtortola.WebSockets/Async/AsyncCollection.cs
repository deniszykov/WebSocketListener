using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Security;
using System.Threading;
using vtortola.WebSockets.Tools;

#pragma warning disable 420

namespace vtortola.WebSockets.Async
{
    internal abstract class AsyncCollection<ItemT, CollectionT> : IProducerConsumerCollection<ItemT> where CollectionT : class, IProducerConsumerCollection<ItemT>
    {
        private static readonly ItemT[] EmptyList = new ItemT[0];

        protected const int UNBOUND = int.MaxValue;
        private const string DEFAULT_PARALLEL_TAKE_ERROR_MESSAGE = "Parallel 'TakeAsync' is not allowed.";
        private const string DEFAULT_CLOSED_ERROR_MESSAGE = "Collection is closed and can't accept or give new items.";

        // ReSharper disable StaticMemberInGenericType
        private static readonly Lazy<ExceptionDispatchInfo> DefaultCanceledError;
        // ReSharper restore StaticMemberInGenericType

        public string ParallelTakeErrorMessage = DEFAULT_PARALLEL_TAKE_ERROR_MESSAGE;
        public string ClosedErrorMessage = DEFAULT_CLOSED_ERROR_MESSAGE;

        private readonly CollectionT innerCollection;
        private readonly Func<CollectionT, bool> emptyCheck;
        private readonly int boundedCapacity;

        private volatile int count;
        private volatile TakeResult takeResult;
        private volatile ExceptionDispatchInfo closeError;
        private int sendCounter;

        public int Count => this.count;
        public bool IsClosed => this.closeError != null;
        public bool IsEmpty => this.emptyCheck(this.innerCollection) || this.IsClosed;
        /// <inheritdoc />
        object ICollection.SyncRoot => this.innerCollection;
        /// <inheritdoc />
        bool ICollection.IsSynchronized => false;

        static AsyncCollection()
        {
            DefaultCanceledError = new Lazy<ExceptionDispatchInfo>(() =>
            {
                try
                {
                    throw new OperationCanceledException();
                }
                catch (OperationCanceledException canceledError)
                {
                    return ExceptionDispatchInfo.Capture(canceledError);
                }
            }, LazyThreadSafetyMode.ExecutionAndPublication);
        }
        protected AsyncCollection(CollectionT collection, Func<CollectionT, bool> emptyCheck, int boundedCapacity = UNBOUND)
        {
            if (collection == null) throw new ArgumentNullException(nameof(collection));
            if (emptyCheck == null) throw new ArgumentNullException(nameof(emptyCheck));
            if (boundedCapacity <= 0) throw new ArgumentOutOfRangeException(nameof(boundedCapacity));

            this.innerCollection = collection;
            this.emptyCheck = emptyCheck;
            this.boundedCapacity = boundedCapacity;
            this.count = 0;
            this.closeError = null;
        }

        protected TakeResult TakeAsync(CancellationToken cancellation)
        {
            var newDequeueAsync = new TakeResult(this, cancellation, true, false);
            if (Interlocked.CompareExchange(ref this.takeResult, newDequeueAsync, null) != null)
            {
                newDequeueAsync.UnsubscribeCancellation();
                throw new InvalidOperationException(this.ParallelTakeErrorMessage ?? DEFAULT_PARALLEL_TAKE_ERROR_MESSAGE);
            }

            return newDequeueAsync;
        }
        protected bool TryTake(out ItemT value)
        {
            value = default(ItemT);
            if (this.IsClosed)
                return false;

            if (this.takeResult == null && this.innerCollection.TryTake(out value))
            {
                Interlocked.Decrement(ref this.count);
                return true;
            }
            return false;
        }
        protected bool TryAdd(ItemT value)
        {
            Interlocked.Increment(ref this.sendCounter);
            var result = false;
            try
            {
                if (this.IsClosed)
                    return false;

                if (Interlocked.Increment(ref this.count) > this.boundedCapacity)
                {
                    Interlocked.Decrement(ref this.count);
                    return false;
                }
                result = this.innerCollection.TryAdd(value);
            }
            finally
            {
                Interlocked.Decrement(ref this.sendCounter);
            }

            this.takeResult?.CheckForCompletion();
            return result;
        }
        bool IProducerConsumerCollection<ItemT>.TryTake(out ItemT value)
        {
            return this.TryTake(out value);
        }
        bool IProducerConsumerCollection<ItemT>.TryAdd(ItemT value)
        {
            return this.TryAdd(value);
        }

        public void ClearAndClose(Exception closeError = null)
        {
            var closeErrorDispatchInfo = this.GetCloseErrorDispatchInfo(closeError);
            if (Interlocked.CompareExchange(ref this.closeError, closeErrorDispatchInfo, null) != null)
                return;

            this.takeResult?.CheckForCompletion();
        }
        public IReadOnlyList<ItemT> TakeAllAndClose(Exception closeError = null)
        {
            var closeErrorDispatchInfo = this.GetCloseErrorDispatchInfo(closeError);
            if (Interlocked.CompareExchange(ref this.closeError, closeErrorDispatchInfo, null) != null)
                return EmptyList;

            var resultList = default(List<ItemT>);
            var spinWait = new SpinWait();
            while (this.sendCounter > 0)
                spinWait.SpinOnce();

            var value = default(ItemT);
            while (this.innerCollection.TryTake(out value))
            {
                if (resultList == null) resultList = new List<ItemT>(this.innerCollection.Count);
                Interlocked.Decrement(ref this.count);
                resultList.Add(value);
            }

            this.takeResult?.CheckForCompletion();

            return (IReadOnlyList<ItemT>)resultList ?? EmptyList;
        }

        private ExceptionDispatchInfo GetCloseErrorDispatchInfo(Exception closeError)
        {
            var closeErrorDispatchInfo = default(ExceptionDispatchInfo);
            if (closeError != null)
            {
                closeErrorDispatchInfo = ExceptionDispatchInfo.Capture(closeError);
            }
            else
            {
                try
                {
                    throw new InvalidOperationException(this.ClosedErrorMessage ?? DEFAULT_CLOSED_ERROR_MESSAGE);
                }
                catch (InvalidOperationException error)
                {
                    closeErrorDispatchInfo = ExceptionDispatchInfo.Capture(error);
                }
            }

            return closeErrorDispatchInfo;
        }

        /// <inheritdoc />
        public IEnumerator<ItemT> GetEnumerator()
        {
            return this.innerCollection.GetEnumerator();
        }
        /// <inheritdoc />
        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.innerCollection.GetEnumerator();
        }
        /// <inheritdoc />
        public void CopyTo(ItemT[] array, int index)
        {
            this.innerCollection.CopyTo(array, index);
        }
        /// <inheritdoc />
        public ItemT[] ToArray()
        {
            return this.innerCollection.ToArray();
        }
        /// <inheritdoc />
        public void CopyTo(Array array, int index)
        {
            this.innerCollection.CopyTo(array, index);
        }

        public class TakeResult : ICriticalNotifyCompletion
        {
            private const int STATUS_NONE = 0;
            private const int STATUS_UPDATING_RESULT = 1;
            private const int STATUS_COMPLETE = 2;

            private readonly AsyncCollection<ItemT, CollectionT> collection;
            private readonly CancellationToken cancellation;
            private readonly CancellationTokenRegistration cancellationRegistration;
            private volatile int status = STATUS_NONE;
            private Action safeContinuation;
            private Action unsafeContinuation;
            private ItemT result;
            private ExceptionDispatchInfo error;

            public bool ContinueOnCapturedContext;
            public bool Schedule;

            public TakeResult(AsyncCollection<ItemT, CollectionT> collection, CancellationToken cancellation, bool continueOnCapturedContext, bool schedule)
            {
                if (collection == null) throw new ArgumentNullException(nameof(collection));

                this.collection = collection;
                this.ContinueOnCapturedContext = continueOnCapturedContext;
                this.Schedule = schedule;
                this.cancellation = cancellation;
                // ReSharper disable ImpureMethodCallOnReadonlyValueField
                if (this.cancellation.CanBeCanceled)
                    this.cancellationRegistration = this.cancellation.Register(s => ((TakeResult)s).CheckForCompletion(), this, this.ContinueOnCapturedContext);
                // ReSharper restore ImpureMethodCallOnReadonlyValueField
            }

            public TakeResult GetAwaiter()
            {
                return this;
            }
            public TakeResult ConfigureAwait(bool continueOnCapturedContext, bool schedule = true)
            {
                this.ContinueOnCapturedContext = continueOnCapturedContext;
                this.Schedule = schedule;
                return this;
            }

            public bool IsCompleted
            {
                get
                {
                    if (this.status == STATUS_COMPLETE)
                        return true;

                    this.CheckForCompletion();

                    return this.status == STATUS_COMPLETE;
                }
            }

            [SecuritySafeCritical]
            public void OnCompleted(Action continuation)
            {
                if (this.collection == null) throw new InvalidOperationException();

                if (this.IsCompleted)
                {
                    DelegateHelper.QueueContinuation(continuation, this.ContinueOnCapturedContext, this.Schedule);
                    return;
                }

                DelegateHelper.InterlockedCombine(ref this.safeContinuation, continuation);

                if (this.IsCompleted)
                {
                    if (DelegateHelper.InterlockedRemove(ref this.safeContinuation, continuation))
                        DelegateHelper.QueueContinuation(continuation, this.ContinueOnCapturedContext, this.Schedule);

                }
            }
            [SecurityCritical]
            public void UnsafeOnCompleted(Action continuation)
            {
                if (this.collection == null) throw new InvalidOperationException();

                DelegateHelper.InterlockedCombine(ref this.unsafeContinuation, continuation);

                if (this.IsCompleted)
                {
                    if (DelegateHelper.InterlockedRemove(ref this.unsafeContinuation, continuation))
                        DelegateHelper.UnsafeQueueContinuation(continuation, this.ContinueOnCapturedContext, this.Schedule);

                }
            }

            private bool TryComplete()
            {
                if (this.status == STATUS_COMPLETE)
                    return false;

                if (this.cancellation.IsCancellationRequested)
                {
                    if (Interlocked.CompareExchange(ref this.status, STATUS_UPDATING_RESULT, STATUS_NONE) != STATUS_NONE)
                        return false; // some thread is doing this in parallel

                    this.error = DefaultCanceledError.Value;
                    this.Complete();
                    return true;
                }

                if (this.collection.IsClosed)
                {
                    if (Interlocked.CompareExchange(ref this.status, STATUS_UPDATING_RESULT, STATUS_NONE) != STATUS_NONE)
                        return false; // some thread is doing this in parallel

                    this.error = this.collection.closeError;
                    this.Complete();
                    return true;
                }

                if (this.collection.IsEmpty == false)
                {
                    if (Interlocked.CompareExchange(ref this.status, STATUS_UPDATING_RESULT, STATUS_NONE) != STATUS_NONE)
                        return false; // some thread is doing this in parallel

                    if (this.collection.innerCollection.TryTake(out this.result))
                    {
                        Interlocked.Decrement(ref this.collection.count);
                        this.Complete();
                        return true;
                    }
                    else
                    {
                        Interlocked.Exchange(ref this.status, STATUS_NONE);
                    }
                }

                return false; // no result is available
            }
            private void Complete()
            {
                this.UnsubscribeCancellation();
                Interlocked.CompareExchange(ref this.collection.takeResult, null, this);
                Interlocked.Exchange(ref this.status, STATUS_COMPLETE);
            }
            private void ResumeContinuations()
            {
                var continuation = Interlocked.Exchange(ref this.safeContinuation, null);
                if (continuation != null) DelegateHelper.QueueContinuation(continuation, this.ContinueOnCapturedContext, this.Schedule);

                continuation = Interlocked.Exchange(ref this.unsafeContinuation, null);
                if (continuation != null) DelegateHelper.UnsafeQueueContinuation(continuation, this.ContinueOnCapturedContext, this.Schedule);
            }

            internal void CheckForCompletion()
            {
                if (this.collection.IsEmpty == false || this.collection.IsClosed || this.cancellation.IsCancellationRequested)
                    if (this.TryComplete())
                        this.ResumeContinuations();
            }
            internal void UnsubscribeCancellation()
            {
                // ReSharper disable once ImpureMethodCallOnReadonlyValueField
                this.cancellationRegistration.Dispose();
            }

            public ItemT GetResult()
            {
                if (this.status != STATUS_COMPLETE)
                    throw new InvalidOperationException($"Async operation is not complete. Check '{nameof(this.IsCompleted)}' property before calling this method.");

                this.error?.Throw();
                return this.result;
            }
        }
    }
}
