/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.utils.flow;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.util.concurrent.Uninterruptibles;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.reactivex.Completable;
import io.reactivex.CompletableObserver;
import io.reactivex.CompletableSource;
import io.reactivex.Single;
import io.reactivex.SingleObserver;
import io.reactivex.disposables.Disposable;
import io.reactivex.functions.BiFunction;
import io.reactivex.functions.Function;

import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCTaskType;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.LineNumberInference;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.Reducer;
import org.apache.cassandra.utils.Throwables;

/**
 * An asynchronous flow of items modelled similarly to Java 9's Flow and RxJava's Flowable with some simplifications.
 */
public abstract class Flow<T>
{
    private static final Logger logger = LoggerFactory.getLogger(Flow.class);

    public final static LineNumberInference LINE_NUMBERS = new LineNumberInference();

    /**
     * Create a subscription linking the content of the flow with the given subscriber.
     * The subscriber is expected to call request() on the returned subscription; in response, it will receive an
     * onNext(item), onComplete(), or onError(throwable). To get further items, the subscriber must call request()
     * again _after_ receiving the onNext (usually before returning from the call).
     *
     * When done with the content (regardless of whether onComplete or onError was received), the subscriber must
     * close the subscription. Closing cannot be done concurrently with any requests.
     */
    abstract public FlowSubscription subscribe(FlowSubscriber<T> subscriber);

    // Flow manipulation methods and implementations follow

    public static final boolean DEBUG_ENABLED = Boolean.getBoolean("cassandra.debugflow");

    /**
     * Op for element-wise transformation.
     *
     * This wrapper allows lambdas to extend to FlowableOp without extra objects being created.
     */
    static class Map<I, O> extends FlowTransformNext<I, O>
    {
        final Function<I, O> mapper;

        public Map(Flow<I> source, Function<I, O> mapper)
        {
            super(source);
            this.mapper = mapper;
        }

        public void onNext(I next)
        {
            O out;
            try
            {
                out = mapper.apply(next);
            }
            catch (Throwable t)
            {
                subscriber.onError(t);
                return;
            }

            subscriber.onNext(out);
        }

        public String toString()
        {
            return formatTrace(getClass().getSimpleName(), mapper, subscriber);
        }
    }

    /**
     * Tranforms each item in the flow using the specified mapper.
     */
    public <O> Flow<O> map(Function<T, O> mapper)
    {
        return new Map(this, mapper);
    }

    /**
     * Op for element-wise transformation which return null, treated as an indication to skip the item.
     *
     * This wrapper allows lambdas to extend to FlowableOp without extra objects being created.
     */
    public static class SkippingMap<I, O> extends FlowTransformNext<I, O>
    {
        final Function<I, O> mapper;

        public SkippingMap(Flow<I> source, Function<I, O> mapper)
        {
            super(source);
            this.mapper = mapper;
        }

        public void onNext(I next)
        {
            O out;
            try
            {
                out = mapper.apply(next);
            }
            catch (Throwable t)
            {
                subscriber.onError(t);
                return;
            }

            if (out != null)
                subscriber.onNext(out);
            else
                requestInLoop(source);
        }

        public String toString()
        {
            return formatTrace(getClass().getSimpleName(), mapper, subscriber);
        }
    }

    /**
     * Like map, but permits mapper to return null, which is treated as an intention to skip the item.
     */
    public <O> Flow<O> skippingMap(Function<T, O> mapper)
    {
        return new SkippingMap(this, mapper);
    }

    /**
     * Op for filtering out items.
     *
     * This wrapper allows lambdas to extend to FlowableOp without extra objects being created.
     */
    static class Filter<I> extends FlowTransformNext<I, I>
    {
        final Predicate<I> tester;

        public Filter(Flow<I> source, Predicate<I> tester)
        {
            super(source);
            this.tester = tester;
        }

        public void onNext(I next)
        {
            boolean pass;
            try
            {
                pass = tester.test(next);
            }
            catch (Throwable t)
            {
                subscriber.onError(t);
                return;
            }

            if (pass)
                subscriber.onNext(next);
            else
                requestInLoop(source);
        }

        public String toString()
        {
            return formatTrace(getClass().getSimpleName(), tester, subscriber);
        }
    }

    /**
     * Take only the items from from the flow that pass the supplied tester.
     */
    public Flow<T> filter(Predicate<T> tester)
    {
        return new Filter<>(this, tester);
    }

    /**
     * Op for element-wise transformation which return null, treated as an indication to stop the flow.
     *
     * This wrapper allows lambdas to extend to FlowableOp without extra objects being created.
     */
    static class StoppingMap<I, O> extends FlowTransformNext<I, O>
    {
        final Function<I, O> mapper;

        public StoppingMap(Flow<I> source, Function<I, O> mapper)
        {
            super(source);
            this.mapper = mapper;
        }

        public void onNext(I next)
        {
            O out;
            try
            {
                out = mapper.apply(next);
            }
            catch (Throwable t)
            {
                subscriber.onError(t);
                return;
            }

            if (out != null)
                subscriber.onNext(out);
            else
                subscriber.onComplete();
        }

        public String toString()
        {
            return formatTrace(getClass().getSimpleName(), mapper, subscriber);
        }
    }

    /**
     * Like map, but permits mapper to return null, which is treated as indication that the flow should stop.
     */
    public <O> Flow<O> stoppingMap(Function<T, O> mapper)
    {
        return new StoppingMap<>(this, mapper);
    }

    /**
     * Op for stopping the flow once a predicate returns false.
     *
     * This wrapper allows lambdas to extend to FlowableOp without extra objects being created.
     */
    static class TakeWhile<I> extends FlowTransformNext<I, I>
    {
        final Predicate<I> tester;

        public TakeWhile(Flow<I> source, Predicate<I> tester)
        {
            super(source);
            this.tester = tester;
        }

        public void onNext(I next)
        {
            boolean pass;
            try
            {
                pass = tester.test(next);
            }
            catch (Throwable t)
            {
                subscriber.onError(t);
                return;
            }

            if (pass)
                subscriber.onNext(next);
            else
                subscriber.onComplete();
        }

        public String toString()
        {
            return formatTrace(getClass().getSimpleName(), tester, subscriber);
        }
    }

    /**
     * Take only the items from the flow until the tester fails for the first time, not including that item.
     *
     * This applies on the onNext phase of execution, after the item has been produced. If the item holds resources,
     * tester must release these resources as the item will not be passed on downstream.
     */
    public Flow<T> takeWhile(Predicate<T> tester)
    {
        return new TakeWhile<>(this, tester);
    }

    /**
     * Stops requesting items from the flow when the given predicate succeeds.
     *
     * Unlike takeWhile, this applies on the request phase of execution, before anything has been produced (and as such
     * cannot be given the next item in the flow as argument).
     */
    public Flow<T> takeUntil(BooleanSupplier tester)
    {
        Flow<T> sourceFlow = this;

        class TakeUntil extends FlowSource<T>
        {
            FlowSubscription source;

            @SuppressWarnings("resource") // source subscriber is closed with close, `subscribe` result can be safely ignored
            public FlowSubscription subscribe(FlowSubscriber<T> subscriber)
            {
                super.subscribe(subscriber);
                source = sourceFlow.subscribe(subscriber);
                return this;
            }

            public void request()
            {
                boolean stop;
                try
                {
                    stop = tester.getAsBoolean();
                }
                catch (Throwable t)
                {
                    subscriber.onError(t);
                    return;
                }

                if (stop)
                    subscriber.onComplete();
                else
                    source.request();
            }

            public void close() throws Exception
            {
                source.close();
            }

            public String toString()
            {
                return Flow.formatTrace(getClass().getSimpleName(), tester, subscriber);
            }
        };

        return new TakeUntil();
    }

    /**
     * Apply the operation when the flow is closed.
     */
    public Flow<T> doOnClose(Runnable onClose)
    {
        Flow<T> sourceFlow = this;

        class DoOnClose extends FlowSource<T>
        {
            FlowSubscription source;

            @SuppressWarnings("resource") // source subscriber is closed with close, `subscribe` result can be safely ignored
            public FlowSubscription subscribe(FlowSubscriber<T> subscriber)
            {
                super.subscribe(subscriber);
                source = sourceFlow.subscribe(subscriber);
                return this;
            }

            public void request()
            {
                source.request();
            }

            public void close() throws Exception
            {
                try
                {
                    source.close();
                }
                finally
                {
                    onClose.run();
                }
            }

            public String toString()
            {
                return Flow.formatTrace(getClass().getSimpleName(), onClose, subscriber);
            }
        };

        return new DoOnClose();
    }

    /**
     * Combination of takeUntil and doOnClose using a single subscription object.
     *
     * Stops requesting items when the supplied tester returns true, and executes the runnable when the flow is closed.
     */
    public Flow<T> takeUntilAndDoOnClose(BooleanSupplier tester, Runnable onClose)
    {
        Flow<T> sourceFlow = this;

        class TakeUntilAndDoOnClose extends FlowSource<T>
        {
            FlowSubscription source;

            @SuppressWarnings("resource") // source subscriber is closed with close, `subscribe` result can be safely ignored
            public FlowSubscription subscribe(FlowSubscriber<T> subscriber)
            {
                super.subscribe(subscriber);
                source = sourceFlow.subscribe(subscriber);
                return this;
            }

            public void request()
            {
                boolean stop;
                try
                {
                    stop = tester.getAsBoolean();
                }
                catch (Throwable t)
                {
                    subscriber.onError(t);
                    return;
                }

                if (stop)
                    subscriber.onComplete();
                else
                    source.request();
            }

            public void close() throws Exception
            {
                try
                {
                    source.close();
                }
                finally
                {
                    onClose.run();
                }
            }

            public String toString()
            {
                return Flow.formatTrace(getClass().getSimpleName(), tester, subscriber);
            }
        };

        return new TakeUntilAndDoOnClose();
    }

    /**
     * Apply the operation when the flow errors out. If handler throws, exception will be added as suppressed.
     */
    public Flow<T> doOnError(Consumer<Throwable> onError)
    {
        class DoOnError extends FlowTransformNext<T, T>
        {
            protected DoOnError(Flow<T> source)
            {
                super(source);
            }

            public void onNext(T item)
            {
                subscriber.onNext(item);
            }

            public void onError(Throwable t)
            {
                try
                {
                    onError.accept(t);
                }
                catch (Throwable mt)
                {
                    t.addSuppressed(mt);
                }

                subscriber.onError(t);
            }

            public String toString()
            {
                return Flow.formatTrace(getClass().getSimpleName(), onError, subscriber);
            }
        }

        return new DoOnError(this);
    }

    /**
     * Apply the operation when the flow completes. If handler throws, subscriber's onError will be called instead
     * of onComplete.
     */
    public Flow<T> doOnComplete(Runnable onComplete)
    {
        class DoOnComplete extends FlowTransformNext<T, T>
        {
            DoOnComplete(Flow<T> source)
            {
                super(source);
            }

            public void onNext(T item)
            {
                subscriber.onNext(item);
            }

            public void onComplete()
            {
                try
                {
                    onComplete.run();
                }
                catch (Throwable t)
                {
                    subscriber.onError(t);
                    return;
                }

                subscriber.onComplete();
            }

            public String toString()
            {
                return Flow.formatTrace(getClass().getSimpleName(), onComplete, subscriber);
            }
        }

        return new DoOnComplete(this);
    }

    /**
     * Group items using the supplied group op. See {@link GroupOp}
     */
    public <O> Flow<O> group(GroupOp<T, O> op)
    {
        return GroupOp.group(this, op);
    }

    /**
     * Pass all elements through the given transformation, then concatenate the results together.
     */
    public <O> Flow<O> flatMap(FlatMap.FlatMapper<T, O> op)
    {
        return FlatMap.flatMap(this, op);
    }

    public static <T> Flow<T> concat(Flow<Flow<T>> source)
    {
        return Concat.concat(source);
    }

    /**
     * Concatenate the input flows one after another and return a flow of items.
     * <p>
     * This is currently implemented with {@link #fromIterable(Iterable)} and {@link #flatMap(FlatMap.FlatMapper)},
     * to be optimized later.
     *
     * @param sources - an iterable of Flow<O></O>
     * @param <T> - the type of the flow items
     *
     * @return a concatenated flow of items
     */
    public static <T> Flow<T> concat(Iterable<Flow<T>> sources)
    {
        return Concat.concat(sources);
    }

    /**
     * Concatenate the input flows one after another and return a flow of items.
     *
     * @param o - an array of Flow<O></O>
     * @param <O> - the type of the flow items
     *
     * @return a concatenated flow of items
     */
    public static <O> Flow<O> concat(Flow<O>... o)
    {
        return Concat.concat(o);
    }

    /**
     * Concatenate this flow with any flow produced by the supplier. After this flow has completed,
     * query the supplier for the next flow and subscribe to it. Continue in this fashion until the
     * supplier returns null, at which point the entire flow is completed.
     *
     * @param supplier - a function that produces a new flow to be concatenated or null when finished
     * @return a flow that will complete when this and all the flows produced by the supplier have
     * completed.
     */
    public Flow<T> concatWith(Supplier<Flow<T>> supplier)
    {
        return Concat.concatWith(this, supplier);
    }

    /**
     * Map each element of the flow into CompletableSources, subscribe to them and
     * wait until the upstream and all CompletableSources complete.
     *
     * @param mapper the function that receives each source value and transforms them into CompletableSources.
     *
     * @return the new Completable instance
     * */
    public Completable flatMapCompletable(Function<? super T, ? extends CompletableSource> mapper)
    {
        return FlatMapCompletable.flatMap(this, mapper);
    }

    /**
     * Return a flow that executes normally except that in case of error it fallbacks to the
     * flow provided by the next source supplier.
     *
     * @param nextSourceSupplier - a function that given an error returns a new flow.
     *
     * @return a flow that on error will subscribe to the flow provided by the next source supplier.
     */
    public Flow<T> onErrorResumeNext(Function<Throwable, Flow<T>> nextSourceSupplier)
    {
        class OnErrorResultNext extends Flow<T> implements FlowSubscriber<T>, FlowSubscription
        {
            private FlowSubscriber<T> subscriber;
            private FlowSubscription subscription;
            private boolean nextSubscribed = false;

            OnErrorResultNext(Flow<T> source)
            {
                this.subscription = source.subscribe(this);
            }

            public FlowSubscription subscribe(FlowSubscriber<T> subscriber)
            {
                assert this.subscriber == null : "Flow are single-use.";
                this.subscriber = subscriber;
                return this;
            }

            public void onNext(T item)
            {
                subscriber.onNext(item);
            }

            public void onComplete()
            {
                subscriber.onComplete();
            }

            public void request()
            {
                subscription.request();
            }

            public void close() throws Exception
            {
                subscription.close();
            }

            @Override
            public String toString()
            {
                return formatTrace("onErrorResumeNext", subscriber);
            }

            @Override
            public Throwable addSubscriberChainFromSource(Throwable t)
            {
                return subscription.addSubscriberChainFromSource(t);
            }

            public final void onError(Throwable t)
            {
                Throwable error = addSubscriberChainFromSource(t);

                if (nextSubscribed)
                {
                    subscriber.onError(error);
                    return;
                }

                FlowSubscription prevSubscription = subscription;
                try
                {
                    Flow<T> nextSource = nextSourceSupplier.apply(error);
                    subscription = nextSource.subscribe(subscriber);
                    nextSubscribed = true;

                }
                catch (Exception ex)
                {
                    subscriber.onError(Throwables.merge(error, ex));
                    // Note: close() will take care of closing the original subscription.
                    return;
                }

                try
                {
                    prevSubscription.close();
                }
                catch (Throwable ex)
                {
                    logger.debug("Failed to close previous subscription in onErrorResumeNext: {}/{}", ex.getClass(), ex.getMessage());
                }

                subscription.request();
            }
        }

        return new OnErrorResultNext(this);
    }

    public Flow<T> mapError(Function<Throwable, Throwable> mapper)
    {
        class MapError extends FlowTransformNext<T, T>
        {
            protected MapError(Flow<T> source)
            {
                super(source);
            }

            public void onNext(T item)
            {
                subscriber.onNext(item);
            }

            public void onError(Throwable t)
            {
                try
                {
                    t = mapper.apply(t);
                }
                catch (Throwable mt)
                {
                    mt.addSuppressed(t);
                    t = mt;
                }

                subscriber.onError(t);
            }

            public String toString()
            {
                return Flow.formatTrace(getClass().getSimpleName(), mapper, subscriber);
            }
        }

        return new MapError(this);
    }

    /**
     * Converts and iterable to a flow. If the iterator is closeable, also closes it with the subscription.
     *
     * onNext() is called synchronously with the next item from the iterator for each request().
     */
    public static <O> Flow<O> fromIterable(Iterable<O> o)
    {
        return new IteratorSubscription(o.iterator());
    }

    static class IteratorSubscription<O> extends FlowSource<O>
    {
        final Iterator<O> iter;

        IteratorSubscription(Iterator<O> iter)
        {
            this.iter = iter;
        }

        public void request()
        {
            boolean hasNext = false;
            O next = null;
            try
            {
                if (iter.hasNext())
                {
                    hasNext = true;
                    next = iter.next();
                }
            }
            catch (Throwable t)
            {
                subscriber.onError(t);
                return;
            }

            if (!hasNext)
                subscriber.onComplete();
            else
                subscriber.onNext(next);
        }

        public void close() throws Exception
        {
            if (iter instanceof AutoCloseable)
                ((AutoCloseable) iter).close();
        }
    }

    enum RequestLoopState
    {
        OUT_OF_LOOP,
        IN_LOOP_READY,
        IN_LOOP_REQUESTED
    }

    /**
     * Helper class for implementing requests looping to avoid stack overflows when an operation needs to request
     * many items from its source. In such cases requests have to be performed in a loop to avoid growing the stack with
     * a full request... -> onNext... -> chain for each new requested element, which can easily cause stack overflow
     * if that is not guaranteed to happen only a small number of times for each item generated by the operation.
     *
     * The main idea is that if a request was issued in response to onNext which an ongoing request triggered (and thus
     * control will return to the request loop after the onNext and request chains return), mark it and process
     * it when control returns to the loop.
     *
     * Note that this does not have to be used for _all_ requests served by an operation. It suffices to apply the loop
     * only for requests that the operation itself makes. By convention the downstream subscriber is also issuing
     * requests to the operation in a loop.
     *
     * This version is used in operations that are not Flow instances (e.g. reduce). Unfortunately we can't extends from
     * both Flow and RequestLoop, so the code is explicitly copied below for Flow operators.
     */
    public static class RequestLoop
    {
        volatile RequestLoopState state = RequestLoopState.OUT_OF_LOOP;
        static final AtomicReferenceFieldUpdater<RequestLoop, RequestLoopState> stateUpdater =
            AtomicReferenceFieldUpdater.newUpdater(RequestLoop.class, RequestLoopState.class, "state");

        public void requestInLoop(FlowSubscription source)
        {
            // See DebugRequestLoop below for a more precise description of the state transitions on which this is built.

            if (stateUpdater.compareAndSet(this, RequestLoopState.IN_LOOP_READY, RequestLoopState.IN_LOOP_REQUESTED))
                // Another call (concurrent or in the call chain) has the loop and we successfully told it to continue.
                return;

            // If the above failed, we must be OUT_OF_LOOP (possibly just concurrently transitioned out of it).
            // Since there can be no other concurrent access, we can grab the loop now.
            do
            {
                state = RequestLoopState.IN_LOOP_READY;
                source.request();
                // If we didn't get another request, leave.
            }
            while (!stateUpdater.compareAndSet(this, RequestLoopState.IN_LOOP_READY, RequestLoopState.OUT_OF_LOOP));
        }
    }

    /**
     * Helper class for implementing requests looping to avoid stack overflows when an operation needs to request
     * many items from its source. In such cases requests have to be performed in a loop to avoid growing the stack with
     * a full request... -> onNext... -> chain for each new requested element, which can easily cause stack overflow
     * if that is not guaranteed to happen only a small number of times for each item generated by the operation.
     *
     * The main idea is that if a request was issued in response to onNext which an ongoing request triggered (and thus
     * control will return to the request loop after the onNext and request chains return), mark it and process
     * it when control returns to the loop.
     *
     * Note that this does not have to be used for _all_ requests served by an operation. It suffices to apply the loop
     * only for requests that the operation itself makes. By convention the downstream subscriber is also issuing
     * requests to the operation in a loop.
     *
     * This is implemented in two versions, RequestLoopFlow and DebugRequestLoopFlow, where the former assumes
     * everything is working correctly while the latter verifies that there is no unexpected behaviour.
     */
    public static abstract class RequestLoopFlow<T> extends Flow<T>
    {
        volatile RequestLoopState state = RequestLoopState.OUT_OF_LOOP;
        static final AtomicReferenceFieldUpdater<RequestLoopFlow, RequestLoopState> stateUpdater =
        AtomicReferenceFieldUpdater.newUpdater(RequestLoopFlow.class, RequestLoopState.class, "state");

        public void requestInLoop(FlowSubscription source)
        {
            // See DebugRequestLoop below for a more precise description of the state transitions on which this is built.

            if (stateUpdater.compareAndSet(this, RequestLoopState.IN_LOOP_READY, RequestLoopState.IN_LOOP_REQUESTED))
                // Another call (concurrent or in the call chain) has the loop and we successfully told it to continue.
                return;

            // If the above failed, we must be OUT_OF_LOOP (possibly just concurrently transitioned out of it).
            // Since there can be no other concurrent access, we can grab the loop now.
            do
            {
                state = RequestLoopState.IN_LOOP_READY;
                source.request();
                // If we didn't get another request, leave.
            }
            while (!stateUpdater.compareAndSet(this, RequestLoopState.IN_LOOP_READY, RequestLoopState.OUT_OF_LOOP));
        }
    }

    static abstract class DebugRequestLoopFlow<T> extends RequestLoopFlow<T>
    {
        abstract FlowSubscriber<T> errorRecepient();

        @Override
        public void requestInLoop(FlowSubscription source)
        {
            if (stateUpdater.compareAndSet(this, RequestLoopState.IN_LOOP_READY, RequestLoopState.IN_LOOP_REQUESTED))
                // Another call (concurrent or in the call chain) has the loop and we successfully told it to continue.
                return;

            // If the above failed, we must be OUT_OF_LOOP (possibly just concurrently transitioned out of it).
            // Since there can be no other concurrent access, we can grab the loop now.
            if (!verifyStateChange(RequestLoopState.OUT_OF_LOOP, RequestLoopState.IN_LOOP_REQUESTED))
                return;

            // We got the loop.
            while (true)
            {
                // The loop can only be entered with a pending request; from here, make our state as having issued a
                // request and ready to receive another before performing the request.
                verifyStateChange(RequestLoopState.IN_LOOP_REQUESTED, RequestLoopState.IN_LOOP_READY);

                source.request();

                // If we didn't get another request, leave.
                if (stateUpdater.compareAndSet(this, RequestLoopState.IN_LOOP_READY, RequestLoopState.OUT_OF_LOOP))
                    return;
            }

        }

        private boolean verifyStateChange(RequestLoopState from, RequestLoopState to)
        {
            RequestLoopState prev = stateUpdater.getAndSet(this, to);
            if (prev == from)
                return true;

            errorRecepient().onError(new AssertionError("Invalid state " + prev));
            return false;
        }

    }

    /**
     * Implementation of the reduce operation, used with small variations in {@link #reduceBlocking(Object, ReduceFunction)},
     * {@link #reduce(Object, ReduceFunction)} and {@link #reduceToFuture(Object, ReduceFunction)}.
     */
    abstract static private class ReduceSubscriber<T, O> extends RequestLoop implements FlowSubscriber<T>, FlowSubscription
    {
        final BiFunction<O, T, O> reducer;
        O current;
        private final StackTraceElement[] stackTrace;
        final FlowSubscription source;

        ReduceSubscriber(O seed, Flow<T> source, BiFunction<O, T, O> reducer)
        {
            this.reducer = reducer;
            current = seed;
            this.source = source.subscribe(this);
            this.stackTrace = maybeGetStackTrace();
        }

        public void onNext(T item)
        {
            try
            {
                current = reducer.apply(current, item);
            }
            catch (Throwable t)
            {
                onError(t);
                return;
            }

            requestInLoop(source);
        }

        public void request()
        {
            requestInLoop(source);
        }

        public void close() throws Exception
        {
            source.close();
        }

        public String toString()
        {
            return formatTrace("reduce", reducer) + "\n" + stackTraceString(stackTrace);
        }

        @Override
        public Throwable addSubscriberChainFromSource(Throwable t)
        {
            return source.addSubscriberChainFromSource(t);
        }

        public final void onError(Throwable t)
        {
            onErrorInternal(addSubscriberChainFromSource(t));
        }

        protected abstract void onErrorInternal(Throwable t);

        // onComplete and onErrorInternal are to be implemented by the concrete use-case.
    }

    public interface ReduceFunction<ACC, I> extends BiFunction<ACC, I, ACC>
    {
    }

    /**
     * Reduce the flow, blocking until the operation completes.
     * Note: The reduced value must not hold resources, because it is lost if an exception occurs.
     *
     * @param seed Initial value for the reduction.
     * @param reducer Called repeatedly with the reduced value (starting with seed and continuing with the result
     *          returned by the previous call) and the next item.
     * @return The final reduced value.
     * @throws Exception
     */
    public <O> O reduceBlocking(O seed, ReduceFunction<O, T> reducer) throws Exception
    {
        CountDownLatch latch = new CountDownLatch(1);

        class ReduceBlockingSubscription extends ReduceSubscriber<T, O>
        {
            Throwable error = null;

            ReduceBlockingSubscription(Flow<T> source, ReduceFunction<O, T> reducer) throws Exception
            {
                super(seed, source, reducer);
            }

            public void onComplete()
            {
                latch.countDown();
            }

            public void onErrorInternal(Throwable t)
            {
                error = t;
                latch.countDown();
            }
        };

        @SuppressWarnings("resource") // subscription is closed right away
        ReduceBlockingSubscription s = new ReduceBlockingSubscription(this, reducer);
        s.request();
        Uninterruptibles.awaitUninterruptibly(latch);
        s.close();

        Throwables.maybeFail(s.error);
        return s.current;
    }

    /**
     * Reduce the flow, returning a completable future that is completed when the operations complete.
     * Note: The reduced value must not hold resources, because it is lost if an exception occurs.
     *
     * @param seed Initial value for the reduction.
     * @param reducer Called repeatedly with the reduced value (starting with seed and continuing with the result
     *          returned by the previous call) and the next item.
     * @return The final reduced value.
     */
    @SuppressWarnings("resource") // subscription is closed on future completion
    public <O> CompletableFuture<O> reduceToFuture(O seed, ReduceFunction<O, T> reducer)
    {
        Future<O> future = new Future<>();
        class ReduceToFutureSubscription extends ReduceSubscriber<T, O>
        {
            ReduceToFutureSubscription(O seed, Flow<T> source, ReduceFunction<O, T> reducer) throws Exception
            {
                super(seed, source, reducer);
            }

            @Override   // onNext is overridden to enable cancellation
            public void onNext(T item)
            {
                // We may be already cancelled, but we prefer to first pass current to the reducer who may know how to
                // release a resource.
                try
                {
                    current = reducer.apply(current, item);
                }
                catch (Throwable t)
                {
                    onError(t);
                    return;
                }

                if (future.isCancelled())
                {
                    try
                    {
                        close();
                    }
                    catch (Throwable t)
                    {
                        logger.error("Error closing flow after cancellation", t);
                    }
                    return;
                }

                requestInLoop(source);
            }

            public void onComplete()
            {
                try
                {
                    close();
                }
                catch (Throwable t)
                {
                    future.completeExceptionallyInternal(t);
                    return;
                }

                future.completeInternal(current);
            }

            protected void onErrorInternal(Throwable t)
            {
                try
                {
                    close();
                }
                catch (Throwable t2)
                {
                    t.addSuppressed(t2);
                }

                future.completeExceptionallyInternal(t);
            }
        }

        ReduceToFutureSubscription s;
        try
        {
            s = new ReduceToFutureSubscription(seed, this, reducer);
        }
        catch (Exception e)
        {
            future.completeExceptionallyInternal(e);
            return future;
        }

        s.request();

        return future;
    }

    /**
     * A future that is linked to the completion of an underlying flow.
     * <p>
     * This extends and behave like a {@link CompletableFuture}, with the exception that one cannot call the
     * {@link #complete(Object)} and {@link #completeExceptionally(Throwable)} (they throw {@link UnsupportedOperationException}):
     * those are called when the flow this is future on completes. It is however possible to cancel() this future, which
     * will stop the underlying flow at the next request.
     */
    public static class Future<T> extends CompletableFuture<T>
    {
        @Override
        public boolean complete(T t)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean completeExceptionally(Throwable throwable)
        {
            throw new UnsupportedOperationException();
        }

        private void completeInternal(T t)
        {
            super.complete(t);
        }

        private void completeExceptionallyInternal(Throwable throwable)
        {
            super.completeExceptionally(throwable);
        }
    }

    public Flow<T> last()
    {
        return reduce(null, (prev, next) -> next);
    }

    /**
     * An abstract subscriber used by the reduce operators for rx java. It implements the Disposable
     * interface of rx java, and stops requesting once disposed.
     */
    private static abstract class DisposableReduceSubscriber<T, O> extends ReduceSubscriber<T, O> implements Disposable
    {
        private volatile boolean isDisposed;
        private volatile int isClosed;
        private static AtomicIntegerFieldUpdater<DisposableReduceSubscriber> isClosedUpdater =
                AtomicIntegerFieldUpdater.newUpdater(DisposableReduceSubscriber.class, "isClosed");

        DisposableReduceSubscriber(O seed, Flow<T> source, ReduceFunction<O, T> reducer) throws Exception
        {
            super(seed, source, reducer);
            isDisposed = false;
            isClosed = 0;
        }

        public void request()
        {
            if (!isDisposed())
                super.request();
            else
            {
                try
                {
                    close();
                }
                catch (Throwable t)
                {
                    onError(t);
                }
            }
        }

        public void dispose()
        {
            isDisposed = true;
        }

        public boolean isDisposed()
        {
            return isDisposed;
        }

        @Override
        public void onComplete()
        {
            try
            {
                close();
            }
            catch (Throwable t)
            {
                signalError(addSubscriberChainFromSource(t));
                return;
            }
            signalSuccess(current);
        }

        @Override
        public void onErrorInternal(Throwable t)
        {
            try
            {
                close();
            }
            catch (Throwable e)
            {
                t = Throwables.merge(t, e);
            }

            signalError(t);
        }

        @Override
        public void close() throws Exception
        {
            // We may get onComplete/onError after disposal. Make sure we don't double-close in that case.
            if (!isClosedUpdater.compareAndSet(this, 0, 1))
                return;

            assert isClosed == 1;
            super.close();
        }

        abstract void signalError(Throwable t);
        abstract void signalSuccess(O value);

        // onNext/onComplete/onError may arrive after disposal. It's not really possible to prevent consumer
        // receiving these messages after disposal (as it could be paused e.g. at the entrance point of the method), so
        // we aren't really worried about that.
        // Importantly, if onComplete arrives after disposal, it is not because it was triggered by the disposal.
    }


    /**
     * Reduce the flow, returning a completable future that is completed when the operations complete.
     * Note: The reduced value must not hold resources, because it is lost if an exception occurs.
     *
     * @param seed Initial value for the reduction.
     * @param reducerToSingle Called repeatedly with the reduced value (starting with seed and continuing with the result
     *          returned by the previous call) and the next item.
     * @return The final reduced value.
     */
    public <O> Single<O> reduceToRxSingle(O seed, ReduceFunction<O, T> reducerToSingle)
    {
        @SuppressWarnings("resource") // reduce subscriber is disposed by the Rx observer
        class SingleFromFlow extends Single<O>
        {
            protected void subscribeActual(SingleObserver<? super O> observer)
            {
                class ReduceToSingle extends DisposableReduceSubscriber<T, O>
                {
                    ReduceToSingle() throws Exception
                    {
                        super(seed, Flow.this, reducerToSingle);
                    }

                    @Override
                    public void signalSuccess(O value)
                    {
                        observer.onSuccess(value);
                    }

                    @Override
                    public void signalError(Throwable t)
                    {
                        observer.onError(t);
                    }
                };

                ReduceToSingle s;
                try
                {
                    s = new ReduceToSingle();
                }
                catch (Exception e)
                {
                    observer.onError(e);
                    return;
                }

                observer.onSubscribe(s);
                s.request();
            }
        }

        return new SingleFromFlow();
    }

    public interface RxSingleMapper<I, O> extends Function<I, O>, ReduceFunction<O, I>
    {
        @Override
        default O apply(O prev, I curr) throws Exception
        {
            assert prev == null;
            return apply(curr);
        }
    }

    /**
     * Maps a Flow holding a single value into an Rx Single using the supplied mapper.
     */
    public <O> Single<O> mapToRxSingle(RxSingleMapper<T, O> mapper)
    {
        return reduceToRxSingle(null, mapper);
    }

    /**
     * Maps a Flow holding a single value into an Rx Single using the supplied mapper.
     */
    public Single<T> mapToRxSingle()
    {
        return mapToRxSingle(x -> x);
    }

    // Not fully tested -- this is not meant for long-term use
    public Completable processToRxCompletable(ConsumingOp<T> consumer)
    {
        @SuppressWarnings("resource") // future is disposed by the Rx observer
        class CompletableFromFlow extends Completable
        {
            protected void subscribeActual(CompletableObserver observer)
            {
                class CompletableFromFlowSubscriber extends DisposableReduceSubscriber<T, Void>
                {
                    private CompletableFromFlowSubscriber() throws Exception
                    {
                        super(null, Flow.this, consumer);
                    }

                    @Override
                    public void signalSuccess(Void value)
                    {
                        observer.onComplete();
                    }

                    @Override
                    public void signalError(Throwable t)
                    {
                        observer.onError(t);
                    }
                }

                CompletableFromFlowSubscriber cs;
                try
                {
                    cs = new CompletableFromFlowSubscriber();
                }
                catch (Throwable t)
                {
                    observer.onError(t);
                    return;
                }
                observer.onSubscribe(cs);
                cs.request();
            }
        }
        return new CompletableFromFlow();
    }

    public Completable processToRxCompletable()
    {
        return processToRxCompletable(v -> {});
    }

    /**
     * Subscribe to this flow and return a future on the completion of this flow.
     * <p>
     * Note that the elements of the flow itself are ignored by the underlying subscriber this method creates (which
     * usually suggests some operations with side-effects have been applied to the flow and we now only want to
     * subscribe and wait for completion).
     *
     * @return a future on the completion of this this flow.
     */
    public CompletableFuture<Void> processToFuture()
    {
        return reduceToFuture(null, (v, t) -> null);
    }

    /**
     * Reduce the flow and return a Flow containing the result.
     *
     * Note: the reduced should not hold resources that need to be released, as the content will be lost on error.
     *
     * @param seed The initial value for the reduction.
     * @param reducer Called repeatedly with the reduced value (starting with seed and continuing with the result
     *          returned by the previous call) and the next item.
     * @return The final reduced value.
     */
    public <O> Flow<O> reduce(O seed, ReduceFunction<O, T> reducer)
    {
        Flow<T> self = this;
        return new Flow<O>()
        {
            public FlowSubscription subscribe(FlowSubscriber<O> subscriber)
            {
                return new ReduceSubscriber<T, O>(seed, self, reducer)
                {
                    volatile boolean completed = false;

                    public void request()
                    {
                        if (completed)
                            subscriber.onComplete();
                        else
                            super.request();
                    }

                    public void onComplete()
                    {
                        completed = true;
                        subscriber.onNext(current); // This should request; if it does we will give it onComplete immediately.
                    }

                    public void onErrorInternal(Throwable t)
                    {
                        subscriber.onError(t);
                    }

                    @Override
                    public String toString()
                    {
                        return formatTrace("reduceWith", reducer);
                    }
                };
            }
        };
    }

    public interface ConsumingOp<T> extends ReduceFunction<Void, T>
    {
        void accept(T item) throws Exception;

        default Void apply(Void v, T item) throws Exception
        {
            accept(item);
            return v;
        }
    }

    private static final ConsumingOp<Object> NO_OP_CONSUMER = (v) -> {};

    @SuppressWarnings("unchecked")
    static <T> ConsumingOp<T> noOp()
    {
        return (ConsumingOp<T>) NO_OP_CONSUMER;
    }

    public Flow<Void> process(ConsumingOp<T> consumer)
    {
        return reduce(null,
                      consumer);
    }

    public Flow<Void> process()
    {
        return process(noOp());
    }

    public <O> Flow<Void> flatProcess(FlatMap.FlatMapper<T, O> mapper)
    {
        return this.flatMap(mapper)
                   .process();
    }

    /**
     * If the contents are empty, replaces them with the given singleton value.
     */
    public Flow<T> ifEmpty(T value)
    {
        return ifEmpty(this, value);
    }

    public static <T> Flow<T> ifEmpty(Flow<T> source, T value)
    {
        return new IfEmptyFlow<>(source, value);
    }

    static class IfEmptyFlow<T> extends FlowTransform<T, T>
    {
        final T value;
        boolean hadItem;
        boolean completed;

        IfEmptyFlow(Flow<T> source, T value)
        {
            super(source);
            this.value = value;
        }

        public void request()
        {
            if (!completed)
                source.request();
            else
                subscriber.onComplete();
        }

        public void close() throws Exception
        {
            source.close();
        }

        public void onNext(T item)
        {
            hadItem = true;
            subscriber.onNext(item);
        }

        public void onComplete()
        {
            completed = true;
            if (hadItem)
                subscriber.onComplete();
            else
                subscriber.onNext(value);
        }

        public void onError(Throwable t)
        {
            subscriber.onError(t);
        }
    }

    /**
     * Converts the flow to singleton flow of the list of items.
     */
    public Flow<List<T>> toList()
    {
        return group(new GroupOp<T, List<T>>(){
            public boolean inSameGroup(T l, T r)
            {
                return true;
            }

            public List<T> map(List<T> inputs)
            {
                return inputs;
            }
        }).ifEmpty(Collections.emptyList());
    }

    /**
     * Returns the singleton item in the flow, or throws if none/multiple are present. Blocks until operations complete.
     */
    public T blockingSingle()
    {
        try
        {
            T res = reduceBlocking(null, (prev, item) ->
            {
                assert (prev == null) : "Call to blockingSingle with more than one element: " + item;
                return item;
            });
            assert (res != null) : "Call to blockingSingle with empty flow.";
            return res;
        }
        catch (Exception e)
        {
            throw com.google.common.base.Throwables.propagate(e);
        }
    }

    /**
     * Returns the last item in the flow, or the given default if empty. Blocks until operations complete.
     */
    public T blockingLast(T def) throws Exception
    {
        return reduceBlocking(def, (prev, item) -> item);
    }

    /**
     * Runs the flow to completion and blocks until done.
     */
    public void executeBlocking() throws Exception
    {
        blockingLast(null);
    }

    /**
     * Returns an empty flow.
     */
    public static <T> Flow<T> empty()
    {
        return new EmptyFlow<>();
    }

    static class EmptyFlow<T> extends FlowSource<T>
    {
        public void request()
        {
            subscriber.onComplete();
        }

        public void close() throws Exception
        {
        }
    };

    /**
     * Returns a flow that simply emits an error as soon as the subscriber
     * requests the first item.
     *
     * @param t - the error to emit
     *
     * @return a flow that emits the error immediately on request
     */
    public static <T> Flow<T> error(Throwable t)
    {
        class ErrorFlow extends FlowSource<T>
        {
            public void request()
            {
                subscriber.onError(t);
            }

            public void close() throws Exception
            {
            }
        };

        return new ErrorFlow();
    }

    /**
     * Return a Flow that will subscribe to the completable once the first request is received and,
     * when the completable completes, it actually subscribes and passes on the first request to the source flow.
     *
     * @param completable - the completable to execute first
     * @param source - the flow that should execute once the completable has completed
     *
     * @param <T> - the type of the source items
     * @return a Flow that executes the completable, followed by the source flow
     */
    public static <T> Flow<T> concat(Completable completable, Flow<T> source)
    {
        class CompletableFlow extends FlowSource<T>
        {
            private FlowSubscription subscription;

            public void request()
            {
                if (subscription != null)
                    subscription.request();
                else
                    completable.subscribe(this::onCompletableSuccess, error -> subscriber.onError(error));
            }

            private void onCompletableSuccess()
            {
                try
                {
                    subscription = source.subscribe(subscriber);
                    subscription.request();
                }
                catch (Exception e)
                {
                    onError(e);
                }
            }

            private void onError(Throwable t)
            {
                subscriber.onError(t);
            }

            public void close() throws Exception
            {
                if (subscription != null)
                    subscription.close();
            }

            public Throwable addSubscriberChainFromSource(Throwable throwable)
            {
                if (subscription != null)
                    return subscription.addSubscriberChainFromSource(throwable);

                return throwable;
            }

            @Override
            public String toString()
            {
                return formatTrace("concatCompletableFlow", completable, subscriber);
            }
        }
        return new CompletableFlow();
    }

    /**
     * Return a Flow that will subscribe to the source flow first and then it will delay
     * the final onComplete() by executing the completable first.
     *
     * @param completable - the completable to execute before the final onComplete of the source
     * @param source - the flow that should execute before the final completable
     *
     * @param <T> - the type of the source items
     * @return a Flow that executes the source flow, then the completable
     */
    public static <T> Flow<T> concat(Flow<T> source, Completable completable)
    {
        return new FlowTransformNext<T, T>(source)
        {
            public void onNext(T item)
            {
                subscriber.onNext(item);
            }

            public void onComplete()
            {
                completable.subscribe(() -> subscriber.onComplete(), error -> onError(error));
            }

            @Override
            public String toString()
            {
                return formatTrace("concatFlowCompletable", completable, subscriber);
            }
        };
    }

    /**
     * Returns a singleton flow with the given value.
     */
    public static <T> Flow<T> just(T value)
    {
        return new SingleFlow<>(value);
    }

    static class SingleFlow<T> extends FlowSource<T>
    {
        final T value;
        boolean supplied = false;

        SingleFlow(T value)
        {
            this.value = value;
        }

        public void request()
        {
            if (supplied)
                subscriber.onComplete();
            else
            {
                supplied = true;
                subscriber.onNext(value);
            }
        }

        public void close() throws Exception
        {
        }
    }

    /**
     * Performs an ordered merge of the given flows. See {@link Merge}
     */
    public static <I, O> Flow<O> merge(List<Flow<I>> flows,
                                       Comparator<? super I> comparator,
                                       Reducer<I, O> reducer)
    {
        return Merge.get(flows, comparator, reducer);
    }


    /**
     * Materializes a list of Flow<I> into a Flow with a single List of I
     */
    public static <I> Flow<List<I>> zipToList(List<Flow<I>> flows)
    {
        return Flow.merge(flows, Comparator.comparing((c) -> 0),
                          new Reducer<I, List<I>>()
                            {
                                List<I> list = new ArrayList<>(flows.size());

                                public void reduce(int idx, I current)
                                {
                                    list.add(current);
                                }

                                public List<I> getReduced()
                                {
                                    return list;
                                }
                            });
    }

    public interface Operator<I, O>
    {
        FlowSubscription subscribe(Flow<I> source, FlowSubscriber<O> subscriber);
    }

    /**
     * Used to apply Operators. See {@link Threads} for usages.
     */
    public <O> Flow<O> lift(Operator<T, O> operator)
    {
        Flow<T> self = this;
        return new Flow<O>()
        {
            public FlowSubscription subscribe(FlowSubscriber<O> subscriber)
            {
                return operator.subscribe(self, subscriber);
            }
        };
    }

    /**
     * Try-with-resources equivalent.
     *
     * The resource supplier is called on subscription, the flow is constructed, and the resource disposer is called
     * when the subscription is closed.
     */
    public static <T, R> Flow<T> using(Supplier<R> resourceSupplier, Function<R, Flow<T>> flowSupplier, Consumer<R> resourceDisposer)
    {
        return new Flow<T>()
        {
            public FlowSubscription subscribe(FlowSubscriber<T> subscriber)
            {
                R resource = resourceSupplier.get();
                try
                {
                    return flowSupplier.apply(resource)
                                       .doOnClose(() -> resourceDisposer.accept(resource))
                                       .subscribe(subscriber);
                }
                catch (Throwable t)
                {
                    resourceDisposer.accept(resource);
                    throw com.google.common.base.Throwables.propagate(t);
                }
            }
        };
    }

    /**
     * Take no more than the first count items from the flow.
     */
    public Flow<T> take(long count)
    {
        AtomicLong cc = new AtomicLong(count);
        return takeUntil(() -> cc.decrementAndGet() < 0);
    }

    /**
     * Delays the execution of each onNext by the given time.
     * Used for testing.
     */
    public Flow<T> delayOnNext(long sleepFor, TimeUnit timeUnit, TPCTaskType taskType)
    {
        return new FlowTransformNext<T, T>(this)
        {
            public void onNext(T item)
            {
                TPC.bestTPCScheduler().scheduleDirect(() -> subscriber.onNext(item),
                                                      taskType,
                                                      sleepFor,
                                                      timeUnit);
            }
        };
    }

    /**
     * Count the items in the flow, blocking until operations complete.
     * Used for tests.
     */
    public long countBlocking() throws Exception
    {
        // Note: using AtomicLong to avoid boxing a long at every iteration.
        return reduceBlocking(new AtomicLong(0),
                              (count, value) ->
                              {
                                  count.incrementAndGet();
                                  return count;
                              }).get();
    }

    /**
     * Returns a flow containing:
     * - the source if it is not empty.
     * - nothing if it is empty.
     *
     * Note: Both resulting flows are single-use.
     */
    public Flow<Flow<T>> skipEmpty()
    {
        return SkipEmpty.skipEmpty(this);
    }

    /**
     * Returns a flow containing:
     * - the source passed through the supplied mapper if it is not empty.
     * - nothing if it is empty.
     *
     * Note: The flow passed to the mapper, as well as the returned result, are single-use.
     */
    public <U> Flow<U> skipMapEmpty(Function<Flow<T>, U> mapper)
    {
        return SkipEmpty.skipMapEmpty(this, mapper);
    }

    public static <T> CloseableIterator<T> toIterator(Flow<T> source) throws Exception
    {
        return new ToIteratorSubscriber<T>(source);
    }

    static class ToIteratorSubscriber<T> implements CloseableIterator<T>, FlowSubscriber<T>
    {
        static final Object POISON_PILL = new Object();

        final FlowSubscription subscription;
        BlockingQueue<Object> queue = new ArrayBlockingQueue<>(1);
        Throwable error = null;
        Object next = null;

        public ToIteratorSubscriber(Flow<T> source) throws Exception
        {
            subscription = source.subscribe(this);
        }

        public void close()
        {
            try
            {
                subscription.close();
            }
            catch (Exception e)
            {
                throw com.google.common.base.Throwables.propagate(e);
            }
        }

        @Override
        public void onComplete()
        {
            Uninterruptibles.putUninterruptibly(queue, POISON_PILL);
        }

        @Override
        public void onError(Throwable arg0)
        {
            error = org.apache.cassandra.utils.Throwables.merge(error, arg0);
            Uninterruptibles.putUninterruptibly(queue, POISON_PILL);
        }

        @Override
        public void onNext(T arg0)
        {
            Uninterruptibles.putUninterruptibly(queue, arg0);
        }

        protected Object computeNext()
        {
            if (next != null)
                return next;

            assert queue.isEmpty();
            subscription.request();

            next = Uninterruptibles.takeUninterruptibly(queue);
            if (error != null)
                throw com.google.common.base.Throwables.propagate(error);
            return next;
        }

        @Override
        public boolean hasNext()
        {
            return computeNext() != POISON_PILL;
        }

        public String toString()
        {
            return formatTrace("toIterator");
        }

        @Override
        public T next()
        {
            boolean has = hasNext();
            assert has;

            @SuppressWarnings("unchecked")
            T toReturn = (T) next;
            next = null;
            return toReturn;
        }
    }

    /**
     * Simple interface for extracting the chilren of a tee operation.
     */
    public interface Tee<T>
    {
        public Flow<T> child(int index);
    }

    /**
     * Splits the flow into multiple resulting flows which can be independently operated and closed. The same
     * items, completions and errors are provided to all of the clients.
     * All resulting flows must be subscribed to and closed.
     *
     * The tee implementation (see {@link TeeImpl} executes by waiting for all clients to act before issuing the act
     * upstream. This means that the use of one client cannot be delayed until the completion of another (as this would
     * have required caching).
     *
     * The typical usage of this operation is to apply a "reduceToFuture" operation to all but one of the resulting
     * flows. This operation subscribes and initiates requests, but will not actually perform anything until the last
     * "controlling" flow subscribes and requests. When that happens onNext calls will be issued to all clients, and
     * any of them can delay (by not requesting) or withdraw from (by closing) futher processing.
     */
    public Tee<T> tee(int count)
    {
        return new TeeImpl<>(this, count);
    }

    /**
     * Splits the flow into two resulting flows which can be independently operated and closed. The same
     * items, completions and errors are provided to both of the clients.
     * Both resulting flows must be subscribed to and closed.
     *
     * The tee implementation (see {@link TeeImpl} executes by waiting for all clients to act before issuing the act
     * upstream. This means that the use of one client cannot be delayed until the completion of another (as this would
     * have required caching).
     *
     * The typical usage of this operation is to apply a "reduceToFuture" operation to all but one of the resulting
     * flows. This operation subscribes and initiates requests, but will not actually perform anything until the last
     * "controlling" flow subscribes and requests. When that happens onNext calls will be issued to all clients, and
     * any of them can delay (by not requesting) or withdraw from (by closing) futher processing.
     */
    public Tee<T> tee()
    {
        return new TeeImpl<>(this, 2);
    }

    public static StackTraceElement[] maybeGetStackTrace()
    {
        if (Flow.DEBUG_ENABLED)
            return Thread.currentThread().getStackTrace();
        return null;
    }

    public static Throwable wrapException(Throwable throwable, Object tag)
    {
        // for well known exceptions that can occur and that are handled downstream,
        // do not attach the subscriber chain as this would slow things down too much
        if (throwable instanceof NonWrappableException)
            return throwable;

        // Avoid re-wrapping an exception if it was already added
        for (Throwable t : throwable.getSuppressed())
        {
            if (t instanceof FlowException)
                return throwable;
        }

        // Load lambdas before calling `toString` on the object
        LINE_NUMBERS.preloadLambdas();
        throwable.addSuppressed(new FlowException(tag.toString()));
        return throwable;
    }

    /**
     * An interface to signal to {@link #wrapException(Throwable, Object)}
     * not to wrap these exceptions with the caller stack trace since these
     * exceptions may occur regularly and therefore preloading lambdas would
     * be too slow.
     */
    public interface NonWrappableException
    {

    }

    private static class FlowException extends RuntimeException
    {
        private FlowException(Object tag)
        {
            super("Flow call chain:\n" + tag.toString());
        }
    }

    public static String stackTraceString(StackTraceElement[] stackTrace)
    {
        if (stackTrace == null)
            return "";

        return " created during\n\t   " + Stream.of(stackTrace)
                                                .skip(4)
                                                .map(Object::toString)
                                                .collect(Collectors.joining("\n\tat "))
               + "\n";
    }
    public static String withLineNumber(Object obj)
    {
        LINE_NUMBERS.maybeProcessClass(obj.getClass());
        Pair<String, Integer> lineNumber = LINE_NUMBERS.getLine(obj.getClass());
        return obj + "(" + lineNumber.left + ":" + lineNumber.right + ")";
    }

    public static String formatTrace(String prefix)
    {
        return String.format("\t%-20s", prefix);
    }

    public static String formatTrace(String prefix, Object tag)
    {
        return String.format("\t%-20s%s", prefix, Flow.withLineNumber(tag));
    }

    public static String formatTrace(String prefix, Object tag, FlowSubscriber subscriber)
    {
        return String.format("\t%-20s%s\n%s", prefix, Flow.withLineNumber(tag), subscriber);
    }

    public static String formatTrace(String prefix, FlowSubscriber subscriber)
    {
        return String.format("\t%-20s\n%s", prefix, subscriber);
    }
}