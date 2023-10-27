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
package org.apache.cassandra.service.accord;

import java.util.concurrent.Callable;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.ToLongFunction;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Ints;

import accord.local.Command.TransientListener;
import accord.local.Listeners;
import accord.utils.IntrusiveLinkedListNode;
import accord.utils.Invariants;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncResults.RunnableResult;
import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.utils.ObjectSizes;

import static java.lang.String.format;
import static org.apache.cassandra.service.accord.AccordCachingState.Status.EVICTED;
import static org.apache.cassandra.service.accord.AccordCachingState.Status.FAILED_TO_LOAD;
import static org.apache.cassandra.service.accord.AccordCachingState.Status.FAILED_TO_SAVE;
import static org.apache.cassandra.service.accord.AccordCachingState.Status.LOADED;
import static org.apache.cassandra.service.accord.AccordCachingState.Status.LOADING;
import static org.apache.cassandra.service.accord.AccordCachingState.Status.MODIFIED;
import static org.apache.cassandra.service.accord.AccordCachingState.Status.SAVING;
import static org.apache.cassandra.service.accord.AccordCachingState.Status.UNINITIALIZED;

/**
 * Global (per CommandStore) state of a cached entity (Command or CommandsForKey).
 */
public class AccordCachingState<K, V> extends IntrusiveLinkedListNode
{
    static final long EMPTY_SIZE = ObjectSizes.measure(new AccordCachingState<>(null, 0, null));

    public interface Factory<K, V>
    {
        AccordCachingState<K, V> create(K key, int index);
    }

    static <K, V> Factory<K, V> defaultFactory()
    {
        return AccordCachingState::new;
    }

    private final K key;
    private State<K, V> state;

    int references = 0;
    int lastQueriedEstimatedSizeOnHeap = 0;
    final byte index;
    private boolean shouldUpdateSize;

    /**
     * Transient listeners aren't meant to survive process restart, but must survive cache eviction.
     */
    private Listeners<TransientListener> transientListeners;

    AccordCachingState(K key, int index)
    {
        this.key = key;
        Invariants.checkArgument(index >= 0 && index <= Byte.MAX_VALUE);
        this.index = (byte) index;
        //noinspection unchecked
        this.state = (State<K, V>) Uninitialized.instance;
    }

    private AccordCachingState(K key, int index, State<K, V> state)
    {
        this.key = key;
        Invariants.checkArgument(index >= 0 && index <= Byte.MAX_VALUE);
        this.index = (byte) index;
        this.state = state;
    }

    void unlink()
    {
        remove();
    }

    boolean isLinked()
    {
        return !isFree();
    }

    public K key()
    {
        return key;
    }

    public int referenceCount()
    {
        return references;
    }

    boolean isLoaded()
    {
        return status().isLoaded();
    }

    public boolean isComplete()
    {
        return status().isComplete();
    }

    public boolean canEvict()
    {
        return true;
    }

    int estimatedSizeOnHeap(ToLongFunction<V> estimator)
    {
        shouldUpdateSize = false;
        return lastQueriedEstimatedSizeOnHeap = Ints.checkedCast(EMPTY_SIZE + estimateStateOnHeapSize(estimator));
    }

    long estimatedSizeOnHeapDelta(ToLongFunction<V> estimator)
    {
        long prevSize = lastQueriedEstimatedSizeOnHeap;
        return estimatedSizeOnHeap(estimator) - prevSize;
    }

    boolean shouldUpdateSize()
    {
        return shouldUpdateSize;
    }

    @Override
    public String toString()
    {
        return "Node{" + state.status() +
               ", key=" + key() +
               ", references=" + references +
               "}@" + Integer.toHexString(System.identityHashCode(this));
    }

    public Status status()
    {
        return complete().status();
    }

    public void addListener(TransientListener listener)
    {
        if (transientListeners == null)
            transientListeners = new Listeners<>();
        transientListeners.add(listener);
    }

    public boolean removeListener(TransientListener listener)
    {
        return transientListeners != null && transientListeners.remove(listener);
    }

    public void listeners(Listeners<TransientListener> listeners)
    {
        transientListeners = listeners;
    }

    public Listeners<TransientListener> listeners()
    {
        return transientListeners == null ? Listeners.EMPTY : transientListeners;
    }

    public boolean hasListeners()
    {
        return !listeners().isEmpty();
    }

    State<K, V> complete()
    {
        return state.isCompleteable() ? state(state.complete()) : state;
    }

    /**
     * Submits a load runnable to the specified executor. When the runnable
     * has completed, the state load will have either completed or failed.
     */
    public AsyncChain<V> load(ExecutorPlus executor, Function<K, V> loadFunction)
    {
        Loading<K, V> loading = state.load(key, loadFunction);
        executor.submit(loading);
        state(loading);
        return loading;
    }

    public void initialize(V value)
    {
        state(state.initialize(value));
    }

    protected State<K, V> state(State<K, V> next)
    {
        State<K, V> prev = state;
        if (prev != next)
            shouldUpdateSize = true;
        return state = next;
    }

    @VisibleForTesting
    protected State<K, V> state()
    {
        return state;
    }

    public AsyncChain<V> loading()
    {
        // do *not* attempt to complete, to prevent races where the caller found a pending load, attempts
        // to register a callback, but gets an exception because the load completed in the meantime
        return state.loading();
    }

    public V get()
    {
        return complete().get();
    }

    public void set(V value)
    {
        shouldUpdateSize = true;
        state(complete().set(value));
    }

    /**
     * Submits a save runnable to the specified executor. When the runnable
     * has completed, the state save will have either completed or failed.
     */
    @VisibleForTesting
    public void save(ExecutorPlus executor, BiFunction<?, ?, Runnable> saveFunction)
    {
        @SuppressWarnings("unchecked")
        State<K, V> savingOrLoaded = state.save((BiFunction<V, V, Runnable>) saveFunction);
        if (savingOrLoaded.status() == SAVING)
            executor.submit(savingOrLoaded.saving());
        state(savingOrLoaded);
    }

    public AsyncChain<Void> saving()
    {
        // do *not* attempt to complete, to prevent races where the caller found a pending save, attempts
        // to register a callback, but gets an exception because the save completed in the meantime
        return state.saving();
    }

    public AccordCachingState<K, V> reset()
    {
        state(state.reset());
        return this;
    }

    public Throwable failure()
    {
        return complete().failure();
    }

    public void markEvicted()
    {
        state(complete().evict());
        lastQueriedEstimatedSizeOnHeap = 0;
        shouldUpdateSize = false;
    }

    long estimateStateOnHeapSize(ToLongFunction<V> estimateFunction)
    {
        return state.estimateOnHeapSize(estimateFunction);
    }

    public enum Status
    {
        UNINITIALIZED,
        LOADING,
        LOADED,
        FAILED_TO_LOAD,
        MODIFIED,
        SAVING,

        /**
         * Attempted to save but failed. Shouldn't normally happen unless we have a bug in serialization,
         * or commit log has been stopped.
         */
        FAILED_TO_SAVE,

        /**
         * Entry has been successfully evicted, but there were transient listeners present, so we kept the
         * Node around (transient listeners must survive cache eviction).
         */
        EVICTED,
        ;

        boolean isLoaded()
        {
            return this == LOADED || this == MODIFIED || this == FAILED_TO_SAVE;
        }

        boolean isComplete()
        {
            return !(this == LOADING || this == SAVING);
        }
    }

    interface State<K, V>
    {
        Status status();

        default boolean isCompleteable()
        {
            return false;
        }

        default State<K, V> complete()
        {
            throw illegalState(this, "complete()");
        }

        default Loading<K, V> load(K key, Function<K, V> loadFunction)
        {
            throw illegalState(this, "load(key, loadFunction)");
        }

        default Loaded<K, V> initialize(V value)
        {
            throw illegalState(this, "initialize(value)");
        }

        default RunnableResult<V> loading()
        {
            throw illegalState(this, "loading()");
        }

        default V get()
        {
            throw illegalState(this, "get()");
        }

        default State<K, V> set(V value)
        {
            throw illegalState(this, "set(value)");
        }

        default State<K, V> save(BiFunction<V, V, Runnable> saveFunction)
        {
            throw illegalState(this, "save(saveFunction)");
        }

        default RunnableResult<Void> saving()
        {
            throw illegalState(this, "saving()");
        }

        default Throwable failure()
        {
            throw illegalState(this, "failure()");
        }

        default Uninitialized<K, V> reset()
        {
            throw illegalState(this, "reset()");
        }

        default Evicted<K, V> evict()
        {
            throw illegalState(this, "evict()");
        }

        default long estimateOnHeapSize(ToLongFunction<V> estimateFunction)
        {
            return 0;
        }
    }

    private static IllegalStateException illegalState(State<?, ?> state, String method)
    {
        return new IllegalStateException(format("%s invoked on %s", method, state.status()));
    }

    static class Uninitialized<K, V> implements State<K, V>
    {
        static final Uninitialized<?, ?> instance = new Uninitialized<>();

        @SuppressWarnings("unchecked")
        static <K, V> Uninitialized<K, V> instance()
        {
            return (Uninitialized<K, V>) instance;
        }

        @Override
        public Status status()
        {
            return UNINITIALIZED;
        }

        @Override
        public Loading<K, V> load(K key, Function<K, V> loadFunction)
        {
            return new Loading<>(() -> loadFunction.apply(key));
        }

        public Loaded<K, V> initialize(V value)
        {
            return new Loaded<>(value);
        }

        @Override
        public Evicted<K, V> evict()
        {
            return Evicted.instance();
        }
    }

    static class Loading<K, V> extends RunnableResult<V> implements State<K, V>
    {
        Loading(Callable<V> callable)
        {
            super(callable);
        }

        @Override
        public Status status()
        {
            return LOADING;
        }

        @Override
        public boolean isCompleteable()
        {
            return isDone();
        }

        @Override
        public State<K, V> complete()
        {
            if      (!isDone())   return this;
            else if (isSuccess()) return new Loaded<>(result());
            else                  return new FailedToLoad<>(failure());
        }

        @Override
        public RunnableResult<V> loading()
        {
            return this;
        }
    }

    static class Loaded<K, V> implements State<K, V>
    {
        final V original;

        Loaded(V original)
        {
            this.original = original;
        }

        @Override
        public Status status()
        {
            return LOADED;
        }

        @Override
        public V get()
        {
            return original;
        }

        @Override
        public State<K, V> set(V value)
        {
            return value == original ? this : new Modified<>(original, value);
        }

        @Override
        public Evicted<K, V> evict()
        {
            return Evicted.instance();
        }

        @Override
        public long estimateOnHeapSize(ToLongFunction<V> estimateFunction)
        {
            return null == original ? 0 : estimateFunction.applyAsLong(original);
        }
    }

    static class FailedToLoad<K, V> implements State<K, V>
    {
        final Throwable cause;

        FailedToLoad(Throwable cause)
        {
            this.cause = cause;
        }

        @Override
        public Status status()
        {
            return FAILED_TO_LOAD;
        }

        @Override
        public Throwable failure()
        {
            return cause;
        }

        @Override
        public Uninitialized<K, V> reset()
        {
            return Uninitialized.instance();
        }

        @Override
        public Evicted<K, V> evict()
        {
            return Evicted.instance();
        }
    }

    static class Modified<K, V> implements State<K, V>
    {
        final V original;
        V current;

        Modified(V original, V current)
        {
            this.original = original;
            this.current = current;
        }

        @Override
        public Status status()
        {
            return MODIFIED;
        }

        @Override
        public V get()
        {
            return current;
        }

        @Override
        public State<K, V> set(V value)
        {
            if (value == original) // change reverted
                return new Loaded<>(original);

            current = value;
            return this;
        }

        @Override
        public State<K, V> save(BiFunction<V, V, Runnable> saveFunction)
        {
            Runnable runnable = saveFunction.apply(original, current);
            if (null == runnable) // null mutation -> null Runnable -> no change on disk
                return new Loaded<>(current);
            else
                return new Saving<>(current, runnable);
        }

        @Override
        public long estimateOnHeapSize(ToLongFunction<V> estimateFunction)
        {
            return (null == original ? 0 : estimateFunction.applyAsLong(original))
                 + (null == current  ? 0 : estimateFunction.applyAsLong(current));
        }
    }

    static class Saving<K, V> extends RunnableResult<Void> implements State<K, V>
    {
        V current;

        Saving(V current, Runnable saveRunnable)
        {
            super(() -> { saveRunnable.run(); return null; });
            this.current = current;
        }

        @Override
        public Status status()
        {
            return SAVING;
        }

        @Override
        public boolean isCompleteable()
        {
            return isDone();
        }

        @Override
        public State<K, V> complete()
        {
            if      (!isDone())   return this;
            else if (isSuccess()) return new Loaded<>(current);
            else                  return new FailedToSave<>(current, failure());
        }

        @Override
        public RunnableResult<Void> saving()
        {
            return this;
        }
    }

    static class FailedToSave<K, V> implements State<K, V>
    {
        V current;
        final Throwable cause;

        FailedToSave(V current, Throwable cause)
        {
            this.current = current;
            this.cause = cause;
        }

        @Override
        public Status status()
        {
            return FAILED_TO_SAVE;
        }

        @Override
        public V get()
        {
            return current;
        }

        @Override
        public State<K, V> set(V value)
        {
            current = value;
            return this;
        }

        @Override
        public Throwable failure()
        {
            return cause;
        }
    }

    static class Evicted<K, V> implements State<K, V>
    {
        static final Evicted<?, ?> instance = new Evicted<>();

        @SuppressWarnings("unchecked")
        static <K, V> Evicted<K, V> instance()
        {
            return (Evicted<K, V>) instance;
        }

        @Override
        public Status status()
        {
            return EVICTED;
        }

        @Override
        public Uninitialized<K, V> reset()
        {
            return Uninitialized.instance();
        }
    }
}
