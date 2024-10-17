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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.ToLongFunction;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Ints;

import accord.utils.ArrayBuffers.BufferList;
import accord.utils.IntrusiveLinkedListNode;
import accord.utils.Invariants;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.concurrent.Future;

import static org.apache.cassandra.service.accord.AccordCachingState.Status.EVICTED;
import static org.apache.cassandra.service.accord.AccordCachingState.Status.FAILED_TO_LOAD;
import static org.apache.cassandra.service.accord.AccordCachingState.Status.FAILED_TO_SAVE;
import static org.apache.cassandra.service.accord.AccordCachingState.Status.LOADED;
import static org.apache.cassandra.service.accord.AccordCachingState.Status.LOADING;
import static org.apache.cassandra.service.accord.AccordCachingState.Status.MODIFIED;
import static org.apache.cassandra.service.accord.AccordCachingState.Status.SAVING;
import static org.apache.cassandra.service.accord.AccordCachingState.Status.UNINITIALIZED;
import static org.apache.cassandra.service.accord.AccordCachingState.Status.WAITING_TO_LOAD;

/**
 * Global (per CommandStore) state of a cached entity (Command or CommandsForKey).
 */
public class AccordCachingState<K, V> extends IntrusiveLinkedListNode
{
    static final long EMPTY_SIZE = ObjectSizes.measure(new AccordCachingState<>(null, null, null));

    public interface Factory<K, V>
    {
        AccordCachingState<K, V> create(K key, AccordStateCache.Type<K, V, ?>.Instance owner);
    }

    static <K, V> Factory<K, V> defaultFactory()
    {
        return AccordCachingState::new;
    }

    private final K key;
    final AccordStateCache.Type<K, V, ?>.Instance owner;

    private State<K, V> state;

    int references;
    int lastQueriedEstimatedSizeOnHeap;

    AccordCachingState(K key, AccordStateCache.Type<K, V, ?>.Instance owner)
    {
        this.key = key;
        this.owner = owner;
        //noinspection unchecked
        this.state = (State<K, V>) Uninitialized.instance;
    }

    private AccordCachingState(K key, AccordStateCache.Type<K, V, ?>.Instance owner, State<K, V> state)
    {
        this.key = key;
        this.owner = owner;
        this.state = state;
    }

    void unlink()
    {
        Invariants.checkState(references == 0);
        remove();
    }

    boolean isUnqueued()
    {
        return isFree();
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

    boolean isLoadingOrWaiting()
    {
        return status().isLoadingOrWaiting();
    }

    public boolean isComplete()
    {
        return status().isComplete();
    }

    int lastQueriedEstimatedSizeOnHeap()
    {
        return lastQueriedEstimatedSizeOnHeap & 0x7fffffff;
    }

    int estimatedSizeOnHeap(ToLongFunction<V> estimator)
    {
        return lastQueriedEstimatedSizeOnHeap = Ints.checkedCast(EMPTY_SIZE + estimateStateOnHeapSize(estimator));
    }

    long estimatedSizeOnHeapDelta(ToLongFunction<V> estimator)
    {
        long prevSize = lastQueriedEstimatedSizeOnHeap();
        return estimatedSizeOnHeap(estimator) - prevSize;
    }

    void setShouldUpdateSize()
    {
        lastQueriedEstimatedSizeOnHeap |= 0x80000000;
    }

    boolean shouldUpdateSize()
    {
        return lastQueriedEstimatedSizeOnHeap < 0;
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
        return state.status();
    }

    public void initialize(V value)
    {
        state(state.initialize(value));
    }

    protected State<K, V> state(State<K, V> next)
    {
        return state = next;
    }

    @VisibleForTesting
    protected State<K, V> state()
    {
        return state;
    }

    public void readyToLoad()
    {
        state(state.readyToLoad());
    }

    public LoadingOrWaiting<K, V> loadingOrWaiting()
    {
        return state.loadingOrWaiting();
    }

    void notifyListeners(BiConsumer<AccordStateCache.Listener<K, V>, AccordCachingState<K, V>> notify)
    {
        owner.notifyListeners(notify, this);
    }

    public interface OnLoaded
    {
        <K, V> void onLoaded(AccordCachingState<K, V> state, V value, Throwable fail);

        static OnLoaded immediate()
        {
            return new OnLoaded()
            {
                @Override
                public <K, V> void onLoaded(AccordCachingState<K, V> state, V value, Throwable fail)
                {
                    if (fail == null) state.state(state.state().loaded(value));
                    else state.state(state.state().failedToLoad());
                }
            };
        }
    }

    public interface OnSaved
    {
        <K, V> void onSaved(AccordCachingState<K, V> state, Object identity, Throwable fail);

        static OnSaved immediate()
        {
            return new OnSaved()
            {
                @Override
                public <K, V> void onSaved(AccordCachingState<K, V> state, Object identity, Throwable fail)
                {
                    state.saved(identity, fail);
                }
            };
        }
    }

    public Loading<K, V> load(Function<Runnable, Future<?>> loadExecutor, BiFunction<AccordCommandStore, K, V> load, OnLoaded onLoaded)
    {
        Loading<K, V> loading = state.load(loadExecutor.apply(() -> {
            V result;
            try
            {
                result = load.apply(owner.commandStore, key);
            }
            catch (Throwable t)
            {
                onLoaded.onLoaded(this, null, t);
                throw t;
            }
            onLoaded.onLoaded(this, result, null);
        }));
        state(loading);
        return loading;
    }

    public Loading<K, V> loading()
    {
        return state.loading();
    }

    public V get()
    {
        return state.get();
    }

    public void set(V value)
    {
        setShouldUpdateSize();
        state(state.set(value));
    }

    public void loaded(V value)
    {
        setShouldUpdateSize();
        state(state.loaded(value));
    }

    public void failedToLoad()
    {
        state(state.failedToLoad());
    }

    /**
     * Submits a save runnable to the specified executor. When the runnable
     * has completed, the state save will have either completed or failed.
     */
    @VisibleForTesting
    void save(Function<Runnable, Future<?>> saveExecutor, BiFunction<AccordCommandStore, V, Runnable> saveFunction, OnSaved onSaved)
    {
        State<K, V> current = state;
        Runnable save = saveFunction.apply(owner.commandStore, current.get());
        if (null == save) // null mutation -> null Runnable -> no change on disk
        {
            state(current.saved());
        }
        else
        {
            Object identity = new Object();
            state(state.save(saveExecutor.apply(() -> {
                try
                {
                    save.run();
                }
                catch (Throwable t)
                {
                    onSaved.onSaved(this, identity, t);
                    throw t;
                }
                onSaved.onSaved(this, identity, null);
            }), identity));
        }
    }

    boolean saved(Object identity, Throwable fail)
    {
        if (state.status() != SAVING || state.saving().identity != identity)
            return false;

        if (fail != null)
        {
            state(state.failedToSave(fail));
            return false;
        }
        else
        {
            state(state.saved());
            return true;
        }
    }

    public Future<?> saving()
    {
        return state.saving().saving;
    }

    public AccordCachingState<K, V> evicted()
    {
        state(state.evicted());
        return this;
    }

    public Throwable failure()
    {
        return state.failure();
    }

    long estimateStateOnHeapSize(ToLongFunction<V> estimateFunction)
    {
        return state.estimateOnHeapSize(estimateFunction);
    }

    public enum Status
    {
        UNINITIALIZED,
        WAITING_TO_LOAD,
        LOADING,
        LOADED,
        MODIFIED,
        SAVING,

        /**
         * Consumers should never see this state
         */
        FAILED_TO_LOAD,

        /**
         * Attempted to save but failed. Shouldn't normally happen unless we have a bug in serialization,
         * or commit log has been stopped.
         */
        FAILED_TO_SAVE,

        EVICTED
        ;

        boolean isLoaded()
        {
            return this == LOADED || this == MODIFIED || this == SAVING || this == FAILED_TO_SAVE;
        }

        boolean isLoadingOrWaiting()
        {
            return this == LOADING || this == WAITING_TO_LOAD;
        }

        boolean isComplete()
        {
            return !(this == LOADING || this == SAVING);
        }
    }

    interface State<K, V>
    {
        Status status();

        // transition to Uninitialized
        default Uninitialized<K, V> reset()
        {
            throw illegalState(this, "reset()");
        }

        // transition to Loaded from Uninitialized
        default Loaded<K, V> initialize(V value)
        {
            throw illegalState(this, "initialize(value)");
        }

        // transition to WaitingToLoad
        default WaitingToLoad<K, V> readyToLoad()
        {
            throw illegalState(this, "waitToLoad()");
        }

        // transition to Loading
        default Loading<K, V> load(Future<?> loading)
        {
            throw illegalState(this, "load()");
        }

        // transition to Loaded
        default Loaded<K, V> loaded(V value)
        {
            throw illegalState(this, "loaded(value)");
        }

        // transition to Uninitialized (should be removed from cache immediately, and all referents notified of failure)
        default FailedToLoad<K, V> failedToLoad()
        {
            throw illegalState(this, "failedToLoad()");
        }

        // transition to Modified
        default State<K, V> set(V value)
        {
            throw illegalState(this, "set(value)");
        }

        // transition to Saving
        default State<K, V> save(Future<?> saving, Object identity)
        {
            throw illegalState(this, "save(saveFunction, executor)");
        }

        // transition to Saved
        default State<K, V> saved()
        {
            throw illegalState(this, "saved()");
        }

        // transition to Evicted
        default Evicted<K, V> evicted()
        {
            return (Evicted<K, V>) Evicted.instance;
        }

        default State<K, V> failedToSave(Throwable cause)
        {
            throw illegalState(this, "failedToSave(cause)");
        }

        // get current LoadingOrWaiting
        default LoadingOrWaiting<K, V> loadingOrWaiting()
        {
            throw illegalState(this, "loadingOrWaiting()");
        }

        // get current Loading
        default Loading<K, V> loading()
        {
            throw illegalState(this, "loading()");
        }

        // get any save future
        default Saving<K, V> saving()
        {
            throw illegalState(this, "saving()");
        }

        // get current value
        default V get()
        {
            throw illegalState(this, "get()");
        }

        // get current failure
        default Throwable failure()
        {
            throw illegalState(this, "failure()");
        }

        default long estimateOnHeapSize(ToLongFunction<V> estimateFunction)
        {
            return 0;
        }
    }

    private static IllegalStateException illegalState(State<?, ?> state, String method)
    {
        throw Invariants.illegalState("%s invoked on %s", method, state.status());
    }

    static class Uninitialized<K, V> implements State<K, V>
    {
        static final Uninitialized<?, ?> instance = new Uninitialized<>();

        @Override
        public Status status()
        {
            return UNINITIALIZED;
        }

        @Override
        public WaitingToLoad<K, V> readyToLoad()
        {
            return new WaitingToLoad<>();
        }

        public Loaded<K, V> initialize(V value)
        {
            return new Loaded<>(value);
        }
    }

    static class FailedToLoad<K, V> implements State<K, V>
    {
        static final FailedToLoad<?, ?> instance = new FailedToLoad<>();

        @Override
        public Status status()
        {
            return FAILED_TO_LOAD;
        }
    }

    public static abstract class LoadingOrWaiting<K, V> implements State<K, V>
    {
        Collection<AccordTask<?>> waiters;

        public LoadingOrWaiting()
        {
        }

        public LoadingOrWaiting(Collection<AccordTask<?>> waiters)
        {
            this.waiters = waiters;
        }

        public Collection<AccordTask<?>> waiters()
        {
            return waiters != null ? waiters : Collections.emptyList();
        }

        public BufferList<AccordTask<?>> copyWaiters()
        {
            BufferList<AccordTask<?>> list = new BufferList<>();
            if (waiters != null)
                list.addAll(waiters);
            return list;
        }

        public void add(AccordTask<?> waiter)
        {
            if (waiters == null)
                waiters = new ArrayList<>();
            waiters.add(waiter);
        }

        public void remove(AccordTask<?> waiter)
        {
            if (waiters != null)
            {
                waiters.remove(waiter);
                if (waiters.isEmpty())
                    waiters = null;
            }
        }

        @Override
        public LoadingOrWaiting<K, V> loadingOrWaiting()
        {
            return this;
        }
    }

    static class WaitingToLoad<K, V> extends LoadingOrWaiting<K, V>
    {
        @Override
        public Status status()
        {
            return WAITING_TO_LOAD;
        }

        @Override
        public Loading<K, V> load(Future<?> loading)
        {
            Invariants.checkState(waiters == null || !waiters.isEmpty());
            Loading<K, V> result = new Loading<>(waiters, loading);
            waiters = Collections.emptyList();
            return result;
        }
    }

    static class Loading<K, V> extends LoadingOrWaiting<K, V>
    {
        // TODO (expected): support cancellation
        public final Future<?> loading;

        public Loading(Collection<AccordTask<?>> waiters, Future<?> loading)
        {
            super(waiters);
            this.loading = loading;
        }

        @Override
        public Status status()
        {
            return LOADING;
        }

        @Override
        public Loading<K, V> loading()
        {
            return this;
        }

        @Override
        public Loaded<K, V> loaded(V value)
        {
            return new Loaded<>(value);
        }

        @Override
        public FailedToLoad<K, V> failedToLoad()
        {
            return (FailedToLoad<K, V>) FailedToLoad.instance;
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
            return value == original ? this : new Modified<>(value);
        }

        @Override
        public long estimateOnHeapSize(ToLongFunction<V> estimateFunction)
        {
            return null == original ? 0 : estimateFunction.applyAsLong(original);
        }
    }

    static class Modified<K, V> implements State<K, V>
    {
        V current;

        Modified(V current)
        {
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
            current = value;
            return this;
        }

        @Override
        public State<K, V> saved()
        {
            return new Loaded<>(current);
        }

        @Override
        public Saving<K, V> save(Future<?> saving, Object identity)
        {
            return new Saving<>(saving, identity, current);
        }

        @Override
        public long estimateOnHeapSize(ToLongFunction<V> estimateFunction)
        {
            return (null == current  ? 0 : estimateFunction.applyAsLong(current));
        }
    }

    static class Saving<K, V> implements State<K, V>
    {
        final Future<?> saving;
        final Object identity;
        final V value;

        Saving(Future<?> saving, Object identity, V current)
        {
            this.saving = saving;
            this.identity = identity;
            this.value = current;
        }

        @Override
        public Status status()
        {
            return SAVING;
        }

        @Override
        public V get()
        {
            return value;
        }

        @Override
        public State<K, V> set(V value)
        {
            saving.cancel(false);
            return new Modified<>(value);
        }

        @Override
        public State<K, V> saved()
        {
            return new Loaded<>(value);
        }

        @Override
        public State<K, V> failedToSave(Throwable cause)
        {
            return new FailedToSave<>(value, cause);
        }

        @Override
        public Saving<K, V> saving()
        {
            return this;
        }
    }

    static class FailedToSave<K, V> implements State<K, V>
    {
        final V current;
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
            return new Modified<>(value);
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

        private Evicted()
        {
        }

        @Override
        public Status status()
        {
            return EVICTED;
        }
    }

}
