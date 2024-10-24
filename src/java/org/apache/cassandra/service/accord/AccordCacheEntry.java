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
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Ints;

import accord.utils.ArrayBuffers.BufferList;
import accord.utils.IntrusiveLinkedListNode;
import accord.utils.Invariants;
import accord.utils.async.Cancellable;
import org.apache.cassandra.service.accord.AccordCache.Adapter;
import org.apache.cassandra.utils.ObjectSizes;

import static org.apache.cassandra.service.accord.AccordCacheEntry.Status.EVICTED;
import static org.apache.cassandra.service.accord.AccordCacheEntry.Status.FAILED_TO_LOAD;
import static org.apache.cassandra.service.accord.AccordCacheEntry.Status.FAILED_TO_SAVE;
import static org.apache.cassandra.service.accord.AccordCacheEntry.Status.LOADED;
import static org.apache.cassandra.service.accord.AccordCacheEntry.Status.LOADING;
import static org.apache.cassandra.service.accord.AccordCacheEntry.Status.MODIFIED;
import static org.apache.cassandra.service.accord.AccordCacheEntry.Status.SAVING;
import static org.apache.cassandra.service.accord.AccordCacheEntry.Status.WAITING_TO_LOAD;

/**
 * Global (per CommandStore) state of a cached entity (Command or CommandsForKey).
 */
public class AccordCacheEntry<K, V> extends IntrusiveLinkedListNode
{
    public enum Status
    {
        UNINITIALIZED,
        WAITING_TO_LOAD(UNINITIALIZED),
        LOADING(WAITING_TO_LOAD),
        /**
         * Consumers should never see this state
         */
        FAILED_TO_LOAD(LOADING),

        LOADED(true, false, UNINITIALIZED, LOADING),
        MODIFIED(true, false, LOADED),
        SAVING(true, true, MODIFIED),

        /**
         * Attempted to save but failed. Shouldn't normally happen unless we have a bug in serialization,
         * or commit log has been stopped.
         */
        FAILED_TO_SAVE(true, true, SAVING),

        UNUSED, // spacing to permit easier bit masks

        EVICTED(WAITING_TO_LOAD, LOADING, LOADED, FAILED_TO_LOAD),
        ;

        static final Status[] VALUES = values();
        static
        {
            MODIFIED.permittedFrom |= 1 << MODIFIED.ordinal();
            MODIFIED.permittedFrom |= 1 << SAVING.ordinal();
            MODIFIED.permittedFrom |= 1 << FAILED_TO_SAVE.ordinal();
            LOADED.permittedFrom |= 1 << SAVING.ordinal();
            LOADED.permittedFrom |= 1 << MODIFIED.ordinal();
            for (Status status : VALUES)
            {
                Invariants.checkState((status.ordinal() & IS_LOADED) != 0 == status.loaded);
                Invariants.checkState(((status.ordinal() & IS_LOADED) != 0 && (status.ordinal() & IS_NESTED) != 0) == status.nested);
            }
        }

        final boolean loaded;
        final boolean nested;
        int permittedFrom;

        Status(Status ... statuses)
        {
            this(false, false, statuses);
        }

        Status(boolean loaded, boolean nested, Status ... statuses)
        {
            this.loaded = loaded;
            this.nested = nested;
            for (Status status : statuses)
                permittedFrom |= 1 << status.ordinal();
        }
    }

    static final int STATUS_MASK = 0x0000001F;
    static final int SHRUNK = 0x00000040;
    static final int NO_EVICT = 0x00000020;
    static final int IS_LOADED = 0x4;
    static final int IS_NESTED = 0x2; // only valid to test if already tested NORMAL
    static final int IS_LOADING_OR_WAITING_MASK = 0x6; // only valid to test if already tested NORMAL
    static final int IS_LOADING_OR_WAITING = 0x2; // only valid to test if already tested NORMAL
    static final long EMPTY_SIZE = ObjectSizes.measure(new AccordCacheEntry<>(null, null));

    private final K key;
    final AccordCache.Type<K, V, ?>.Instance owner;

    private Object state;
    private int status;
    int sizeOnHeap;
    private volatile int references;
    private static final AtomicIntegerFieldUpdater<AccordCacheEntry> referencesUpdater = AtomicIntegerFieldUpdater.newUpdater(AccordCacheEntry.class, "references");

    AccordCacheEntry(K key, AccordCache.Type<K, V, ?>.Instance owner)
    {
        this.key = key;
        this.owner = owner;
    }

    void unlink()
    {
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

    public int references()
    {
        return references;
    }

    public int increment()
    {
        return referencesUpdater.incrementAndGet(this);
    }

    public int decrement()
    {
        return referencesUpdater.decrementAndGet(this);
    }

    boolean isLoaded()
    {
        return (status & IS_LOADED) != 0;
    }

    boolean isNested()
    {
        Invariants.checkState(isLoaded());
        return (status & IS_NESTED) != 0;
    }

    boolean isShrunk()
    {
        return (status & SHRUNK) != 0;
    }

    public boolean is(Status status)
    {
        return (this.status & STATUS_MASK) == status.ordinal();
    }

    boolean isLoadingOrWaiting()
    {
        return (status & IS_LOADING_OR_WAITING_MASK) == IS_LOADING_OR_WAITING;
    }

    public boolean isComplete()
    {
        return !is(LOADING) && !is(SAVING);
    }

    int noEvictGeneration()
    {
        Invariants.checkState(isNoEvict());
        return (status >>> 8) & 0xffff;
    }

    int noEvictMaxAge()
    {
        Invariants.checkState(isNoEvict());
        return status >>> 24;
    }

    boolean isNoEvict()
    {
        return (status & NO_EVICT) != 0;
    }

    int sizeOnHeap()
    {
        return sizeOnHeap;
    }

    void updateSize(AccordCache.Type<K, V, ?> parent)
    {
        // TODO (expected): we aren't weighing the keys
        int newSizeOnHeap = Ints.saturatedCast(EMPTY_SIZE + estimateOnHeapSize(parent.adapter()));
        parent.updateSize(newSizeOnHeap, newSizeOnHeap - sizeOnHeap, references == 0, true);
        sizeOnHeap = newSizeOnHeap;
    }

    void initSize(AccordCache.Type<K, V, ?> parent)
    {
        // TODO (expected): we aren't weighing the keys
        sizeOnHeap = Ints.saturatedCast(EMPTY_SIZE);
        parent.updateSize(sizeOnHeap, sizeOnHeap, false, false);
    }

    @Override
    public String toString()
    {
        return "Node{" + status() +
               ", key=" + key() +
               ", references=" + references +
               "}@" + Integer.toHexString(System.identityHashCode(this));
    }

    public Status status()
    {
        return Status.VALUES[(status & STATUS_MASK)];
    }

    private void setStatus(Status newStatus)
    {
        Invariants.checkState((newStatus.permittedFrom & (1 << (status & STATUS_MASK))) != 0, "%s not permitted from %s", newStatus, status());
        status &= ~STATUS_MASK;
        status |= newStatus.ordinal();
        Invariants.checkState(status() == newStatus);
    }

    public void initialize(V value)
    {
        Invariants.checkState(state == null);
        setStatus(LOADED);
        state = value;
    }

    public void readyToLoad()
    {
        Invariants.checkState(state == null);
        setStatus(WAITING_TO_LOAD);
        state = new WaitingToLoad();
    }

    public void markNoEvict(int generation, int maxAge)
    {
        Invariants.checkState((maxAge & ~0xff) == 0);
        Invariants.checkState((generation & ~0xffff) == 0);
        status |= NO_EVICT;
        status |= generation << 8;
        status |= maxAge << 24;
    }

    public LoadingOrWaiting loadingOrWaiting()
    {
        return (LoadingOrWaiting)state;
    }

    void notifyListeners(BiConsumer<AccordCache.Listener<K, V>, AccordCacheEntry<K, V>> notify)
    {
        owner.notifyListeners(notify, this);
    }

    public interface OnLoaded
    {
        <K, V> void onLoaded(AccordCacheEntry<K, V> state, V value, Throwable fail);

        static OnLoaded immediate()
        {
            return new OnLoaded()
            {
                @Override
                public <K, V> void onLoaded(AccordCacheEntry<K, V> state, V value, Throwable fail)
                {
                    if (fail == null) state.loaded(value);
                    else state.failedToLoad();
                }
            };
        }
    }

    public interface OnSaved
    {
        <K, V> void onSaved(AccordCacheEntry<K, V> state, Object identity, Throwable fail);

        static OnSaved immediate()
        {
            return new OnSaved()
            {
                @Override
                public <K, V> void onSaved(AccordCacheEntry<K, V> state, Object identity, Throwable fail)
                {
                    state.saved(identity, fail);
                }
            };
        }
    }

    public <P> Loading load(BiFunction<P, Runnable, Cancellable> loadExecutor, P param, Adapter<K, V, ?> adapter, OnLoaded onLoaded)
    {
        Invariants.checkState(is(WAITING_TO_LOAD), "%s", this);
        Loading loading = ((WaitingToLoad)state).load(loadExecutor.apply(param, () -> {
            V result;
            try
            {
                result = adapter.load(owner.commandStore, key);
            }
            catch (Throwable t)
            {
                onLoaded.onLoaded(this, null, t);
                throw t;
            }
            onLoaded.onLoaded(this, result, null);
        }));
        setStatus(LOADING);
        state = loading;
        return loading;
    }

    public Loading testLoad()
    {
        Invariants.checkState(is(WAITING_TO_LOAD));
        Loading loading = ((WaitingToLoad)state).load(() -> {});
        setStatus(LOADING);
        state = loading;
        return loading;
    }

    public Loading loading()
    {
        Invariants.checkState(is(LOADING), "%s", this);
        return (Loading) state;
    }

    // must own the cache's lock when invoked. this is true of most methods in the class,
    // but this one is less obvious so named as to draw attention
    public V getExclusive()
    {
        Invariants.checkState(owner == null || owner.commandStore == null || owner.commandStore.executor().isOwningThread());
        Invariants.checkState(isLoaded(), "%s", this);
        if (isShrunk())
        {
            AccordCache.Type<K, V, ?> parent = owner.parent();
            inflate(key, parent.adapter());
            updateSize(parent);
        }

        return (V)unwrap();
    }

    private Object unwrap()
    {
        return isNested() ? ((Nested)state).state : state;
    }

    // must own the cache's lock when invoked
    void setExclusive(V value)
    {
        if (value == state)
            return;

        Saving cancel = is(SAVING) ? ((Saving)state) : null;
        setStatus(MODIFIED);
        state = value;
        updateSize(owner.parent());
        // TODO (expected): do we want to always cancel in-progress saving?
        if (cancel != null)
            cancel.saving.cancel();
    }

    public void loaded(V value)
    {
        setStatus(LOADED);
        state = value;
        updateSize(owner.parent());
    }

    public void testLoaded(V value)
    {
        setStatus(LOADED);
        state = value;
    }

    public void failedToLoad()
    {
        setStatus(FAILED_TO_LOAD);
        state = null;
    }

    boolean tryShrink()
    {
        if (!isLoaded())
            return false;

        AccordCache.Type<K, V, ?> parent = owner.parent();
        if (!tryShrink(key, parent.adapter()))
            return false;
        updateSize(parent);
        return true;
    }

    V tryGetFull()
    {
        return isShrunk() ? null : (V)unwrap();
    }

    Object tryGetShrunk()
    {
        return isShrunk() ? unwrap() : null;
    }

    boolean isNull()
    {
        return state == null;
    }

    /**
     * Submits a save runnable to the specified executor. When the runnable
     * has completed, the state save will have either completed or failed.
     */
    @VisibleForTesting
    void save(Function<Runnable, Cancellable> saveExecutor, Adapter<K, V, ?> adapter, OnSaved onSaved)
    {
        V full = isShrunk() ? null : (V)state;
        Object shrunk = isShrunk() ? state : null;
        Runnable save = adapter.save(owner.commandStore, key, full, shrunk);
        if (null == save) // null mutation -> null Runnable -> no change on disk
        {
            setStatus(LOADED);
        }
        else
        {
            setStatus(SAVING);
            Object identity = new Object();
            Cancellable saving = saveExecutor.apply(() -> {
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
            });
            state = new Saving(saving, identity, state);
        }
    }

    boolean saved(Object identity, Throwable fail)
    {
        if (!is(SAVING))
            return false;

        Saving saving = (Saving) state;
        if (saving.identity != identity)
            return false;

        if (fail != null)
        {
            setStatus(FAILED_TO_SAVE);
            state = new FailedToSave(fail, ((Saving)state).state);
            return false;
        }
        else
        {
            setStatus(LOADED);
            state = saving.state;
            return true;
        }
    }

    protected void saved()
    {
        Invariants.checkState(is(MODIFIED));
        setStatus(LOADED);
    }

    public Cancellable saving()
    {
        return ((Saving)state).saving;
    }

    public AccordCacheEntry<K, V> evicted()
    {
        setStatus(EVICTED);
        state = null;
        return this;
    }

    public Throwable failure()
    {
        return ((FailedToSave)state).cause;
    }

    private boolean tryShrink(K key, Adapter<K, V, ?> adapter)
    {
        Invariants.checkState(!isNested());
        if (isShrunk() || state == null)
            return false;

        Object update = adapter.fullShrink(key, (V)state);
        if (update == null || update == state)
            return false;

        state = update;
        status |= SHRUNK;
        return true;
    }

    private void inflate(K key, Adapter<K, V, ?> adapter)
    {
        Invariants.checkState(isShrunk());
        if (isNested())
        {
            Nested nested = (Nested) state;
            nested.state = adapter.inflate(key, nested.state);
        }
        else
        {
            state = adapter.inflate(key, state);
        }
        status &= ~SHRUNK;
    }

    private long estimateOnHeapSize(Adapter<K, V, ?> adapter)
    {
        Object current = unwrap();
        if (current == null) return 0;
        else if (isShrunk()) return adapter.estimateShrunkHeapSize(current);
        return adapter.estimateHeapSize((V)current);
    }

    public static abstract class LoadingOrWaiting
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
    }

    static class WaitingToLoad extends LoadingOrWaiting
    {
        public Loading load(Cancellable loading)
        {
            Invariants.paranoid(waiters == null || !waiters.isEmpty());
            Loading result = new Loading(waiters, loading);
            waiters = Collections.emptyList();
            return result;
        }
    }

    static class Loading extends LoadingOrWaiting
    {
        public final Cancellable loading;

        public Loading(Collection<AccordTask<?>> waiters, Cancellable loading)
        {
            super(waiters);
            this.loading = loading;
        }
    }

    static class Nested
    {
        Object state;
    }

    static class Saving extends Nested
    {
        final Cancellable saving;
        final Object identity;

        Saving(Cancellable saving, Object identity, Object state)
        {
            this.saving = saving;
            this.identity = identity;
            this.state = state;
        }
    }

    static class FailedToSave extends Nested
    {
        final Throwable cause;

        FailedToSave(Throwable cause, Object state)
        {
            this.cause = cause;
            this.state = state;
        }

        public Throwable failure()
        {
            return cause;
        }
    }

    public static <K, V> AccordCacheEntry<K, V> createReadyToLoad(K key, AccordCache.Type<K, V, ?>.Instance owner)
    {
        AccordCacheEntry<K, V> node = new AccordCacheEntry<>(key, owner);
        node.readyToLoad();
        return node;
    }
}
