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
import java.util.List;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.BiConsumer;

import javax.annotation.Nullable;

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
import static org.apache.cassandra.service.accord.AccordCacheEntry.Status.WAITING_TO_SAVE;

/**
 * Global (per CommandStore) state of a cached entity (Command or CommandsForKey).
 */
public class AccordCacheEntry<K, V> extends IntrusiveLinkedListNode
{
    public enum Status
    {
        UNINITIALIZED,

        UNUSED1, // spacing to permit easier bit masks

        WAITING_TO_LOAD(UNINITIALIZED),
        LOADING(WAITING_TO_LOAD),

        /**
         * Consumers should never see this state
         */
        FAILED_TO_LOAD(LOADING),

        UNUSED2, // spacing to permit easier bit masks
        UNUSED3, // spacing to permit easier bit masks
        UNUSED4, // spacing to permit easier bit masks

        LOADED(true, false, UNINITIALIZED, LOADING),
        MODIFIED(true, false, LOADED),

        UNUSED5, // spacing to permit easier bit masks
        UNUSED6, // spacing to permit easier bit masks

        WAITING_TO_SAVE(true, true, MODIFIED),
        SAVING(true, true, MODIFIED, WAITING_TO_SAVE),

        /**
         * Attempted to save but failed. Shouldn't normally happen unless we have a bug in serialization,
         * or commit log has been stopped.
         */
        FAILED_TO_SAVE(true, true, SAVING),

        UNUSED7, // spacing to permit easier bit masks

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
                if (status.name().startsWith("UNUSED")) continue;
                Invariants.require((status.ordinal() & IS_LOADED) != 0 == status.loaded);
                Invariants.require(((status.ordinal() & IS_LOADED) != 0 && (status.ordinal() & IS_NESTED) != 0) == status.nested);
                Invariants.require(((status.ordinal() & IS_LOADING_OR_WAITING_MASK) == IS_LOADING_OR_WAITING) == (status == LOADING || status == WAITING_TO_LOAD));
                Invariants.require(((status.ordinal() & IS_SAVING_OR_WAITING_MASK) == IS_SAVING_OR_WAITING) == (status == SAVING || status == WAITING_TO_SAVE));
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
    static final int IS_NOT_EVICTED = 0xF;
    static final int IS_LOADED = 0x8;
    static final int IS_NESTED = 0x4;
    static final int IS_LOADING_OR_WAITING_MASK = 0x6;
    static final int IS_LOADING_OR_WAITING = 0x2;
    static final int IS_SAVING_OR_WAITING_MASK = 0xE;
    static final int IS_SAVING_OR_WAITING = 0xC;
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

    boolean isModified()
    {
        return (status & IS_NOT_EVICTED) >= MODIFIED.ordinal();
    }

    boolean isNested()
    {
        Invariants.require(isLoaded());
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

    boolean isSavingOrWaiting()
    {
        return (status & IS_SAVING_OR_WAITING_MASK) == IS_SAVING_OR_WAITING;
    }

    public boolean isComplete()
    {
        return !is(LOADING) && !is(SAVING);
    }

    int noEvictGeneration()
    {
        Invariants.require(isNoEvict());
        return (status >>> 8) & 0xffff;
    }

    int noEvictMaxAge()
    {
        Invariants.require(isNoEvict());
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
        Invariants.require((newStatus.permittedFrom & (1 << (status & STATUS_MASK))) != 0, "%s not permitted from %s", newStatus, status());
        setStatusUnsafe(newStatus);
    }

    private void setStatusUnsafe(Status newStatus)
    {
        status &= ~STATUS_MASK;
        status |= newStatus.ordinal();
    }

    public void initialize(V value)
    {
        Invariants.require(state == null);
        setStatus(LOADED);
        state = value;
    }

    public void readyToLoad()
    {
        Invariants.require(state == null);
        setStatus(WAITING_TO_LOAD);
        state = new WaitingToLoad();
    }

    public void markNoEvict(int generation, int maxAge)
    {
        Invariants.require((maxAge & ~0xff) == 0);
        Invariants.require((generation & ~0xffff) == 0);
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

    public interface LoadExecutor<P1, P2>
    {
        <K, V> Cancellable load(P1 p1, P2 p2, AccordCacheEntry<K, V> entry);
    }

    // functions as both an identity object, and a register of listeners
    public static class UniqueSave
    {
        @Nullable List<Runnable> onSuccess;
        void onSuccess(Runnable onSuccess)
        {
            if (this.onSuccess == null)
                this.onSuccess = new ArrayList<>();
            this.onSuccess.add(onSuccess);
        }
    }

    public interface SaveExecutor
    {
        Cancellable save(AccordCacheEntry<?, ?> saving, UniqueSave identity, Runnable save);
    }

    public <P1, P2> Loading load(LoadExecutor<P1, P2> loadExecutor, P1 p1, P2 p2)
    {
        Invariants.require(is(WAITING_TO_LOAD), "%s", this);

        WaitingToLoad cur = (WaitingToLoad)state;
        Loading loading = cur.load(loadExecutor.load(p1, p2, this));
        setStatus(LOADING);
        state = loading;
        return loading;
    }

    public Loading testLoad()
    {
        Invariants.require(is(WAITING_TO_LOAD));
        Loading loading = ((WaitingToLoad)state).load(() -> {});
        setStatus(LOADING);
        state = loading;
        return loading;
    }

    public Loading loading()
    {
        Invariants.require(is(LOADING), "%s", this);
        return (Loading) state;
    }

    // must own the cache's lock when invoked. this is true of most methods in the class,
    // but this one is less obvious so named as to draw attention
    public V getExclusive()
    {
        Invariants.require(owner == null || owner.commandStore == null || owner.commandStore.executor().isOwningThread());
        Invariants.require(isLoaded(), "%s", this);
        if (isShrunk())
        {
            AccordCache.Type<K, V, ?> parent = owner.parent();
            inflate(owner.commandStore, key, parent.adapter());
            updateSize(parent);
        }

        return (V)unwrap();
    }

    public Object getOrShrunkExclusive()
    {
        Invariants.require(owner == null || owner.commandStore == null || owner.commandStore.executor().isOwningThread());
        Invariants.require(isLoaded(), "%s", this);
        return unwrap();
    }

    public V tryGetExclusive()
    {
        Invariants.require(owner == null || owner.commandStore == null || owner.commandStore.executor().isOwningThread());
        if (!isLoaded() || isShrunk())
            return null;
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
        if (is(WAITING_TO_SAVE))
        {
            ((WaitingToSave<K, V>) state).state = value;
            if (owner.parent().adapter().canSave(value, null))
                save();
        }
        else
        {
            setStatus(MODIFIED);
            state = value;
        }
        updateSize(owner.parent());
        // TODO (expected): do we want to cancel in-progress saving?
        if (cancel != null && cancel.identity.onSuccess == null)
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

    boolean saveWhenReady()
    {
        V full = isShrunk() ? null : (V)state;
        Object shrunk = isShrunk() ? state : null;
        if (owner.parent().adapter().canSave(full, shrunk))
            return save();

        setStatus(WAITING_TO_SAVE);
        UniqueSave identity = new UniqueSave();
        state = new WaitingToSave<>(identity, state);
        return true;
    }

    /**
     * Submits a save runnable to the specified executor. When the runnable
     * has completed, the state save will have either completed or failed.
     */
    @VisibleForTesting
    boolean save()
    {
        WaitingToSave<K, V> waitingToSave = is(WAITING_TO_SAVE) ? (WaitingToSave<K, V>)state : null;
        Object state = waitingToSave == null ? this.state : waitingToSave.state;
        V full = isShrunk() ? null : (V)state;
        Object shrunk = isShrunk() ? state : null;
        Runnable save = owner.parent().adapter().save(owner.commandStore, key, full, shrunk);

        UniqueSave identity = waitingToSave == null ? new UniqueSave() : waitingToSave.identity;
        if (null == save) // null mutation -> null Runnable -> no change on disk
        {
            setStatus(LOADED);
            if (waitingToSave != null)
                this.state = state;
            if (identity.onSuccess != null)
                identity.onSuccess.forEach(Runnable::run);
            return false;
        }
        else
        {
            setStatus(SAVING);
            Cancellable saving = owner.parent().parent().saveExecutor.save(this, identity, save);
            this.state = new Saving(saving, identity, state);
            return true;
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
        Invariants.require(is(MODIFIED));
        setStatus(LOADED);
    }

    public SavingOrWaitingToSave savingOrWaitingToSave()
    {
        return (SavingOrWaitingToSave) state;
    }

    public AccordCacheEntry<K, V> evicted()
    {
        if (isNoEvict())
            setStatusUnsafe(EVICTED);
        else setStatus(EVICTED);
        state = null;
        return this;
    }

    public Throwable failure()
    {
        return ((FailedToSave)state).cause;
    }

    private boolean tryShrink(K key, Adapter<K, V, ?> adapter)
    {
        if (isShrunk() || state == null)
            return false;

        Object cur = unwrap();
        Object upd = adapter.fullShrink(key, (V)cur);
        if (upd == null || upd == cur)
            return false;

        if (isNested()) ((Nested)this.state).state = upd;
        else this.state = upd;
        status |= SHRUNK;
        return true;
    }

    private void inflate(AccordCommandStore commandStore, K key, Adapter<K, V, ?> adapter)
    {
        Invariants.require(isShrunk());
        if (isNested())
        {
            Nested nested = (Nested) state;
            nested.state = adapter.inflate(commandStore, key, nested.state);
        }
        else
        {
            state = adapter.inflate(commandStore, key, state);
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

    static class SavingOrWaitingToSave extends Nested
    {
        final UniqueSave identity;

        SavingOrWaitingToSave(UniqueSave identity, Object state)
        {
            this.identity = identity;
            this.state = state;
        }
    }

    static class Saving extends SavingOrWaitingToSave
    {
        final Cancellable saving;

        Saving(Cancellable saving, UniqueSave identity, Object state)
        {
            super(identity, state);
            this.saving = saving;
        }
    }

    static class WaitingToSave<K, V> extends SavingOrWaitingToSave
    {
        WaitingToSave(UniqueSave identity, Object state)
        {
            super(identity, state);
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
