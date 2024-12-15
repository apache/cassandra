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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.ToLongFunction;
import java.util.stream.Stream;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.RoutingKey;
import accord.local.Command;
import accord.local.cfk.CommandsForKey;
import accord.local.cfk.Serialize;
import accord.primitives.Routable;
import accord.primitives.SaveStatus;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.utils.IntrusiveLinkedList;
import accord.utils.Invariants;
import accord.utils.QuadFunction;
import accord.utils.TriFunction;
import accord.utils.async.Cancellable;
import org.agrona.collections.Object2ObjectHashMap;
import org.apache.cassandra.cache.CacheSize;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.UnknownTableException;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.metrics.AccordCacheMetrics;
import org.apache.cassandra.metrics.CacheAccessMetrics;
import org.apache.cassandra.service.accord.AccordCacheEntry.Status;
import org.apache.cassandra.service.accord.events.CacheEvents;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.NoSpamLogger.NoSpamLogStatement;
import org.apache.cassandra.utils.ObjectSizes;

import static accord.utils.Invariants.checkState;
import static accord.utils.Invariants.illegalState;
import static org.apache.cassandra.net.MessagingService.current_version;
import static org.apache.cassandra.service.accord.AccordCacheEntry.Status.EVICTED;
import static org.apache.cassandra.service.accord.AccordCacheEntry.Status.LOADED;
import static org.apache.cassandra.service.accord.AccordCacheEntry.Status.MODIFIED;

/**
 * Cache for AccordCommand and AccordCommandsForKey, available memory is shared between the two object types.
 * </p>
 * Supports dynamic object sizes. After each acquire/free cycle, the cacheable objects size is recomputed to
 * account for data added/removed during txn processing if it's modified flag is set
 *
 * TODO (required): we only iterate over unreferenced entries
 */
public class AccordCache implements CacheSize
{
    private static final Logger logger = LoggerFactory.getLogger(AccordCache.class);
    private static final NoSpamLogStatement evictNoEvict = NoSpamLogger.getStatement(logger, "Found and expired {} marked no evict, with age {}, exceeding its expected max age of {}", 1L, TimeUnit.MINUTES);

    // Debug mode to verify that loading from journal + system tables results in
    // functionally identical (or superceding) command to the one we've just evicted.
    private static boolean VALIDATE_LOAD_ON_EVICT = false;

    @VisibleForTesting
    public static void validateLoadOnEvict(boolean value)
    {
        VALIDATE_LOAD_ON_EVICT = value;
    }

    public interface Adapter<K, V, S>
    {
        @Nullable V load(AccordCommandStore commandStore, K key);
        @Nullable Runnable save(AccordCommandStore commandStore, K key, @Nullable V value, @Nullable Object shrunk);
        // a result of null means we can immediately evict, without saving
        @Nullable V quickShrink(V value);
        // a result of null means we cannot shrink, and should save/evict as appropriate
        @Nullable Object fullShrink(K key, V value);
        @Nullable V inflate(K key, Object shrunk);
        long estimateHeapSize(V value);
        long estimateShrunkHeapSize(Object shrunk);
        boolean validate(AccordCommandStore commandStore, K key, V value);
        S safeRef(AccordCacheEntry<K, V> node);

        default AccordCacheEntry<K, V> newEntry(K key, AccordCache.Type<K, V, ?>.Instance owner)
        {
            return AccordCacheEntry.createReadyToLoad(key, owner);
        }
    }

    static class Stats
    {
        long queries;
        long hits;
        long misses;
    }

    public static final class ImmutableStats
    {
        public final long queries;
        public final long hits;
        public final long misses;
        
        public ImmutableStats(Stats stats)
        {
            queries = stats.queries;
            hits = stats.hits;
            misses = stats.misses;
        }
    }

    private final List<Type<?, ?, ?>> types = new CopyOnWriteArrayList<>();
    private final Function<Runnable, Cancellable> saveExecutor;
    private final AccordCacheEntry.OnSaved onSaved;
    // TODO (required): monitor this queue and periodically clean up entries, or implement an eviction deadline system
    private final IntrusiveLinkedList<AccordCacheEntry<?,?>> evictQueue = new IntrusiveLinkedList<>();
    private final IntrusiveLinkedList<AccordCacheEntry<?,?>> noEvictQueue = new IntrusiveLinkedList<>();

    private int unreferencedBytes;
    private int unreferenced;
    private long maxSizeInBytes;
    private long bytesCached;
    private int noEvictGeneration;
    private boolean shrinkingOn = true;

    @VisibleForTesting
    final AccordCacheMetrics metrics;
    final Stats stats = new Stats();

    public AccordCache(Function<Runnable, Cancellable> saveExecutor, AccordCacheEntry.OnSaved onSaved, long maxSizeInBytes, AccordCacheMetrics metrics)
    {
        this.saveExecutor = saveExecutor;
        this.onSaved = onSaved;
        this.maxSizeInBytes = maxSizeInBytes;
        this.metrics = metrics;
    }

    @Override
    public void setCapacity(long sizeInBytes)
    {
        maxSizeInBytes = sizeInBytes;
        maybeShrinkOrEvictSomeNodes();
    }

    public void setShrinkingOn(boolean shrinkingOn)
    {
        this.shrinkingOn = shrinkingOn;
    }

    @Override
    public long capacity()
    {
        return maxSizeInBytes;
    }

    /**
     * Make sure we don't have any items lingering too long in the no evict queue, to avoid cache memory leaks
     */
    void processNoEvictQueue()
    {
        noEvictGeneration = (noEvictGeneration + 1) & 0xffff;
        if (noEvictQueue.isEmpty())
            return;

        Iterator<AccordCacheEntry<?, ?>> iter = noEvictQueue.iterator();
        int skipCount = 3;
        while (skipCount > 0 && iter.hasNext())
        {
            AccordCacheEntry<?, ?> entry = iter.next();
            int age = (noEvictGeneration - entry.noEvictGeneration()) & 0xffff;
            if (age >= entry.noEvictMaxAge())
            {
                evictNoEvict.warn(entry, age, entry.noEvictMaxAge());
                entry.unlink();
                evict(entry, true);
            }
            else
            {
                --skipCount;
            }
        }
    }

    /*
     * Roughly respects LRU semantics when evicting. Might consider prioritising keeping MODIFIED nodes around
     * for longer to maximise the chances of hitting system tables fewer times (or not at all).
     */
    private void maybeShrinkOrEvictSomeNodes()
    {
        while (bytesCached > maxSizeInBytes && !evictQueue.isEmpty())
        {
            AccordCacheEntry<?, ?> node = evictQueue.peek();
            shrinkOrEvict(node);
        }
    }

    @VisibleForTesting
    private <K, V> void shrinkOrEvict(AccordCacheEntry<K, V> node)
    {
        checkState(node.references() == 0);

        if (shrinkingOn && node.tryShrink())
        {
            IntrusiveLinkedList<AccordCacheEntry<?,?>> queue;
            queue = node.isNoEvict() ? noEvictQueue : evictQueue;
            node.unlink();
            queue.addLast(node);
        }
        else
        {
            tryEvict(node);
        }
    }

    @VisibleForTesting
    public <K, V> void tryEvict(AccordCacheEntry<K, V> node)
    {
        checkState(node.references() == 0);

        if (node.isNoEvict())
        {
            node.unlink();
            noEvictQueue.addLast(node);
            return;
        }

        Status status = node.status();
        switch (status)
        {
            default: throw new IllegalStateException("Unhandled status " + status);
            case LOADING:
                node.loading().loading.cancel();
            case WAITING_TO_LOAD:
                Invariants.paranoid(node.loadingOrWaiting().waiters == null);
            case LOADED:
                node.unlink();
                evict(node, true);
                break;
            case MODIFIED:
                Type<K, V, ?> parent = node.owner.parent();
                node.save(saveExecutor, parent.adapter, onSaved);
                boolean evict = node.status() == LOADED;
                node.unlink();
                if (evict) evict(node, true);
        }
    }

    private void evict(AccordCacheEntry<?, ?> node, boolean updateUnreferenced)
    {
        if (logger.isTraceEnabled())
            logger.trace("Evicting {}", node);

        checkState(node.isUnqueued());

        if (updateUnreferenced)
        {
            unreferencedBytes -= node.sizeOnHeap;
            --unreferenced;
        }
        bytesCached -= node.sizeOnHeap;
        Type<?, ?, ?>.Instance owner = node.owner;
        Type<?, ?, ?> parent = owner.parent();
        parent.bytesCached -= node.sizeOnHeap;
        --parent.size;

        // TODO (expected): use listeners
        if (node.status() == LOADED && VALIDATE_LOAD_ON_EVICT)
            owner.validateLoadEvicted(node);

        AccordCacheEntry<?, ?> self = node.owner.cache.remove(node.key());
        Invariants.checkState(self.references() == 0);
        checkState(self == node, "Leaked node detected; was attempting to remove %s but cache had %s", node, self);
        node.notifyListeners(Listener::onEvict);
        node.evicted();
    }

    <P, K, V> Collection<AccordTask<?>> load(BiFunction<P, Runnable, Cancellable> loadExecutor, P param, AccordCacheEntry<K, V> node, AccordCacheEntry.OnLoaded onLoaded)
    {
        Type<K, V, ?> parent = node.owner.parent();
        return node.load(loadExecutor, param, parent.adapter, onLoaded).waiters();
    }

    <K, V> void loaded(AccordCacheEntry<K, V> node, V value)
    {
        node.loaded(value);
        node.notifyListeners(Listener::onUpdate);
    }

    <K, V> void failedToLoad(AccordCacheEntry<K, V> node)
    {
        Invariants.checkState(node.references() == 0);
        if (node.isUnqueued())
        {
            Invariants.checkState(node.status() == EVICTED);
            return;
        }
        node.unlink();
        node.failedToLoad();
        evict(node, true);
    }

    <K, V> void saved(AccordCacheEntry<K, V> node, Object identity, Throwable fail)
    {
        if (node.saved(identity, fail) && node.references() == 0)
            evictQueue.addFirst(node); // add to front since we have just saved, so we were eligible for eviction
    }

    public <K, V, S extends AccordSafeState<K, V>> void release(S safeRef, AccordTask<?> owner)
    {
        safeRef.global().owner.release(safeRef, owner);
    }

    public ImmutableStats stats()
    {
        return new ImmutableStats(stats);
    }

    public <K, V, S extends AccordSafeState<K, V>> Type<K, V, S> newType(Class<K> keyClass, Adapter<K, V, S> adapter)
    {
        Type<K, V, S> instance = new Type<>(keyClass, adapter);
        types.add(instance);
        return instance;
    }

    public <K, V, S extends AccordSafeState<K, V>> Type<K, V, S> newType(
        Class<K> keyClass,
        BiFunction<AccordCommandStore, K, V> loadFunction,
        QuadFunction<AccordCommandStore, K, V, Object, Runnable> saveFunction,
        Function<V, V> quickShrink,
        TriFunction<AccordCommandStore, K, V, Boolean> validateFunction,
        ToLongFunction<V> heapEstimator,
        Function<AccordCacheEntry<K, V>, S> safeRefFactory)
    {
        return newType(keyClass, loadFunction, saveFunction, quickShrink, (i, j) -> j, (i, j) -> (V)j, validateFunction, heapEstimator, i -> 0, safeRefFactory);
    }

    public <K, V, S extends AccordSafeState<K, V>> Type<K, V, S> newType(
        Class<K> keyClass,
        BiFunction<AccordCommandStore, K, V> loadFunction,
        QuadFunction<AccordCommandStore, K, V, Object, Runnable> saveFunction,
        Function<V, V> quickShrink,
        BiFunction<K, V, Object> fullShrink,
        BiFunction<K, Object, V> inflate,
        TriFunction<AccordCommandStore, K, V, Boolean> validateFunction,
        ToLongFunction<V> heapEstimator,
        ToLongFunction<Object> shrunkHeapEstimator,
        Function<AccordCacheEntry<K, V>, S> safeRefFactory)
    {
        return newType(keyClass, new FunctionalAdapter<>(loadFunction, saveFunction, quickShrink,
                                                         fullShrink, inflate,
                                                         validateFunction, heapEstimator, shrunkHeapEstimator,
                                                         safeRefFactory, AccordCacheEntry::createReadyToLoad));
    }

    public Collection<Type<?, ? ,? >> types()
    {
        return types;
    }

    public interface Listener<K, V>
    {
        default void onAdd(AccordCacheEntry<K, V> state) {}
        default void onUpdate(AccordCacheEntry<K, V> state) {}
        default void onEvict(AccordCacheEntry<K, V> state) {}
    }

    public class Type<K, V, S extends AccordSafeState<K, V>> implements CacheSize
    {
        public class Instance implements Iterable<AccordCacheEntry<K, V>>
        {
            final AccordCommandStore commandStore;
            // TODO (desired): don't need to store key separately as stored in node; ideally use a hash set that allows us to get the current entry
            private final Map<K, AccordCacheEntry<K, V>> cache = new Object2ObjectHashMap<>();
            private List<Listener<K, V>> listeners = null;

            public Instance(AccordCommandStore commandStore)
            {
                this.commandStore = commandStore;
            }

            public S acquire(K key)
            {
                AccordCacheEntry<K, V> node = acquire(key, false);
                return adapter.safeRef(node);
            }

            public S acquireIfLoaded(K key)
            {
                AccordCacheEntry<K, V> node = acquire(key, true);
                if (node == null)
                    return null;
                return adapter.safeRef(node);
            }

            public S acquire(AccordCacheEntry<K, V> node)
            {
                Invariants.checkState(node.owner == this);
                acquireExisting(node, false);
                return adapter.safeRef(node);
            }

            public void recordPreAcquired(AccordSafeState<K, V> ref)
            {
                Invariants.checkState(ref.global().owner == this);
                incrementCacheHits();
            }

            private AccordCacheEntry<K, V> acquire(K key, boolean onlyIfLoaded)
            {
                incrementCacheQueries();
                @SuppressWarnings("unchecked")
                AccordCacheEntry<K, V> node = cache.get(key);
                return node == null
                       ? acquireAbsent(key, onlyIfLoaded)
                       : acquireExisting(node, onlyIfLoaded);
            }

            /*
             * Can only return a LOADING Node (or null)
             */
            private AccordCacheEntry<K, V> acquireAbsent(K key, boolean onlyIfLoaded)
            {
                incrementCacheMisses();
                if (onlyIfLoaded)
                    return null;
                AccordCacheEntry<K, V> node = adapter.newEntry(key, this);
                node.increment();

                Object prev = cache.put(key, node);
                node.initSize(parent());
                Invariants.checkState(prev == null, "%s not absent from cache: %s already present", key, node);
                ++size;
                node.notifyListeners(Listener::onAdd);
                maybeShrinkOrEvictSomeNodes();
                return node;
            }

            /*
             * Can't return EVICTED or INITIALIZED
             */
            private AccordCacheEntry<K, V> acquireExisting(AccordCacheEntry<K, V> node, boolean onlyIfLoaded)
            {
                boolean isLoaded = node.isLoaded();
                if (isLoaded)
                    incrementCacheHits();
                else
                    incrementCacheMisses();

                if (onlyIfLoaded && !isLoaded)
                    return null;

                if (node.increment() == 1)
                {
                    --unreferenced;
                    unreferencedBytes -= node.sizeOnHeap;
                    node.unlink();
                }

                return node;
            }

            public void release(AccordSafeState<K, V> safeRef, AccordTask<?> owner)
            {
                K key = safeRef.global().key();
                logger.trace("Releasing resources for {}: {}", key, safeRef);

                AccordCacheEntry<K, V> node = cache.get(key);

                checkState(!safeRef.invalidated());
                checkState(safeRef.global() != null, "safeRef node is null for %s", key);
                checkState(safeRef.global() == node, "safeRef node not in map: %s != %s", safeRef.global(), node);
                checkState(node.references() > 0, "references (%d) are zero for %s (%s)", node.references(), key, node);
                checkState(node.isUnqueued());

                boolean evict = false;
                if (safeRef.hasUpdate())
                {
                    V update = safeRef.current();
                    if (update != null)
                        update = adapter.quickShrink(update);
                    node.setExclusive(update);
                    if (update == null)
                    {
                        if (node.is(MODIFIED))
                            node.saved();
                        evict = true;
                    }
                    node.notifyListeners(Listener::onUpdate);
                }
                else if (node.isLoadingOrWaiting())
                {
                    node.loadingOrWaiting().remove(owner);
                }
                else
                {
                    evict = node.is(LOADED) && node.isNull();
                }
                safeRef.invalidate();

                if (node.decrement() == 0)
                {
                    if (evict)
                    {
                        evict(node, false);
                        return;
                    }

                    ++unreferenced;
                    unreferencedBytes += node.sizeOnHeap;
                    Status status = node.status(); // status() completes
                    switch (status)
                    {
                        default: throw new IllegalStateException("Unhandled status " + status);
                        case WAITING_TO_LOAD:
                        case LOADING:
                        case LOADED:
                        case MODIFIED:
                            logger.trace("Moving {} with status {} to eviction queue", key, status);
                            evictQueue.addLast(node);

                        case SAVING:
                        case FAILED_TO_SAVE:
                            break; // can never evict, so no point in adding to eviction queue either
                    }
                }

                maybeShrinkOrEvictSomeNodes();
            }

            public Stream<AccordCacheEntry<K, V>> stream()
            {
                return cache.values().stream();
            }

            Type<K, V, S> parent()
            {
                return Type.this;
            }

            @Override
            public Iterator<AccordCacheEntry<K, V>> iterator()
            {
                return stream().iterator();
            }

            void validateLoadEvicted(AccordCacheEntry<?, ?> node)
            {
                @SuppressWarnings("unchecked")
                AccordCacheEntry<K, V> state = (AccordCacheEntry<K, V>) node;
                K key = state.key();
                V evicted = state.tryGetFull();
                if (evicted == null)
                {
                    try
                    {
                        Object shrunk = state.tryGetShrunk();
                        if (shrunk != null)
                            evicted = adapter.inflate(key, shrunk);
                    }
                    catch (RuntimeException rte)
                    {
                        if (rte.getCause() instanceof UnknownTableException)
                            return;
                        throw rte;
                    }
                }
                if (!adapter.validate(node.owner.commandStore, key, evicted))
                    throw new IllegalStateException("Reloaded value for key " + key + " is not equal to or fuller than evicted value " + evicted);
            }

            @VisibleForTesting
            public AccordCacheEntry<K, V> getUnsafe(K key)
            {
                return cache.get(key);
            }

            public Set<K> keySet()
            {
                return cache.keySet();
            }

            @VisibleForTesting
            public boolean isReferenced(K key)
            {
                AccordCacheEntry<K, V> node = cache.get(key);
                return node != null && node.references() > 0;
            }

            @VisibleForTesting
            boolean keyIsReferenced(Object key, Class<? extends AccordSafeState<?, ?>> valClass)
            {
                AccordCacheEntry<?, ?> node = cache.get(key);
                return node != null && node.references() > 0;
            }

            @VisibleForTesting
            boolean keyIsCached(Object key, Class<? extends AccordSafeState<?, ?>> valClass)
            {
                AccordCacheEntry<?, ?> node = cache.get(key);
                return node != null;
            }

            @VisibleForTesting
            int references(Object key, Class<? extends AccordSafeState<?, ?>> valClass)
            {
                AccordCacheEntry<?, ?> node = cache.get(key);
                return node != null ? node.references() : 0;
            }

            void notifyListeners(BiConsumer<Listener<K, V>, AccordCacheEntry<K, V>> notify, AccordCacheEntry<K, V> node)
            {
                notifyListeners(listeners, notify, node);
                notifyListeners(typeListeners, notify, node);
            }

            void notifyListeners(List<Listener<K, V>> listeners, BiConsumer<Listener<K, V>, AccordCacheEntry<K, V>> notify, AccordCacheEntry<K, V> node)
            {
                if (listeners != null)
                {
                    for (int i = 0, size = listeners.size() ; i < size ; ++i)
                        notify.accept(listeners.get(i), node);

                }
            }

            public void register(Listener<K, V> l)
            {
                if (listeners == null)
                    listeners = new ArrayList<>();
                listeners.add(l);
            }

            public void unregister(Listener<K, V> l)
            {
                if (!tryUnregister(l))
                    throw illegalState("Listener was not registered");
            }

            public boolean tryUnregister(Listener<K, V> l)
            {
                if (listeners == null || !listeners.remove(l))
                    return false;
                if (listeners.isEmpty())
                    listeners = null;
                return true;
            }

        }

        private final Class<K> keyClass;
        private Adapter<K, V, S> adapter;
        private long bytesCached;
        private int size;

        @VisibleForTesting
        final CacheAccessMetrics typeMetrics;
        private final Stats stats = new Stats();
        private List<Listener<K, V>> typeListeners = null;

        public Type(
            Class<K> keyClass,
            Adapter<K, V, S> adapter)
        {
            this.keyClass = keyClass;
            this.adapter = adapter;
            this.typeMetrics = metrics.forInstance(keyClass);
        }

        void updateSize(long newSize, long delta, boolean isUnreferenced, boolean updateHistogram)
        {
            // TODO (expected): deprecate this in favour of a histogram snapshot of any point in time
            bytesCached += delta;
            AccordCache.this.bytesCached += delta;
            if (updateHistogram) metrics.objectSize.update(newSize);
            if (isUnreferenced) AccordCache.this.unreferencedBytes += delta;
        }

        // can be safely garbage collected if empty
        Instance newInstance(AccordCommandStore commandStore)
        {
            return new Instance(commandStore);
        }

        private void incrementCacheQueries()
        {
            typeMetrics.requests.mark();
            metrics.requests.mark();
            stats.queries++;
            AccordCache.this.stats.queries++;
        }

        private void incrementCacheHits()
        {
            typeMetrics.hits.mark();
            metrics.hits.mark();
            stats.hits++;
            AccordCache.this.stats.hits++;
        }

        private void incrementCacheMisses()
        {
            typeMetrics.misses.mark();
            metrics.misses.mark();
            stats.misses++;
            AccordCache.this.stats.misses++;
        }

        AccordCache parent()
        {
            return AccordCache.this;
        }

        public Stats stats()
        {
            return stats;
        }

        public ImmutableStats statsSnapshot()
        {
            return new ImmutableStats(stats);
        }

        public Stats globalStats()
        {
            return AccordCache.this.stats;
        }

        @VisibleForTesting
        public void unsafeSetLoadFunction(BiFunction<AccordCommandStore, K, V> loadFunction)
        {
            if (adapter.getClass() != SettableWrapper.class)
                adapter = new SettableWrapper<>(adapter);
            ((SettableWrapper<K, V, S>)adapter).load = loadFunction;
        }

        public BiFunction<AccordCommandStore, K, V> unsafeGetLoadFunction()
        {
            if (adapter.getClass() != SettableWrapper.class)
                adapter = new SettableWrapper<>(adapter);
            return ((SettableWrapper<K, V, S>)adapter).load;
        }

        Adapter<K, V, S> adapter()
        {
            return adapter;
        }

        @Override
        public long capacity()
        {
            return AccordCache.this.capacity();
        }

        @Override
        public void setCapacity(long capacity)
        {
            throw new UnsupportedOperationException("Capacity is shared between all instances. Please set the capacity on the global cache");
        }

        @Override
        public int size()
        {
            return size;
        }

        @Override
        public long weightedSize()
        {
            return bytesCached;
        }

        public long globalAllocated()
        {
            return AccordCache.this.bytesCached;
        }

        public int globalReferencedEntries()
        {
            return AccordCache.this.numReferencedEntries();
        }

        public int globalUnreferencedEntries()
        {
            return AccordCache.this.numUnreferencedEntries();
        }

        public void register(Listener<K, V> l)
        {
            if (typeListeners == null)
                typeListeners = new ArrayList<>();
            typeListeners.add(l);
        }

        public void unregister(Listener<K, V> l)
        {
            if (typeListeners == null)
                throw new AssertionError("No listeners exist");
            if (!typeListeners.remove(l))
                throw new AssertionError("Listener was not registered");
            if (typeListeners.isEmpty())
                typeListeners = null;
        }

        @Override
        public String toString()
        {
            return "Instance{" +
                   ", keyClass=" + keyClass +
                   '}';
        }
    }

    @VisibleForTesting
    AccordCacheEntry<?, ?> head()
    {
        Iterator<AccordCacheEntry<?, ?>> iter = evictQueue.iterator();
        return iter.hasNext() ? iter.next() : null;
    }

    @VisibleForTesting
    AccordCacheEntry<?, ?> tail()
    {
        AccordCacheEntry<?,?> last = null;
        Iterator<AccordCacheEntry<?, ?>> iter = evictQueue.iterator();
        while (iter.hasNext())
            last = iter.next();
        return last;
    }

    public boolean isEmpty()
    {
        return size() == 0;
    }

    Iterable<AccordCacheEntry<?, ?>> evictionQueue()
    {
        return evictQueue::iterator;
    }

    private int cacheSize()
    {
        int size = 0;
        for (Type<?, ?, ?> type : types)
            size += type.size();
        return size;
    }

    @VisibleForTesting
    int numReferencedEntries()
    {
        return cacheSize() - unreferenced;
    }

    @VisibleForTesting
    int numUnreferencedEntries()
    {
        return unreferenced;
    }

    @VisibleForTesting
    int unreferencedBytes()
    {
        return unreferencedBytes;
    }

    @Override
    public int size()
    {
        return cacheSize();
    }

    @Override
    public long weightedSize()
    {
        return bytesCached;
    }

    static <K, V> void registerJfrListener(int shardId, AccordCache.Type<K, V, ?> type, String name)
    {
        if (!DatabaseDescriptor.getAccordStateCacheListenerJFREnabled())
            return;

        type.register(new AccordCache.Listener<>() {
            private final IdentityHashMap<AccordCacheEntry<?, ?>, CacheEvents.Evict> pendingEvicts = new IdentityHashMap<>();

            @Override
            public void onAdd(AccordCacheEntry<K, V> state)
            {
                CacheEvents.Add add = new CacheEvents.Add();
                CacheEvents.Evict evict = new CacheEvents.Evict();
                if (!add.isEnabled())
                    return;
                add.begin();
                evict.begin();
                add.shard = evict.shard = shardId;
                add.instance = evict.instance = name;
                add.key = evict.key = state.key().toString();
                updateMutable(type, state, add);
                add.commit();
                pendingEvicts.put(state, evict);
            }

            @Override
            public void onEvict(AccordCacheEntry<K, V> state)
            {
                CacheEvents.Evict event = pendingEvicts.remove(state);
                if (event == null) return;
                updateMutable(type, state, event);
                event.commit();
            }
        });
    }

    private static void updateMutable(AccordCache.Type<?, ?, ?> type, AccordCacheEntry<?, ?> state, CacheEvents event)
    {
        event.status = state.status().name();

        event.lastQueriedEstimatedSizeOnHeap = state.sizeOnHeap();

        event.instanceAllocated = type.weightedSize();
        AccordCache.Stats stats = type.stats();
        event.instanceStatsQueries = stats.queries;
        event.instanceStatsHits = stats.hits;
        event.instanceStatsMisses = stats.misses;

        event.globalSize = type.size();
        event.globalReferenced = type.globalReferencedEntries();
        event.globalUnreferenced = type.globalUnreferencedEntries();
        event.globalCapacity = type.capacity();
        event.globalAllocated = type.globalAllocated();

        stats = type.globalStats();
        event.globalStatsQueries = stats.queries;
        event.globalStatsHits = stats.hits;
        event.globalStatsMisses = stats.misses;

        event.update();
    }

    static class FunctionalAdapter<K, V, S> implements Adapter<K, V, S>
    {
        final BiFunction<AccordCommandStore, K, V> load;
        final QuadFunction<AccordCommandStore, K, V, Object, Runnable> save;
        final Function<V, V> quickShrink;
        final BiFunction<K, V, Object> shrink;
        final BiFunction<K, Object, V> inflate;
        final TriFunction<AccordCommandStore, K, V, Boolean> validate;
        final ToLongFunction<V> estimateHeapSize;
        final ToLongFunction<Object> estimateShrunkHeapSize;
        final Function<AccordCacheEntry<K, V>, S> newSafeRef;
        final BiFunction<K, AccordCache.Type<K, V, ?>.Instance, AccordCacheEntry<K, V>> newNode;

        FunctionalAdapter(BiFunction<AccordCommandStore, K, V> load,
                          QuadFunction<AccordCommandStore, K, V, Object, Runnable> save,
                          Function<V, V> quickShrink, BiFunction<K, V, Object> shrink,
                          BiFunction<K, Object, V> inflate,
                          TriFunction<AccordCommandStore, K, V, Boolean> validate,
                          ToLongFunction<V> estimateHeapSize,
                          ToLongFunction<Object> estimateShrunkHeapSize,
                          Function<AccordCacheEntry<K, V>, S> newSafeRef,
                          BiFunction<K, Type<K, V, ?>.Instance, AccordCacheEntry<K, V>> newNode)
        {
            this.load = load;
            this.save = save;
            this.shrink = shrink;
            this.quickShrink = quickShrink;
            this.inflate = inflate;
            this.validate = validate;
            this.estimateHeapSize = estimateHeapSize;
            this.estimateShrunkHeapSize = estimateShrunkHeapSize;
            this.newSafeRef = newSafeRef;
            this.newNode = newNode;
        }

        FunctionalAdapter(Adapter<K, V, S> wrap)
        {
            this(wrap::load, wrap::save, wrap::quickShrink, wrap::fullShrink, wrap::inflate, wrap::validate, wrap::estimateHeapSize, wrap::estimateShrunkHeapSize, wrap::safeRef, wrap::newEntry);
        }

        @Override
        public V load(AccordCommandStore commandStore, K key)
        {
            return load.apply(commandStore, key);
        }

        @Override
        public Runnable save(AccordCommandStore commandStore, K key, @Nullable V value, @Nullable Object shrunk)
        {
            return save.apply(commandStore, key, value, shrunk);
        }

        @Override
        public V quickShrink(V value)
        {
            return quickShrink.apply(value);
        }

        @Override
        public Object fullShrink(K key, V value)
        {
            return shrink.apply(key, value);
        }

        @Override
        public V inflate(K key, Object shrunk)
        {
            return inflate.apply(key, shrunk);
        }

        @Override
        public boolean validate(AccordCommandStore commandStore, K key, V value)
        {
            return validate.apply(commandStore, key, value);
        }

        @Override
        public long estimateHeapSize(V value)
        {
            return estimateHeapSize.applyAsLong(value);
        }

        @Override
        public long estimateShrunkHeapSize(Object shrunk)
        {
            return estimateShrunkHeapSize.applyAsLong(shrunk);
        }

        @Override
        public S safeRef(AccordCacheEntry<K, V> node)
        {
            return newSafeRef.apply(node);
        }

        @Override
        public AccordCacheEntry<K, V> newEntry(K key, Type<K, V, ?>.Instance owner)
        {
            return newNode.apply(key, owner);
        }
    }

    static class SettableWrapper<K, V, S> extends FunctionalAdapter<K, V, S>
    {
        volatile BiFunction<AccordCommandStore, K, V> load;

        SettableWrapper(Adapter<K, V, S> wrapper)
        {
            super(wrapper);
            this.load = super.load;
        }

        public static <K, V> Adapter<K, V, ?> loadOnly(BiFunction<AccordCommandStore, K, V> load)
        {
            SettableWrapper<K, V, ?> result = new SettableWrapper<>(new NoOpAdapter<>());
            result.load = load;
            return result;
        }

        @Override
        public V load(AccordCommandStore commandStore, K key)
        {
            return load.apply(commandStore, key);
        }
    }

    static class NoOpAdapter<K, V, S> implements Adapter<K, V, S>
    {
        @Override public V load(AccordCommandStore commandStore, K key) { return null; }
        @Override public Runnable save(AccordCommandStore commandStore, K key, @Nullable V value, @Nullable Object shrunk) { return null; }
        @Override public V quickShrink(V value) { return null; }
        @Override public Object fullShrink(K key, V value) { return null; }
        @Override public V inflate(K key, Object shrunk) { return null; }
        @Override public long estimateHeapSize(V value) { return 0; }
        @Override public long estimateShrunkHeapSize(Object shrunk) { return 0; }
        @Override public boolean validate(AccordCommandStore commandStore, K key, V value) { return false; }
        @Override public S safeRef(AccordCacheEntry<K, V> node) { return null; }
    }

    public static class CommandsForKeyAdapter implements Adapter<RoutingKey, CommandsForKey, AccordSafeCommandsForKey>
    {
        public static final CommandsForKeyAdapter CFK_ADAPTER = new CommandsForKeyAdapter();
        private CommandsForKeyAdapter() {}

        @Override
        public CommandsForKey load(AccordCommandStore commandStore, RoutingKey key)
        {
            return commandStore.loadCommandsForKey(key);
        }

        @Override
        public Runnable save(AccordCommandStore commandStore, RoutingKey key, @Nullable CommandsForKey value, @Nullable Object serialized)
        {
            return commandStore.saveCommandsForKey(key, value, serialized);
        }

        @Override
        public CommandsForKey quickShrink(CommandsForKey value)
        {
            return value;
        }

        @Override
        public Object fullShrink(RoutingKey key, CommandsForKey value)
        {
            if (value.isEmpty())
                return null;

            return Serialize.toBytesWithoutKey(value.maximalPrune());
        }

        @Override
        public CommandsForKey inflate(RoutingKey key, Object shrunk)
        {
            return Serialize.fromBytes(key, (ByteBuffer)shrunk);
        }

        @Override
        public long estimateHeapSize(CommandsForKey value)
        {
            return AccordObjectSizes.commandsForKey(value);
        }

        @Override
        public long estimateShrunkHeapSize(Object shrunk)
        {
            return ObjectSizes.sizeOnHeapOf((ByteBuffer) shrunk);
        }

        @Override
        public boolean validate(AccordCommandStore commandStore, RoutingKey key, CommandsForKey value)
        {
            return commandStore.validateCommandsForKey(key, value);
        }

        @Override
        public AccordSafeCommandsForKey safeRef(AccordCacheEntry<RoutingKey, CommandsForKey> node)
        {
            return new AccordSafeCommandsForKey(node);
        }
    }

    public static class CommandAdapter implements Adapter<TxnId, Command, AccordSafeCommand>
    {
        public static final CommandAdapter COMMAND_ADAPTER = new CommandAdapter();
        private CommandAdapter() {}

        @Override
        public Command load(AccordCommandStore commandStore, TxnId txnId)
        {
            Invariants.checkState(!txnId.is(Txn.Kind.EphemeralRead));
            return commandStore.loadCommand(txnId);
        }

        @Override
        public Runnable save(AccordCommandStore commandStore, TxnId txnId, @Nullable Command value, @Nullable Object serialized)
        {
            if (txnId.is(Routable.Domain.Key))
                return null;

            if (value == null)
            {
                value = inflate(txnId, serialized);
                if (value == null)
                    return null;
            }

            return commandStore.appendToKeyspace(txnId, value);
        }

        @Override
        public Command quickShrink(Command value)
        {
            if (value.saveStatus() == SaveStatus.Uninitialised)
                return null;
            if (value.txnId().is(Txn.Kind.EphemeralRead) && value.saveStatus().compareTo(SaveStatus.ReadyToExecute) >= 0)
                return null; // TODO (expected): should we manage this with the waiting callback? more work, but maybe cleaner/clearer/safer
            return AccordCommandStore.prepareToCache(value);
        }

        @Override
        public Object fullShrink(TxnId txnId, Command value)
        {
            if (txnId.is(Txn.Kind.EphemeralRead))
                Invariants.checkState(value.saveStatus().compareTo(SaveStatus.ReadyToExecute) < 0);

            try
            {
                return SavedCommand.asSerializedDiff(null, value, current_version);
            }
            catch (IOException e)
            {
                logger.warn("Failed to serialize {}", value, e);
                return null;
            }
        }

        @Override
        public @Nullable Command inflate(TxnId key, Object serialized)
        {
            SavedCommand.Builder builder = new SavedCommand.Builder(key);
            ByteBuffer buffer = (ByteBuffer) serialized;
            buffer.mark();
            try (DataInputBuffer buf = new DataInputBuffer(buffer, false))
            {
                builder.deserializeNext(buf, current_version);
                return builder.construct();
            }
            catch (UnknownTableException e)
            {
                // TODO (required): log, and make sure callers correctly handle null
                return null;
            }
            catch (IOException e)
            {
                // TODO (required): test and make sure recover safely from exceptions OR log and return null
                throw new RuntimeException(e);
            }
            finally
            {
                buffer.reset();
            }
        }

        @Override
        public long estimateHeapSize(Command value)
        {
            return AccordObjectSizes.command(value);
        }

        @Override
        public long estimateShrunkHeapSize(Object shrunk)
        {
            return ObjectSizes.sizeOnHeapOf((ByteBuffer) shrunk);
        }

        @Override
        public boolean validate(AccordCommandStore commandStore, TxnId key, Command value)
        {
            return commandStore.validateCommand(key, value);
        }

        @Override
        public AccordSafeCommand safeRef(AccordCacheEntry<TxnId, Command> node)
        {
            return new AccordSafeCommand(node);
        }

        @Override
        public AccordCacheEntry<TxnId, Command> newEntry(TxnId txnId, Type<TxnId, Command, ?>.Instance owner)
        {
            AccordCacheEntry<TxnId, Command> node = new AccordCacheEntry<>(txnId, owner);
            if (txnId.is(Txn.Kind.EphemeralRead))
            {
                node.initialize(null);
                int maxAge = (int)Math.min(0xff, 1 + DatabaseDescriptor.getReadRpcTimeout(TimeUnit.SECONDS));
                node.markNoEvict(owner.parent().parent().noEvictGeneration, maxAge);
            }
            else
            {
                node.readyToLoad();
            }
            return node;
        }
    }
}
