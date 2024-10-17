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
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.ToLongFunction;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.utils.IntrusiveLinkedList;
import accord.utils.Invariants;
import accord.utils.TriFunction;
import org.agrona.collections.Object2ObjectHashMap;
import org.apache.cassandra.cache.CacheSize;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.metrics.AccordStateCacheMetrics;
import org.apache.cassandra.metrics.CacheAccessMetrics;
import org.apache.cassandra.service.accord.AccordCachingState.Status;
import org.apache.cassandra.service.accord.events.CacheEvents;
import org.apache.cassandra.utils.concurrent.Future;

import static accord.utils.Invariants.checkState;
import static org.apache.cassandra.service.accord.AccordCachingState.Status.EVICTED;
import static org.apache.cassandra.service.accord.AccordCachingState.Status.LOADED;
import static org.apache.cassandra.service.accord.AccordCachingState.Status.SAVING;

/**
 * Cache for AccordCommand and AccordCommandsForKey, available memory is shared between the two object types.
 * </p>
 * Supports dynamic object sizes. After each acquire/free cycle, the cacheable objects size is recomputed to
 * account for data added/removed during txn processing if it's modified flag is set
 */
public class AccordStateCache extends IntrusiveLinkedList<AccordCachingState<?,?>> implements CacheSize
{
    private static final Logger logger = LoggerFactory.getLogger(AccordStateCache.class);

    // Debug mode to verify that loading from journal + system tables results in
    // functionally identical (or superceding) command to the one we've just evicted.
    private static boolean VALIDATE_LOAD_ON_EVICT = false;

    @VisibleForTesting
    public static void validateLoadOnEvict(boolean value)
    {
        VALIDATE_LOAD_ON_EVICT = value;
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
    private final Function<Runnable, Future<?>> saveExecutor;
    private final AccordCachingState.OnSaved onSaved;

    private int unreferenced = 0;
    private long maxSizeInBytes;
    private long bytesCached = 0;

    @VisibleForTesting
    final AccordStateCacheMetrics metrics;
    final Stats stats = new Stats();

    public AccordStateCache(Function<Runnable, Future<?>> saveExecutor, AccordCachingState.OnSaved onSaved, long maxSizeInBytes, AccordStateCacheMetrics metrics)
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
        maybeEvictSomeNodes();
    }

    @Override
    public long capacity()
    {
        return maxSizeInBytes;
    }

    @SuppressWarnings("unchecked")
    private <K, V> void maybeUpdateSize(AccordCachingState<?, ?> node, ToLongFunction<?> estimator)
    {
        if (node.shouldUpdateSize())
        {
            long delta = ((AccordCachingState<K, V>) node).estimatedSizeOnHeapDelta((ToLongFunction<V>) estimator);
            bytesCached += delta;
            node.owner.parent().bytesCached += delta;
        }
    }

    /*
     * Roughly respects LRU semantics when evicting. Might consider prioritising keeping MODIFIED nodes around
     * for longer to maximise the chances of hitting system tables fewer times (or not at all).
     */
    private void maybeEvictSomeNodes()
    {
        if (bytesCached <= maxSizeInBytes)
            return;

        Iterator<AccordCachingState<?, ?>> iter = this.iterator();
        while (iter.hasNext() && bytesCached > maxSizeInBytes)
        {
            AccordCachingState<?, ?> node = iter.next();
            maybeEvict(node);
        }
    }

    @VisibleForTesting
    public <K, V> boolean maybeEvict(AccordCachingState<K, V> node)
    {
        checkState(node.references == 0);

        Status status = node.status();
        switch (status)
        {
            default: throw new IllegalStateException("Unhandled status " + status);
            case LOADING:
                node.loading().loading.cancel(false);
            case WAITING_TO_LOAD:
                Invariants.checkState(node.loadingOrWaiting().waiters == null);
            case LOADED:
                node.unlink();
                evict(node);
                return true;
            case MODIFIED:
                Type<K, V, ?> parent = node.owner.parent();
                node.save(saveExecutor, parent.saveFunction, onSaved);
                boolean evict = node.status() == LOADED;
                node.unlink();
                if (evict) evict(node);
                return evict;
        }
    }

    private void evict(AccordCachingState<?, ?> node)
    {
        if (logger.isTraceEnabled())
            logger.trace("Evicting {} {} - {}", node.status(), node.key(), node.isLoaded() ? node.get() : null);

        checkState(node.isUnqueued());

        bytesCached -= node.lastQueriedEstimatedSizeOnHeap;
        Type<?, ?, ?>.Instance owner = node.owner;
        Type<?, ?, ?> parent = owner.parent();
        parent.bytesCached -= node.lastQueriedEstimatedSizeOnHeap;
        --parent.size;

        // TODO (expected): use listeners
        if (node.status() == LOADED && VALIDATE_LOAD_ON_EVICT)
            owner.validateLoadEvicted(node);

        AccordCachingState<?, ?> self = node.owner.cache.remove(node.key());
        Invariants.checkState(self.references == 0);
        checkState(self == node, "Leaked node detected; was attempting to remove %s but cache had %s", node, self);
        node.notifyListeners(Listener::onEvict);
        node.evicted();
    }

    <K, V> Collection<AccordTask<?>> load(Function<Runnable, Future<?>> loadExecutor, AccordCachingState<K, V> node, AccordCachingState.OnLoaded onLoaded)
    {
        Type<K, V, ?> parent = node.owner.parent();
        return node.load(loadExecutor, parent.loadFunction, onLoaded).waiters();
    }

    <K, V> void loaded(AccordCachingState<K, V> node, V value)
    {
        node.loaded(value);
        node.notifyListeners(Listener::onUpdate);
    }

    <K, V> void failedToLoad(AccordCachingState<K, V> node)
    {
        Invariants.checkState(node.references == 0);
        if (node.isUnqueued())
        {
            Invariants.checkState(node.status() == EVICTED);
            return;
        }
        node.unlink();
        node.failedToLoad();
        evict(node);
    }

    <K, V> void saved(AccordCachingState<K, V> node, Object identity, Throwable fail)
    {
        if (node.saved(identity, fail) && node.referenceCount() == 0)
            addFirst(node); // add to front since we have just saved, so we were eligible for eviction
    }

    public <K, V, S extends AccordSafeState<K, V>> void release(S safeRef, AccordTask<?> owner)
    {
        safeRef.global().owner.release(safeRef, owner);
    }

    public ImmutableStats stats()
    {
        return new ImmutableStats(stats);
    }

    public <K, V, S extends AccordSafeState<K, V>> Type<K, V, S> newType(
        Class<K> keyClass,
        Class<S> valClass,
        Function<AccordCachingState<K, V>, S> safeRefFactory,
        BiFunction<AccordCommandStore, K, V> loadFunction,
        BiFunction<AccordCommandStore, V, Runnable> saveFunction,
        TriFunction<AccordCommandStore, K, V, Boolean> validateFunction,
        ToLongFunction<V> heapEstimator,
        AccordCachingState.Factory<K, V> nodeFactory)
    {
        Type<K, V, S> instance =
            new Type<>(keyClass, safeRefFactory, loadFunction, saveFunction, validateFunction, heapEstimator, nodeFactory);

        types.add(instance);

        return instance;
    }

    public <K, V, S extends AccordSafeState<K, V>> Type<K, V, S> newType(
        Class<K> keyClass,
        Class<S> valClass,
        Function<AccordCachingState<K, V>, S> safeRefFactory,
        BiFunction<AccordCommandStore, K, V> loadFunction,
        BiFunction<AccordCommandStore, V, Runnable> saveFunction,
        TriFunction<AccordCommandStore, K, V, Boolean> validateFunction,
        ToLongFunction<V> heapEstimator)
    {
        return newType(keyClass, valClass, safeRefFactory, loadFunction, saveFunction, validateFunction, heapEstimator, AccordCachingState.defaultFactory());
    }

    public Collection<Type<?, ? ,? >> types()
    {
        return types;
    }

    public interface Listener<K, V>
    {
        default void onAdd(AccordCachingState<K, V> state) {}
        default void onUpdate(AccordCachingState<K, V> state) {}
        default void onRelease(AccordCachingState<K, V> state) {}
        default void onEvict(AccordCachingState<K, V> state) {}
    }

    public class Type<K, V, S extends AccordSafeState<K, V>> implements CacheSize
    {
        public class Instance implements Iterable<AccordCachingState<K, V>>
        {
            final AccordCommandStore commandStore;
            // TODO (desired): don't need to store key separately as stored in node; ideally use a hash set that allows us to get the current entry
            private final Map<K, AccordCachingState<K, V>> cache = new Object2ObjectHashMap<>();
            private List<Listener<K, V>> listeners = null;

            public Instance(AccordCommandStore commandStore)
            {
                this.commandStore = commandStore;
            }

            public S acquire(K key)
            {
                AccordCachingState<K, V> node = acquire(key, false);
                return safeRefFactory.apply(node);
            }

            public S acquireIfLoaded(K key)
            {
                AccordCachingState<K, V> node = acquire(key, true);
                if (node == null)
                    return null;
                return safeRefFactory.apply(node);
            }

            public S acquire(AccordCachingState<K, V> node)
            {
                Invariants.checkState(node.owner == this);
                acquireExisting(node, false);
                return safeRefFactory.apply(node);
            }

            private AccordCachingState<K, V> acquire(K key, boolean onlyIfLoaded)
            {
                incrementCacheQueries();
                @SuppressWarnings("unchecked")
                AccordCachingState<K, V> node = cache.get(key);
                return node == null
                       ? acquireAbsent(key, onlyIfLoaded)
                       : acquireExisting(node, onlyIfLoaded);
            }

            /*
             * Can only return a LOADING Node (or null)
             */
            private AccordCachingState<K, V> acquireAbsent(K key, boolean onlyIfLoaded)
            {
                incrementCacheMisses();
                if (onlyIfLoaded)
                    return null;
                AccordCachingState<K, V> node = nodeFactory.create(key, this);
                node.readyToLoad();
                node.references++;

                Object prev = cache.put(key, node);
                Invariants.checkState(prev == null, "%s not absent from cache: %s already present", key, node);
                ++size;
                node.notifyListeners(Listener::onAdd);
                maybeUpdateSize(node, heapEstimator);
                metrics.objectSize.update(node.lastQueriedEstimatedSizeOnHeap);
                maybeEvictSomeNodes();
                return node;
            }

            /*
             * Can't return EVICTED or INITIALIZED
             */
            private AccordCachingState<K, V> acquireExisting(AccordCachingState<K, V> node, boolean onlyIfLoaded)
            {
                Status status = node.status(); // status() completes

                if (status.isLoaded())
                    incrementCacheHits();
                else
                    incrementCacheMisses();

                if (onlyIfLoaded && !status.isLoaded())
                    return null;

                if (node.references == 0)
                {
                    --unreferenced;
                    node.unlink();
                }

                node.references++;

                return node;
            }

            public void release(AccordSafeState<K, V> safeRef, AccordTask<?> owner)
            {
                K key = safeRef.global().key();
                logger.trace("Releasing resources for {}: {}", key, safeRef);

                AccordCachingState<K, V> node = cache.get(key);

                checkState(safeRef.global() != null, "safeRef node is null for %s", key);
                checkState(safeRef.global() == node, "safeRef node not in map: %s != %s", safeRef.global(), node);
                checkState(node.references > 0, "references (%d) are zero for %s (%s)", node.references, key, node);
                checkState(node.isUnqueued());

                if (safeRef.hasUpdate())
                {
                    node.set(safeRef.current());
                    node.notifyListeners(Listener::onUpdate);
                }
                else if (node.isLoadingOrWaiting())
                {
                    node.loadingOrWaiting().remove(owner);
                }
                safeRef.invalidate();

                maybeUpdateSize(node, heapEstimator);
                node.notifyListeners(Listener::onRelease);

                if (--node.references == 0)
                {
                    ++unreferenced;
                    Status status = node.status(); // status() completes
                    switch (status)
                    {
                        default: throw new IllegalStateException("Unhandled status " + status);
                        case WAITING_TO_LOAD:
                        case LOADING:
                        case LOADED:
                        case MODIFIED:
                            logger.trace("Moving {} with status {} to eviction queue", key, status);
                            addLast(node);
                        case SAVING:
                        case FAILED_TO_SAVE:
                            break; // can never evict, so no point in adding to eviction queue either
                    }
                }

                // TODO (performance, expected): triggering on every release is potentially heavy
                maybeEvictSomeNodes();
            }

            public Stream<AccordCachingState<K, V>> stream()
            {
                return cache.values().stream();
            }

            Type<K, V, S> parent()
            {
                return Type.this;
            }

            @Override
            public Iterator<AccordCachingState<K, V>> iterator()
            {
                return stream().iterator();
            }

            void validateLoadEvicted(AccordCachingState<?, ?> node)
            {
                @SuppressWarnings("unchecked")
                AccordCachingState<K, V> state = (AccordCachingState<K, V>) node;
                K key = state.key();
                V evicted = state.get();
                if (!validateFunction.apply(node.owner.commandStore, key, evicted))
                    throw new IllegalStateException("Reloaded value for key " + key + " is not equal to or fuller than evicted value " + evicted);
            }

            @VisibleForTesting
            public AccordCachingState<K, V> getUnsafe(K key)
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
                AccordCachingState<K, V> node = cache.get(key);
                return node != null && node.references > 0;
            }

            @VisibleForTesting
            boolean keyIsReferenced(Object key, Class<? extends AccordSafeState<?, ?>> valClass)
            {
                AccordCachingState<?, ?> node = cache.get(key);
                return node != null && node.references > 0;
            }

            @VisibleForTesting
            boolean keyIsCached(Object key, Class<? extends AccordSafeState<?, ?>> valClass)
            {
                AccordCachingState<?, ?> node = cache.get(key);
                return node != null;
            }

            @VisibleForTesting
            int references(Object key, Class<? extends AccordSafeState<?, ?>> valClass)
            {
                AccordCachingState<?, ?> node = cache.get(key);
                return node != null ? node.references : 0;
            }

            void notifyListeners(BiConsumer<Listener<K, V>, AccordCachingState<K, V>> notify, AccordCachingState<K, V> node)
            {
                notifyListeners(listeners, notify, node);
                notifyListeners(typeListeners, notify, node);
            }

            void notifyListeners(List<Listener<K, V>> listeners, BiConsumer<Listener<K, V>, AccordCachingState<K, V>> notify, AccordCachingState<K, V> node)
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
                if (listeners == null)
                    throw new AssertionError("No listeners exist");
                if (!listeners.remove(l))
                    throw new AssertionError("Listener was not registered");
                if (listeners.isEmpty())
                    listeners = null;
            }

        }

        private final Class<K> keyClass;
        private final AccordCachingState.Factory<K, V> nodeFactory;
        private final Function<AccordCachingState<K, V>, S> safeRefFactory;
        private BiFunction<AccordCommandStore, K, V> loadFunction;
        private BiFunction<AccordCommandStore, V, Runnable> saveFunction;
        private final TriFunction<AccordCommandStore, K, V, Boolean> validateFunction;
        private final ToLongFunction<V> heapEstimator;
        private long bytesCached;
        private int size;

        @VisibleForTesting
        final CacheAccessMetrics typeMetrics;
        private final Stats stats = new Stats();
        private List<Listener<K, V>> typeListeners = null;

        public Type(
            Class<K> keyClass,
            Function<AccordCachingState<K, V>, S> safeRefFactory,
            BiFunction<AccordCommandStore, K, V> loadFunction,
            BiFunction<AccordCommandStore, V, Runnable> saveFunction,
            TriFunction<AccordCommandStore, K, V, Boolean> validateFunction,
            ToLongFunction<V> heapEstimator,
            AccordCachingState.Factory<K, V> nodeFactory)
        {
            this.keyClass = keyClass;
            this.safeRefFactory = safeRefFactory;
            this.loadFunction = loadFunction;
            this.saveFunction = saveFunction;
            this.validateFunction = validateFunction;
            this.heapEstimator = heapEstimator;
            this.typeMetrics = metrics.forInstance(keyClass);
            this.nodeFactory = nodeFactory;
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
            AccordStateCache.this.stats.queries++;
        }

        private void incrementCacheHits()
        {
            typeMetrics.hits.mark();
            metrics.hits.mark();
            stats.hits++;
            AccordStateCache.this.stats.hits++;
        }

        private void incrementCacheMisses()
        {
            typeMetrics.misses.mark();
            metrics.misses.mark();
            stats.misses++;
            AccordStateCache.this.stats.misses++;
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
            return AccordStateCache.this.stats;
        }

        @VisibleForTesting
        public void unsafeSetLoadFunction(BiFunction<AccordCommandStore, K, V> loadFunction)
        {
            this.loadFunction = loadFunction;
        }

        public BiFunction<AccordCommandStore, K, V> unsafeGetLoadFunction()
        {
            return loadFunction;
        }

        @VisibleForTesting
        public void unsafeSetSaveFunction(BiFunction<AccordCommandStore, V, Runnable> saveFunction)
        {
            this.saveFunction = saveFunction;
        }

        public BiFunction<AccordCommandStore, V, Runnable> unsafeGetSaveFunction()
        {
            return saveFunction;
        }

        @Override
        public long capacity()
        {
            return AccordStateCache.this.capacity();
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
            return AccordStateCache.this.bytesCached;
        }

        public int globalReferencedEntries()
        {
            return AccordStateCache.this.numReferencedEntries();
        }

        public int globalUnreferencedEntries()
        {
            return AccordStateCache.this.numUnreferencedEntries();
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
    AccordCachingState<?, ?> head()
    {
        Iterator<AccordCachingState<?, ?>> iter = iterator();
        return iter.hasNext() ? iter.next() : null;
    }

    @VisibleForTesting
    AccordCachingState<?, ?> tail()
    {
        AccordCachingState<?,?> last = null;
        Iterator<AccordCachingState<?, ?>> iter = iterator();
        while (iter.hasNext())
            last = iter.next();
        return last;
    }

    @VisibleForTesting
    public void awaitSaveResults()
    {
        for (AccordCachingState<?, ?> node : this)
            if (node.status() == SAVING)
                node.saving().awaitUninterruptibly();
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

    static <K, V> void registerJfrListener(int shardId, AccordStateCache.Type<K, V, ?> type, String name)
    {
        if (!DatabaseDescriptor.getAccordStateCacheListenerJFREnabled())
            return;

        type.register(new AccordStateCache.Listener<>() {
            private final IdentityHashMap<AccordCachingState<?, ?>, CacheEvents.Evict> pendingEvicts = new IdentityHashMap<>();

            @Override
            public void onAdd(AccordCachingState<K, V> state)
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
            public void onRelease(AccordCachingState<K, V> state)
            {

            }

            @Override
            public void onEvict(AccordCachingState<K, V> state)
            {
                CacheEvents.Evict event = pendingEvicts.remove(state);
                if (event == null) return;
                updateMutable(type, state, event);
                event.commit();
            }
        });
    }

    private static void updateMutable(AccordStateCache.Type<?, ?, ?> type, AccordCachingState<?, ?> state, CacheEvents event)
    {
        event.status = state.state().status().name();

        event.lastQueriedEstimatedSizeOnHeap = state.lastQueriedEstimatedSizeOnHeap();

        event.instanceAllocated = type.weightedSize();
        AccordStateCache.Stats stats = type.stats();
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

}
