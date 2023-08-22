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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.ToLongFunction;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.utils.IntrusiveLinkedList;
import accord.utils.async.AsyncChains;
import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.service.accord.AccordCachingState.Status;

import static accord.utils.Invariants.checkState;
import static java.lang.String.format;
import static org.apache.cassandra.service.accord.AccordCachingState.Status.EVICTED;
import static org.apache.cassandra.service.accord.AccordCachingState.Status.FAILED_TO_LOAD;
import static org.apache.cassandra.service.accord.AccordCachingState.Status.LOADING;
import static org.apache.cassandra.service.accord.AccordCachingState.Status.SAVING;

/**
 * Cache for AccordCommand and AccordCommandsForKey, available memory is shared between the two object types.
 * </p>
 * Supports dynamic object sizes. After each acquire/free cycle, the cacheable objects size is recomputed to
 * account for data added/removed during txn processing if it's modified flag is set
 */
public class AccordStateCache extends IntrusiveLinkedList<AccordCachingState<?,?>>
{
    private static final Logger logger = LoggerFactory.getLogger(AccordStateCache.class);

    static class Stats
    {
        private long queries;
        private long hits;
        private long misses;
    }

    private final Map<Object, AccordCachingState<?, ?>> cache = new HashMap<>();
    private final HashMap<Class<?>, Instance<?, ?, ?>> instances = new HashMap<>();

    private final ExecutorPlus loadExecutor, saveExecutor;

    private int unreferenced = 0;
    private long maxSizeInBytes;
    private long bytesCached = 0;
    private final Stats stats = new Stats();

    public AccordStateCache(ExecutorPlus loadExecutor, ExecutorPlus saveExecutor, long maxSizeInBytes)
    {
        this.loadExecutor = loadExecutor;
        this.saveExecutor = saveExecutor;
        this.maxSizeInBytes = maxSizeInBytes;
    }

    public void setMaxSize(long size)
    {
        maxSizeInBytes = size;
        maybeEvictSomeNodes();
    }

    public long getMaxSize()
    {
        return maxSizeInBytes;
    }

    private void unlink(AccordCachingState<?, ?> node)
    {
        node.unlink();
        unreferenced--;
    }

    private void link(AccordCachingState<?, ?> node)
    {
        addLast(node);
        unreferenced++;
    }

    @SuppressWarnings("unchecked")
    private <K, V> void maybeUpdateSize(AccordCachingState<?, ?> node, ToLongFunction<?> estimator)
    {
        if (node.shouldUpdateSize())
            bytesCached += ((AccordCachingState<K, V>) node).estimatedSizeOnHeapDelta((ToLongFunction<V>) estimator);
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
            checkState(node.references == 0);

            /*
             * TODO (expected, efficiency):
             *    can this be reworked so we're not skipping unevictable nodes everytime we try to evict?
             */
            Status status = node.status(); // status() call completes (if completeable)
            switch (status)
            {
                default: throw new IllegalStateException("Unhandled status " + status);
                case LOADED:
                    unlink(node);
                    evict(node);
                    break;
                case MODIFIED:
                    // schedule a save to disk, keep linked and in the cache map
                    Instance<?, ?, ?> instance = instanceForNode(node);
                    node.save(saveExecutor, instance.saveFunction);
                    maybeUpdateSize(node, instance.heapEstimator);
                    break;
                case SAVING:
                    // skip over until completes to LOADED or FAILED_TO_SAVE
                    break;
                case FAILED_TO_SAVE:
                    // TODO (consider): panic when a save fails
                    // permanently unlink, but keep in the map
                    unlink(node);
            }
        }
    }

    private boolean isInQueue(AccordCachingState<?, ?> node)
    {
        return node.isLinked();
    }

    private void evict(AccordCachingState<?, ?> node)
    {
        if (logger.isTraceEnabled())
            logger.trace("Evicting {} {} - {}", node.status(), node.key(), node.isLoaded() ? node.get() : null);

        checkState(!isInQueue(node));

        bytesCached -= node.lastQueriedEstimatedSizeOnHeap;
        if (!node.hasListeners())
        {
            AccordCachingState<?, ?> self = cache.remove(node.key());
            checkState(self == node, "Leaked node detected; was attempting to remove %s but cache had %s", node, self);
        }
        else
        {
            node.markEvicted(); // keep the node in the cache to prevent transient listeners from being GCd
        }
    }

    private Instance<?, ?, ?> instanceForNode(AccordCachingState<?, ?> node)
    {
        return instances.get(node.key().getClass());
    }

    public <K, V, S extends AccordSafeState<K, V>> Instance<K, V, S> instance(
        Class<K> keyClass,
        Class<? extends K> realKeyClass,
        Function<AccordCachingState<K, V>, S> safeRefFactory,
        Function<K, V> loadFunction,
        BiFunction<V, V, Runnable> saveFunction,
        ToLongFunction<V> heapEstimator)
    {
        Instance<K, V, S> instance =
            new Instance<>(keyClass, safeRefFactory, loadFunction, saveFunction, heapEstimator);

        if (instances.put(realKeyClass, instance) != null)
            throw new IllegalArgumentException(format("Cache instances for key type %s already exists", realKeyClass.getName()));

        return instance;
    }

    public class Instance<K, V, S extends AccordSafeState<K, V>>
    {
        private final Class<K> keyClass;
        private final Function<AccordCachingState<K, V>, S> safeRefFactory;
        private Function<K, V> loadFunction;
        private BiFunction<V, V, Runnable> saveFunction;
        private final ToLongFunction<V> heapEstimator;
        private final Stats stats = new Stats();

        public Instance(
            Class<K> keyClass,
            Function<AccordCachingState<K, V>, S> safeRefFactory,
            Function<K, V> loadFunction,
            BiFunction<V, V, Runnable> saveFunction,
            ToLongFunction<V> heapEstimator)
        {
            this.keyClass = keyClass;
            this.safeRefFactory = safeRefFactory;
            this.loadFunction = loadFunction;
            this.saveFunction = saveFunction;
            this.heapEstimator = heapEstimator;
        }

        public Stream<AccordCachingState<K, V>> stream()
        {
            return cache.entrySet().stream()
                        .filter(e -> keyClass.isAssignableFrom(e.getKey().getClass()))
                        .map(e -> (AccordCachingState<K, V>) e.getValue());
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

        private AccordCachingState<K, V> acquire(K key, boolean onlyIfLoaded)
        {
            incrementCacheQueries();
            @SuppressWarnings("unchecked")
            AccordCachingState<K, V> node = (AccordCachingState<K, V>) cache.get(key);
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
            AccordCachingState<K, V> node = new AccordCachingState<>(key);
            node.load(loadExecutor, loadFunction);
            node.references++;
            cache.put(key, node);
            maybeUpdateSize(node, heapEstimator);
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
                if (status == FAILED_TO_LOAD || status == EVICTED)
                    node.reset().load(loadExecutor, loadFunction);

                if (isInQueue(node))
                    unlink(node);
            }
            node.references++;

            return node;
        }

        public void release(S safeRef)
        {
            K key = safeRef.global().key();
            logger.trace("Releasing resources for {}: {}", key, safeRef);

            @SuppressWarnings("unchecked")
            AccordCachingState<K, V> node = (AccordCachingState<K, V>) cache.get(key);

            checkState(node != null, "node is null for %s", key);
            checkState(node.references > 0, "references (%d) are zero for %s (%s)", node.references, key, node);
            checkState(safeRef.global() == node);
            checkState(!isInQueue(node));

            if (safeRef.hasUpdate())
                node.set(safeRef.current());

            maybeUpdateSize(node, heapEstimator);

            if (--node.references == 0)
            {
                Status status = node.status(); // status() completes
                switch (status)
                {
                    default: throw new IllegalStateException("Unhandled status " + status);
                    case LOADING:
                    case FAILED_TO_LOAD:
                        logger.trace("Evicting {} with status {}", key, status);
                        evict(node);
                        break;
                    case LOADED:
                    case MODIFIED:
                    case SAVING:
                        logger.trace("Moving {} with status {} to eviction queue", key, status);
                        link(node);
                        break;
                    case FAILED_TO_SAVE:
                        break; // can never evict, so no point in adding to eviction queue either
                }
            }

            // TODO (performance, expected): triggering on every release is potentially heavy
            maybeEvictSomeNodes();
        }

        @VisibleForTesting
        public AccordCachingState<K, V> getUnsafe(K key)
        {
            //noinspection unchecked
            return (AccordCachingState<K, V>) cache.get(key);
        }

        @VisibleForTesting
        public boolean isReferenced(K key)
        {
            //noinspection unchecked
            AccordCachingState<K, V> node = (AccordCachingState<K, V>) cache.get(key);
            return node != null && node.references > 0;
        }

        @VisibleForTesting
        public boolean isLoaded(K key)
        {
            //noinspection unchecked
            AccordCachingState<K, V> node = (AccordCachingState<K, V>) cache.get(key);
            return node != null && node.isLoaded();
        }

        @VisibleForTesting
        public boolean hasLoadResult(K key)
        {
            AccordCachingState<?, ?> node = cache.get(key);
            return node != null && node.status() == LOADING;
        }

        @VisibleForTesting
        public boolean hasSaveResult(K key)
        {
            AccordCachingState<?, ?> node = cache.get(key);
            return node != null && node.status() == SAVING;
        }

        @VisibleForTesting
        public void complete(K key)
        {
            AccordCachingState<?, ?> node = cache.get(key);
            if (node != null)
                node.complete();
        }

        public long cacheQueries()
        {
            return stats.queries;
        }

        public long cacheHits()
        {
            return stats.hits;
        }

        public long cacheMisses()
        {
            return stats.misses;
        }

        private void incrementCacheQueries()
        {
            stats.queries++;
            AccordStateCache.this.stats.queries++;
        }

        private void incrementCacheHits()
        {
            stats.hits++;
            AccordStateCache.this.stats.hits++;
        }

        private void incrementCacheMisses()
        {
            stats.misses++;
            AccordStateCache.this.stats.misses++;
        }

        @VisibleForTesting
        public void unsafeSetLoadFunction(Function<K, V> loadFunction)
        {
            this.loadFunction = loadFunction;
        }

        @VisibleForTesting
        public void unsafeSetSaveFunction(BiFunction<V, V, Runnable> saveFunction)
        {
            this.saveFunction = saveFunction;
        }
    }

    @VisibleForTesting
    void unsafeClear()
    {
        cache.clear();
        //noinspection StatementWithEmptyBody
        while (null != poll());
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
                AsyncChains.awaitUninterruptibly(node.saving());
    }

    @VisibleForTesting
    int numReferencedEntries()
    {
        return cache.size() - unreferenced;
    }

    @VisibleForTesting
    int numUnreferencedEntries()
    {
        return unreferenced;
    }

    @VisibleForTesting
    int totalNumEntries()
    {
        return cache.size();
    }

    @VisibleForTesting
    long bytesCached()
    {
        return bytesCached;
    }

    @VisibleForTesting
    boolean keyIsReferenced(Object key)
    {
        AccordCachingState<?, ?> node = cache.get(key);
        return node != null && node.references > 0;
    }

    @VisibleForTesting
    boolean keyIsCached(Object key)
    {
        AccordCachingState<?, ?> node = cache.get(key);
        return node != null && node.status() != EVICTED;
    }

    @VisibleForTesting
    int references(Object key)
    {
        AccordCachingState<?, ?> node = cache.get(key);
        return node != null ? node.references : 0;
    }

    public long cacheQueries()
    {
        return stats.queries;
    }

    public long cacheHits()
    {
        return stats.hits;
    }

    public long cacheMisses()
    {
        return stats.misses;
    }
}
