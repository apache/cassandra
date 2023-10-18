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
package org.apache.cassandra.hints;

import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.locator.ReplicaLayout;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.IFailureDetector;
import org.apache.cassandra.locator.EndpointsForToken;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.metrics.HintedHandoffMetrics;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.MBeanWrapper;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;

/**
 * A singleton-ish wrapper over various hints components:
 * - a catalog of all hints stores
 * - a single-threaded write executor
 * - a multi-threaded dispatch executor
 * - the buffer pool for writing hints into
 * - an optional scheduled task to clean up the applicable hints files
 *
 * The front-end for everything hints related.
 */
public final class HintsService implements HintsServiceMBean
{
    private static final Logger logger = LoggerFactory.getLogger(HintsService.class);

    public static HintsService instance = new HintsService();

    public static final String MBEAN_NAME = "org.apache.cassandra.hints:type=HintsService";

    private static final int MIN_BUFFER_SIZE = 32 << 20;
    static final ImmutableMap<String, Object> EMPTY_PARAMS = ImmutableMap.of();

    private final HintsCatalog catalog;
    private final HintsWriteExecutor writeExecutor;
    private final HintsBufferPool bufferPool;
    final HintsDispatchExecutor dispatchExecutor;
    final AtomicBoolean isDispatchPaused;

    private volatile boolean isShutDown = false;

    private final ScheduledFuture triggerFlushingFuture;
    private volatile ScheduledFuture triggerDispatchFuture;
    private final ScheduledFuture triggerCleanupFuture;

    public final HintedHandoffMetrics metrics;

    private HintsService()
    {
        this(FailureDetector.instance);
    }

    @VisibleForTesting
    HintsService(IFailureDetector failureDetector)
    {
        File hintsDirectory = DatabaseDescriptor.getHintsDirectory();
        int maxDeliveryThreads = DatabaseDescriptor.getMaxHintsDeliveryThreads();

        catalog = HintsCatalog.load(hintsDirectory, createDescriptorParams());
        writeExecutor = new HintsWriteExecutor(catalog);

        int bufferSize = Math.max(DatabaseDescriptor.getMaxMutationSize() * 2, MIN_BUFFER_SIZE);
        bufferPool = new HintsBufferPool(bufferSize, writeExecutor::flushBuffer);

        isDispatchPaused = new AtomicBoolean(true);
        dispatchExecutor = new HintsDispatchExecutor(hintsDirectory, maxDeliveryThreads, isDispatchPaused, failureDetector::isAlive);

        // periodically empty the current content of the buffers
        int flushPeriod = DatabaseDescriptor.getHintsFlushPeriodInMS();
        triggerFlushingFuture = ScheduledExecutors.optionalTasks.scheduleWithFixedDelay(() -> writeExecutor.flushBufferPool(bufferPool),
                                                                                        flushPeriod,
                                                                                        flushPeriod,
                                                                                        TimeUnit.MILLISECONDS);

        // periodically cleanup the expired hints
        HintsCleanupTrigger cleanupTrigger = new HintsCleanupTrigger(catalog, dispatchExecutor);
        triggerCleanupFuture = ScheduledExecutors.optionalTasks.scheduleWithFixedDelay(cleanupTrigger, 1, 1, TimeUnit.HOURS);

        metrics = new HintedHandoffMetrics();
    }

    private static ImmutableMap<String, Object> createDescriptorParams()
    {
        ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();

        ParameterizedClass compressionConfig = DatabaseDescriptor.getHintsCompression();
        if (compressionConfig != null)
        {
            ImmutableMap.Builder<String, Object> compressorParams = ImmutableMap.builder();

            compressorParams.put(ParameterizedClass.CLASS_NAME, compressionConfig.class_name);
            if (compressionConfig.parameters != null)
            {
                compressorParams.put(ParameterizedClass.PARAMETERS, compressionConfig.parameters);
            }
            builder.put(HintsDescriptor.COMPRESSION, compressorParams.build());
        }

        return builder.build();
    }

    public void registerMBean()
    {
        MBeanWrapper.instance.registerMBean(this, MBEAN_NAME);
    }

    /**
     * Write a hint for a iterable of nodes.
     *
     * @param hostIds host ids of the hint's target nodes
     * @param hint the hint to store
     */
    public void write(Collection<UUID> hostIds, Hint hint)
    {
        if (isShutDown)
            throw new IllegalStateException("HintsService is shut down and can't accept new hints");

        // we have to make sure that the HintsStore instances get properly initialized - otherwise dispatch will not trigger
        catalog.maybeLoadStores(hostIds);

        bufferPool.write(hostIds, hint);

        StorageMetrics.totalHints.inc(hostIds.size());
    }

    /**
     * Write a hint for a single node.
     *
     * @param hostId host id of the hint's target node
     * @param hint the hint to store
     */
    public void write(UUID hostId, Hint hint)
    {
        write(Collections.singleton(hostId), hint);
    }

    /**
     * Write a hint for all replicas. Used to re-dispatch hints whose destination is either missing or no longer correct.
     */
    void writeForAllReplicas(Hint hint)
    {
        String keyspaceName = hint.mutation.getKeyspaceName();
        Token token = hint.mutation.key().getToken();

        EndpointsForToken replicas = ReplicaLayout.forTokenWriteLiveAndDown(Keyspace.open(keyspaceName), token).all();

        // judicious use of streams: eagerly materializing probably cheaper
        // than performing filters / translations 2x extra via Iterables.filter/transform
        List<UUID> hostIds = replicas.stream()
                .filter(replica -> StorageProxy.shouldHint(replica, false))
                .map(replica -> StorageService.instance.getHostIdForEndpoint(replica.endpoint()))
                .collect(Collectors.toList());

        write(hostIds, hint);
    }

    /**
     * Flush the buffer pool for the selected target nodes, then fsync their writers.
     *
     * @param hostIds host ids of the nodes to flush and fsync hints for
     */
    public void flushAndFsyncBlockingly(Iterable<UUID> hostIds)
    {
        Iterable<HintsStore> stores = filter(transform(hostIds, catalog::getNullable), Objects::nonNull);
        writeExecutor.flushBufferPool(bufferPool, stores);
        writeExecutor.fsyncWritersBlockingly(stores);
    }

    public synchronized void startDispatch()
    {
        if (isShutDown)
            throw new IllegalStateException("HintsService is shut down and cannot be restarted");

        isDispatchPaused.set(false);

        HintsServiceDiagnostics.dispatchingStarted(this);

        HintsDispatchTrigger trigger = new HintsDispatchTrigger(catalog, writeExecutor, dispatchExecutor, isDispatchPaused);
        // triggering hint dispatch is now very cheap, so we can do it more often - every 10 seconds vs. every 10 minutes,
        // previously; this reduces mean time to delivery, and positively affects batchlog delivery latencies, too
        triggerDispatchFuture = ScheduledExecutors.scheduledTasks.scheduleWithFixedDelay(trigger, 10, 10, TimeUnit.SECONDS);
    }

    public void pauseDispatch()
    {
        logger.info("Paused hints dispatch");
        isDispatchPaused.set(true);

        HintsServiceDiagnostics.dispatchingPaused(this);
    }

    public void resumeDispatch()
    {
        logger.info("Resumed hints dispatch");
        isDispatchPaused.set(false);

        HintsServiceDiagnostics.dispatchingResumed(this);
    }

    /**
     * Get the total size in bytes of all the hints files associating with the host on disk.
     * @param hostId belonging host
     * @return total file size, in bytes
     */
    public long getTotalHintsSize(UUID hostId)
    {
        HintsStore store = catalog.getNullable(hostId);
        if (store == null)
            return 0;
        return store.getTotalFileSize();
    }

    /**
     * Gracefully and blockingly shut down the service.
     *
     * Will abort dispatch sessions that are currently in progress (which is okay, it's idempotent),
     * and make sure the buffers are flushed, hints files written and fsynced.
     */
    public synchronized void shutdownBlocking() throws ExecutionException, InterruptedException
    {
        if (isShutDown)
            throw new IllegalStateException("HintsService has already been shut down");
        isShutDown = true;

        if (triggerDispatchFuture != null)
            triggerDispatchFuture.cancel(false);
        pauseDispatch();

        triggerFlushingFuture.cancel(false);

        triggerCleanupFuture.cancel(false);

        writeExecutor.flushBufferPool(bufferPool).get();
        writeExecutor.closeAllWriters().get();

        dispatchExecutor.shutdownBlocking();
        writeExecutor.shutdownBlocking();

        HintsServiceDiagnostics.dispatchingShutdown(this);
        bufferPool.close();
    }

    /**
     * Returns all pending hints that this node has.
     *
     * @return a list of {@link PendingHintsInfo}
     */
    public List<PendingHintsInfo> getPendingHintsInfo()
    {
        return catalog.stores()
                      .filter(HintsStore::hasFiles)
                      .map(HintsStore::getPendingHintsInfo)
                      .collect(Collectors.toList());
    }

    /**
     * Returns all pending hints that this node has.
     *
     * @return a list of maps with endpoints' ids, total number of hint files, their oldest and newest timestamps.
     */
    public List<Map<String, String>> getPendingHints()
    {
        return getPendingHintsInfo().stream()
                                    .map(PendingHintsInfo::asMap)
                                    .collect(Collectors.toList());
    }

    /**
     * Deletes all hints for all destinations. Doesn't make snapshots - should be used with care.
     */
    public void deleteAllHints()
    {
        catalog.deleteAllHints();
    }

    /**
     * Deletes all hints for the provided destination. Doesn't make snapshots - should be used with care.
     *
     * @param address inet address of the target node - encoded as a string for easier JMX consumption
     */
    public void deleteAllHintsForEndpoint(String address)
    {
        InetAddressAndPort target;
        try
        {
            target = InetAddressAndPort.getByName(address);
        }
        catch (UnknownHostException e)
        {
            throw new IllegalArgumentException(e);
        }
        deleteAllHintsForEndpoint(target);
    }

    /**
     * Deletes all hints for the provided destination. Doesn't make snapshots - should be used with care.
     *
     * @param target inet address of the target node
     */
    public void deleteAllHintsForEndpoint(InetAddressAndPort target)
    {
        UUID hostId = StorageService.instance.getHostIdForEndpoint(target);
        if (hostId == null)
            throw new IllegalArgumentException("Can't delete hints for unknown address " + target);
        catalog.deleteAllHints(hostId);
    }

    /**
     * Cleans up hints-related state after a node with id = hostId left.
     *
     * Dispatcher can not stop itself (isHostAlive() can not start returning false for the leaving host because this
     * method is called by the same thread as gossip, which blocks gossip), so we can't simply wait for
     * completion.
     *
     * We should also flush the buffer if there are any hints for the node there, and close the writer (if any),
     * so that we don't leave any hint files lying around.
     *
     * Once that is done, we can simply delete all hint files and remove the host id from the catalog.
     *
     * The worst that can happen if we don't get everything right is a hints file (or two) remaining undeleted.
     *
     * @param hostId id of the node being excised
     */
    public void excise(UUID hostId)
    {
        HintsStore store = catalog.getNullable(hostId);
        if (store == null)
            return;

        // flush the buffer and then close the writer for the excised host id, to make sure that no new files will appear
        // for this host id after we are done
        Future flushFuture = writeExecutor.flushBufferPool(bufferPool, Collections.singleton(store));
        Future closeFuture = writeExecutor.closeWriter(store);
        try
        {
            flushFuture.get();
            closeFuture.get();
        }
        catch (InterruptedException e)
        {
            throw new UncheckedInterruptedException(e);
        }
        catch (ExecutionException e)
        {
            throw new RuntimeException(e);
        }

        // interrupt the current dispatch session to end (if any), so that the currently dispatched file gets removed
        dispatchExecutor.interruptDispatch(store.hostId);

        // delete all the hints files and remove the HintsStore instance from the map in the catalog
        catalog.exciseStore(hostId);
    }

    /**
     * Transfer all local hints to the hostId supplied by hostIdSupplier
     *
     * Flushes the buffer to make sure all hints are on disk and closes the hint writers
     * so we don't leave any hint files around.
     *
     * After that, we serially dispatch all the hints in the HintsCatalog.
     *
     * If we fail delivering all hints, we will ask the hostIdSupplier for a new target host
     * and retry delivering any remaining hints there, once, with a delay of 10 seconds before retrying.
     *
     * @param hostIdSupplier supplier of stream target host ids. This is generally
     *                       the closest one according to the DynamicSnitch
     * @return When this future is done, it either has streamed all hints to remote nodes or has failed with a proper
     *         log message
     */
    public Future transferHints(Supplier<UUID> hostIdSupplier)
    {
        Future flushFuture = writeExecutor.flushBufferPool(bufferPool);
        Future closeFuture = writeExecutor.closeAllWriters();
        try
        {
            flushFuture.get();
            closeFuture.get();
        }
        catch (InterruptedException e)
        {
            throw new UncheckedInterruptedException(e);
        }
        catch (ExecutionException e)
        {
            throw new RuntimeException(e);
        }

        // unpause dispatch, or else transfer() will return immediately
        resumeDispatch();

        // wait for the current dispatch session to end
        catalog.stores().forEach(dispatchExecutor::completeDispatchBlockingly);

        return dispatchExecutor.transfer(catalog, hostIdSupplier);
    }

    /**
     * Get the earliest hint written for a particular node,
     * @param hostId UUID of the node to check it's hints.
     * @return earliest hint as per unix time or Long.MIN_VALUE if hostID is null
     */
    public long getEarliestHintForHost(UUID hostId)
    {
        // Need to check only the first descriptor + all buffers.
        HintsStore store = catalog.get(hostId);
        HintsDescriptor desc = store.getFirstDescriptor();
        long timestamp = desc == null ? Clock.Global.currentTimeMillis() : desc.timestamp;
        return Math.min(timestamp, bufferPool.getEarliestHintForHost(hostId));
    }

    HintsCatalog getCatalog()
    {
        return catalog;
    }

    /**
     * Returns true in case service is shut down.
     */
    public boolean isShutDown()
    {
        return isShutDown;
    }
    
    @VisibleForTesting
    public boolean isDispatchPaused()
    {
        return isDispatchPaused.get();
    }
}
