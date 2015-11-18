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

import java.io.File;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.metrics.HintedHandoffMetrics;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;

import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Iterables.size;

/**
 * A singleton-ish wrapper over various hints components:
 * - a catalog of all hints stores
 * - a single-threaded write executor
 * - a multi-threaded dispatch executor
 * - the buffer pool for writing hints into
 *
 * The front-end for everything hints related.
 */
public final class HintsService implements HintsServiceMBean
{
    private static final Logger logger = LoggerFactory.getLogger(HintsService.class);

    public static final HintsService instance = new HintsService();

    private static final String MBEAN_NAME = "org.apache.cassandra.hints:type=HintsService";

    private static final int MIN_BUFFER_SIZE = 32 << 20;
    static final ImmutableMap<String, Object> EMPTY_PARAMS = ImmutableMap.of();

    private final HintsCatalog catalog;
    private final HintsWriteExecutor writeExecutor;
    private final HintsBufferPool bufferPool;
    private final HintsDispatchExecutor dispatchExecutor;
    private final AtomicBoolean isDispatchPaused;

    private volatile boolean isShutDown = false;

    private final ScheduledFuture triggerFlushingFuture;
    private volatile ScheduledFuture triggerDispatchFuture;

    public final HintedHandoffMetrics metrics;

    private HintsService()
    {
        File hintsDirectory = DatabaseDescriptor.getHintsDirectory();
        int maxDeliveryThreads = DatabaseDescriptor.getMaxHintsDeliveryThreads();

        catalog = HintsCatalog.load(hintsDirectory, createDescriptorParams());
        writeExecutor = new HintsWriteExecutor(catalog);

        int bufferSize = Math.max(DatabaseDescriptor.getMaxMutationSize() * 2, MIN_BUFFER_SIZE);
        bufferPool = new HintsBufferPool(bufferSize, writeExecutor::flushBuffer);

        isDispatchPaused = new AtomicBoolean(true);
        dispatchExecutor = new HintsDispatchExecutor(hintsDirectory, maxDeliveryThreads, isDispatchPaused);

        // periodically empty the current content of the buffers
        int flushPeriod = DatabaseDescriptor.getHintsFlushPeriodInMS();
        triggerFlushingFuture = ScheduledExecutors.optionalTasks.scheduleWithFixedDelay(() -> writeExecutor.flushBufferPool(bufferPool),
                                                                                        flushPeriod,
                                                                                        flushPeriod,
                                                                                        TimeUnit.MILLISECONDS);
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
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try
        {
            mbs.registerMBean(this, new ObjectName(MBEAN_NAME));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Write a hint for a iterable of nodes.
     *
     * @param hostIds host ids of the hint's target nodes
     * @param hint the hint to store
     */
    public void write(Iterable<UUID> hostIds, Hint hint)
    {
        if (isShutDown)
            throw new IllegalStateException("HintsService is shut down and can't accept new hints");

        // we have to make sure that the HintsStore instances get properly initialized - otherwise dispatch will not trigger
        catalog.maybeLoadStores(hostIds);

        if (hint.isLive())
            bufferPool.write(hostIds, hint);

        StorageMetrics.totalHints.inc(size(hostIds));
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

        Iterable<UUID> hostIds =
        transform(filter(StorageService.instance.getNaturalAndPendingEndpoints(keyspaceName, token), StorageProxy::shouldHint),
                  StorageService.instance::getHostIdForEndpoint);

        write(hostIds, hint);
    }

    /**
     * Flush the buffer pool for the selected target nodes, then fsync their writers.
     *
     * @param hostIds host ids of the nodes to flush and fsync hints for
     */
    public void flushAndFsyncBlockingly(Iterable<UUID> hostIds)
    {
        Iterable<HintsStore> stores = transform(hostIds, catalog::get);
        writeExecutor.flushBufferPool(bufferPool, stores);
        writeExecutor.fsyncWritersBlockingly(stores);
    }

    public synchronized void startDispatch()
    {
        if (isShutDown)
            throw new IllegalStateException("HintsService is shut down and cannot be restarted");

        isDispatchPaused.set(false);

        HintsDispatchTrigger trigger = new HintsDispatchTrigger(catalog, writeExecutor, dispatchExecutor, isDispatchPaused);
        // triggering hint dispatch is now very cheap, so we can do it more often - every 10 seconds vs. every 10 minutes,
        // previously; this reduces mean time to delivery, and positively affects batchlog delivery latencies, too
        triggerDispatchFuture = ScheduledExecutors.scheduledTasks.scheduleWithFixedDelay(trigger, 10, 10, TimeUnit.SECONDS);
    }

    public void pauseDispatch()
    {
        logger.info("Paused hints dispatch");
        isDispatchPaused.set(true);
    }

    public void resumeDispatch()
    {
        logger.info("Resumed hints dispatch");
        isDispatchPaused.set(false);
    }

    /**
     * Gracefully and blockingly shut down the service.
     *
     * Will abort dispatch sessions that are currently in progress (which is okay, it's idempotent),
     * and make sure the buffers are flushed, hints files written and fsynced.
     */
    public synchronized void shutdownBlocking()
    {
        if (isShutDown)
            throw new IllegalStateException("HintsService has already been shut down");
        isShutDown = true;

        if (triggerDispatchFuture != null)
            triggerDispatchFuture.cancel(false);
        pauseDispatch();

        triggerFlushingFuture.cancel(false);

        writeExecutor.flushBufferPool(bufferPool);
        writeExecutor.closeAllWriters();

        dispatchExecutor.shutdownBlocking();
        writeExecutor.shutdownBlocking();
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
        InetAddress target;
        try
        {
            target = InetAddress.getByName(address);
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
    public void deleteAllHintsForEndpoint(InetAddress target)
    {
        UUID hostId = StorageService.instance.getHostIdForEndpoint(target);
        if (hostId == null)
            throw new IllegalArgumentException("Can't delete hints for unknown address " + target);
        catalog.deleteAllHints(hostId);
    }

    /**
     * Cleans up hints-related state after a node with id = hostId left.
     *
     * Dispatcher should stop itself (isHostAlive() will start returning false for the leaving host), but we'll wait for
     * completion anyway.
     *
     * We should also flush the buffer is there are any thints for the node there, and close the writer (if any),
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
        HintsStore store = catalog.get(hostId);
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
        catch (InterruptedException | ExecutionException e)
        {
            throw new RuntimeException(e);
        }

        // wait for the current dispatch session to end (if any), so that the currently dispatched file gets removed
        dispatchExecutor.completeDispatchBlockingly(store);

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
        catch (InterruptedException | ExecutionException e)
        {
            throw new RuntimeException(e);
        }

        // unpause dispatch, or else transfer() will return immediately
        resumeDispatch();

        // wait for the current dispatch session to end
        catalog.stores().forEach(dispatchExecutor::completeDispatchBlockingly);

        return dispatchExecutor.transfer(catalog, hostIdSupplier);
    }

    HintsCatalog getCatalog()
    {
        return catalog;
    }
}
