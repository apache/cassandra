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

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;

import com.google.common.util.concurrent.RateLimiter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;
import org.apache.cassandra.utils.concurrent.Future;

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;

/**
 * A multi-threaded (by default) executor for dispatching hints.
 *
 * Most of dispatch is triggered by {@link HintsDispatchTrigger} running every ~10 seconds.
 */
final class HintsDispatchExecutor
{
    private static final Logger logger = LoggerFactory.getLogger(HintsDispatchExecutor.class);

    private final File hintsDirectory;
    private final ExecutorPlus executor;
    private final AtomicBoolean isPaused;
    private final Predicate<InetAddressAndPort> isAlive;
    private final Map<UUID, Future> scheduledDispatches;

    HintsDispatchExecutor(File hintsDirectory, int maxThreads, AtomicBoolean isPaused, Predicate<InetAddressAndPort> isAlive)
    {
        this.hintsDirectory = hintsDirectory;
        this.isPaused = isPaused;
        this.isAlive = isAlive;

        scheduledDispatches = new ConcurrentHashMap<>();
        executor = executorFactory()
                .withJmxInternal()
                .configurePooled("HintsDispatcher", maxThreads)
                .withThreadPriority(Thread.MIN_PRIORITY)
                .build();
    }

    /*
     * It's safe to terminate dispatch in process and to deschedule dispatch.
     */
    void shutdownBlocking()
    {
        scheduledDispatches.clear();
        executor.shutdownNow();
        try
        {
            executor.awaitTermination(1, TimeUnit.MINUTES);
        }
        catch (InterruptedException e)
        {
            throw new UncheckedInterruptedException(e);
        }
    }

    boolean isScheduled(HintsStore store)
    {
        return scheduledDispatches.containsKey(store.hostId);
    }

    Future dispatch(HintsStore store)
    {
        return dispatch(store, store.hostId);
    }

    Future dispatch(HintsStore store, UUID hostId)
    {
        /*
         * It is safe to perform dispatch for the same host id concurrently in two or more threads,
         * however there is nothing to win from it - so we don't.
         *
         * Additionally, having just one dispatch task per host id ensures that we'll never violate our per-destination
         * rate limit, without having to share a ratelimiter between threads.
         *
         * It also simplifies reasoning about dispatch sessions.
         */
        return scheduledDispatches.computeIfAbsent(hostId, uuid -> executor.submit(new DispatchHintsTask(store, hostId)));
    }

    Future transfer(HintsCatalog catalog, Supplier<UUID> hostIdSupplier)
    {
        return executor.submit(new TransferHintsTask(catalog, hostIdSupplier));
    }

    void completeDispatchBlockingly(HintsStore store)
    {
        Future future = scheduledDispatches.get(store.hostId);
        try
        {
            if (future != null)
                future.get();
        }
        catch (InterruptedException e)
        {
            throw new UncheckedInterruptedException(e);
        }
        catch (ExecutionException e)
        {
            throw new RuntimeException(e);
        }
    }

    void interruptDispatch(UUID hostId)
    {
        Future future = scheduledDispatches.remove(hostId);

        if (null != future)
            future.cancel(true);
    }

    private final class TransferHintsTask implements Runnable
    {
        private final HintsCatalog catalog;

        /*
         * Supplies target hosts to stream to. Generally returns the one the DynamicSnitch thinks is closest.
         * We use a supplier here to be able to get a new host if the current one dies during streaming.
         */
        private final Supplier<UUID> hostIdSupplier;

        private TransferHintsTask(HintsCatalog catalog, Supplier<UUID> hostIdSupplier)
        {
            this.catalog = catalog;
            this.hostIdSupplier = hostIdSupplier;
        }

        @Override
        public void run()
        {
            UUID hostId = hostIdSupplier.get();
            InetAddressAndPort address = StorageService.instance.getEndpointForHostId(hostId);
            logger.info("Transferring all hints to {}: {}", address, hostId);
            if (transfer(hostId))
                return;

            logger.warn("Failed to transfer all hints to {}: {}; will retry in {} seconds", address, hostId, 10);

            try
            {
                TimeUnit.SECONDS.sleep(10);
            }
            catch (InterruptedException e)
            {
                throw new UncheckedInterruptedException(e);
            }

            hostId = hostIdSupplier.get();
            logger.info("Transferring all hints to {}: {}", address, hostId);
            if (!transfer(hostId))
            {
                logger.error("Failed to transfer all hints to {}: {}", address, hostId);
                throw new RuntimeException("Failed to transfer all hints to " + hostId);
            }
        }

        private boolean transfer(UUID hostId)
        {
            catalog.stores()
                   .map(store -> new DispatchHintsTask(store, hostId, true))
                   .forEach(Runnable::run);

            return !catalog.hasFiles();
        }
    }

    private final class DispatchHintsTask implements Runnable
    {
        private final HintsStore store;
        private final UUID hostId;
        private final RateLimiter rateLimiter;

        DispatchHintsTask(HintsStore store, UUID hostId, boolean isTransfer)
        {
            this.store = store;
            this.hostId = hostId;

            // Rate limit is in bytes per second. Uses Double.MAX_VALUE if disabled (set to 0 in cassandra.yaml).
            // Max rate is scaled by the number of nodes in the cluster (CASSANDRA-5272), unless we are transferring
            // hints during decomission rather than dispatching them to their final destination.
            // The goal is to bound maximum hints traffic going towards a particular node from the rest of the cluster,
            // not total outgoing hints traffic from this node. This is why the rate limiter is not shared between
            // all the dispatch tasks (as there will be at most one dispatch task for a particular host id at a time).
            int nodesCount = isTransfer ? 1 : Math.max(1, StorageService.instance.getTokenMetadata().getAllEndpoints().size() - 1);
            double throttleInBytes = DatabaseDescriptor.getHintedHandoffThrottleInKiB() * 1024.0 / nodesCount;
            this.rateLimiter = RateLimiter.create(throttleInBytes == 0 ? Double.MAX_VALUE : throttleInBytes);
        }

        DispatchHintsTask(HintsStore store, UUID hostId)
        {
            this(store, hostId, false);
        }

        public void run()
        {
            try
            {
                dispatch();
            }
            finally
            {
                scheduledDispatches.remove(hostId);
            }
        }

        private void dispatch()
        {
            while (true)
            {
                if (isPaused.get())
                    break;

                HintsDescriptor descriptor = store.poll();
                if (descriptor == null)
                    break;

                try
                {
                    if (!dispatch(descriptor))
                        break;
                }
                catch (FSReadError e)
                {
                    logger.error(String.format("Failed to dispatch hints file %s: file is corrupted", descriptor.fileName()), e);
                    store.cleanUp(descriptor);
                    store.markCorrupted(descriptor);
                    throw e;
                }
            }
        }

        /*
         * Will return true if dispatch was successful, false if we hit a failure (destination node went down, for example).
         */
        private boolean dispatch(HintsDescriptor descriptor)
        {
            logger.trace("Dispatching hints file {}", descriptor.fileName());

            InetAddressAndPort address = StorageService.instance.getEndpointForHostId(hostId);
            if (address != null)
                return deliver(descriptor, address);

            // address == null means the target no longer exist; find new home for each hint entry.
            convert(descriptor);
            return true;
        }

        private boolean deliver(HintsDescriptor descriptor, InetAddressAndPort address)
        {
            File file = descriptor.file(hintsDirectory);
            InputPosition offset = store.getDispatchOffset(descriptor);

            BooleanSupplier shouldAbort = () -> !isAlive.test(address) || isPaused.get();
            try (HintsDispatcher dispatcher = HintsDispatcher.create(file, rateLimiter, address, descriptor.hostId, shouldAbort))
            {
                if (offset != null)
                    dispatcher.seek(offset);

                try
                {
                    if (dispatcher.dispatch())
                    {
                        store.delete(descriptor);
                        store.cleanUp(descriptor);
                        logger.info("Finished hinted handoff of file {} to endpoint {}: {}", descriptor.fileName(), address, hostId);
                        return true;
                    }
                    else
                    {
                        handleDispatchFailure(dispatcher, descriptor, address);
                        return false;
                    }
                }
                // we wrap InterruptedException in UncheckedInterruptedException
                // without that catch, undispatched HintsDescriptor won't be added back to the store and cleaned
                // up by HintsStore.delete in tests
                catch (UncheckedInterruptedException e)
                {
                    handleDispatchFailure(dispatcher, descriptor, address);
                    throw e;
                }
            }
        }

        private void handleDispatchFailure(HintsDispatcher dispatcher, HintsDescriptor descriptor, InetAddressAndPort address)
        {
            store.markDispatchOffset(descriptor, dispatcher.dispatchPosition());
            store.offerFirst(descriptor);
            logger.info("Finished hinted handoff of file {} to endpoint {}: {}, partially", descriptor.fileName(), address, hostId);
        }

        // for each hint in the hints file for a node that isn't part of the ring anymore, write RF hints for each replica
        private void convert(HintsDescriptor descriptor)
        {
            File file = descriptor.file(hintsDirectory);

            try (HintsReader reader = HintsReader.open(file, rateLimiter))
            {
                reader.forEach(page -> page.hintsIterator().forEachRemaining(HintsService.instance::writeForAllReplicas));
                store.delete(descriptor);
                store.cleanUp(descriptor);
                logger.info("Finished converting hints file {}", descriptor.fileName());
            }
        }
    }

    public boolean isPaused()
    {
        return isPaused.get();
    }

    public boolean hasScheduledDispatches()
    {
        return !scheduledDispatches.isEmpty();
    }
}
