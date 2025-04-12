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

package org.apache.cassandra.tcm;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.cassandra.utils.Closeable;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.cassandra.config.DatabaseDescriptor.getCmsAwaitTimeout;
import static org.apache.cassandra.config.DatabaseDescriptor.getEpochAwareDebounceInFlightTrackerMaxSize;

/**
 * When debouncing from a replica we know exactly which epoch we need, so to avoid retries we
 * keep track of which epoch we are currently debouncing, and if a request for a newer epoch
 * comes in, we create a new future. If a request for a newer epoch comes in, we simply
 * swap out the current future reference for a new one which is requesting the newer epoch.
 */
public class EpochAwareDebounce implements Closeable
{
    public static final EpochAwareDebounce instance = new EpochAwareDebounce();

    private final AtomicReference<EpochAwareFuture> currentFuture = new AtomicReference<>();
    private final Cache<Epoch, Future<ClusterMetadata>> inflight = Caffeine.newBuilder()
                                                                           .maximumSize(getEpochAwareDebounceInFlightTrackerMaxSize())
                                                                           .expireAfterWrite(getCmsAwaitTimeout().to(MILLISECONDS),
                                                                                             MILLISECONDS)
                                                                           .build();

    private EpochAwareDebounce()
    {
    }

    /**
     * Deduplicate requests to catch up log state based on the desired epoch. Callers supply a target epoch and
     * a function obtain the ClusterMetadata that corresponds with it. It is expected that this function will make rpc
     * calls to peers, retrieving a LogState which can be applied locally to produce the necessary {@code
     * ClusterMetadata}. 
     *
     * @param fetchFunction supplies the future that, when dereferenced, will yield metadata for the desired epoch
     * @param epoch the desired epoch
     * @return
     */
    public Future<ClusterMetadata> getAsync(Supplier<Future<ClusterMetadata>> fetchFunction, Epoch epoch)
    {
        while (true)
        {
            EpochAwareFuture running = currentFuture.get();
            // Someone else is about to install a new future
            if (running == SENTINEL)
                continue;

            // The inflight future is sufficient, or we are shutting down the debouncer
            if ((running != null && !running.future.isDone() && running.epoch.isEqualOrAfter(epoch)) || running == CLOSED)
                return running.future;

            if (currentFuture.compareAndSet(running, SENTINEL))
            {
                Future<ClusterMetadata> cmFuture = fetchFunction.get();
                cmFuture.addListener(() -> inflight.asMap().remove(epoch));
                EpochAwareFuture promise = new EpochAwareFuture(epoch, cmFuture);
                EpochAwareFuture current = currentFuture.compareAndExchange(SENTINEL, promise);
                // we have to explicitly check here as close() unconditionally sets currentFuture
                if (current == CLOSED)
                    return CLOSED.future;

                inflight.put(epoch, cmFuture);
                return cmFuture;
            }
        }
    }

    private static final EpochAwareFuture SENTINEL = new EpochAwareFuture(Epoch.EMPTY, null);
    private static final EpochAwareFuture CLOSED = new EpochAwareFuture(Epoch.EMPTY, ImmediateFuture.cancelled());

    @Override
    public void close()
    {
        EpochAwareFuture future = currentFuture.getAndSet(CLOSED);
        if (future != null && future != SENTINEL && future != CLOSED)
            future.future.cancel(true);
        for (Future<ClusterMetadata> toCancel : inflight.asMap().values())
            toCancel.cancel(true);
    }

    public long inflightTrackerSize()
    {
        return inflight.estimatedSize();
    }

    private static class EpochAwareFuture
    {
        private final Epoch epoch;
        private final Future<ClusterMetadata> future;
        public EpochAwareFuture(Epoch epoch, Future<ClusterMetadata> future)
        {
            this.epoch = epoch;
            this.future = future;
        }
    }
}
