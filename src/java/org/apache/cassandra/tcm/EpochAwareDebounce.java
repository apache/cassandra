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

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ExecutorFactory;
import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.concurrent.Shutdownable;
import org.apache.cassandra.tcm.log.LogState;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.Promise;

/**
 * When debouncing from a replica we know exactly which epoch we need, so to avoid retries we
 * keep track of which epoch we are currently debouncing, and if a request for a newer epoch
 * comes in, we create a new future. If a request for a newer epoch comes in, we simply
 * swap out the current future reference for a new one which is requesting the newer epoch.
 */
public class EpochAwareDebounce<T> implements Shutdownable
{
    private static final Logger logger = LoggerFactory.getLogger(EpochAwareDebounce.class);
    public static final EpochAwareDebounce instance = new EpochAwareDebounce();
    private final AtomicReference<EpochAwareAsyncPromise> currentFuture = new AtomicReference<>();
    private final ExecutorPlus executor;
    private final List<Promise<LogState>> inflightRequests = new CopyOnWriteArrayList<>();

    private EpochAwareDebounce()
    {
        // 2 threads since we might start a new debounce for a newer epoch while the old one is executing
        this.executor = ExecutorFactory.Global.executorFactory().pooled("debounce", 2);
    }

    /**
     * Deduplicate requests to catch up log state based on the desired epoch. Callers supply a target epoch and
     * a function obtain the ClusterMetadata that corresponds with it. It is expected that this function will make rpc
     * calls to peers, retrieving a LogState which can be applied locally to produce the necessary {@code
     * ClusterMetadata}. The function takes a {@code Promise<LogState>} as input, with the expectation that this
     * specific instance will be used to provide blocking behaviour when making the rpc calls that fetch the {@code
     * LogState}. These promises are memoized in order to cancel them when {@link #shutdownAndWait(long, TimeUnit)} is
     * called. This causes the fetch function to stop waiting on any in flight {@code LogState} requests and prevents
     * shutdown from being blocked.
     *
     * @param  fetchFunction executes the request for LogState. It's expected that this popluates fetchResult with the
     *                       successful result.
     * @param epoch the desired epoch
     * @return
     */
    public Future<ClusterMetadata> getAsync(Function<Promise<LogState>, ClusterMetadata> fetchFunction,
                                            Epoch epoch)
    {
        while (true)
        {
            EpochAwareAsyncPromise running = currentFuture.get();
            if (running != null && !running.isDone() && running.epoch.isEqualOrAfter(epoch))
                return running;

            Promise<LogState> fetchResult = new AsyncPromise<>();

            EpochAwareAsyncPromise promise = new EpochAwareAsyncPromise(epoch);
            if (currentFuture.compareAndSet(running, promise))
            {
                fetchResult.addCallback((logState, error) -> {
                    logger.debug("Removing future remotely requesting epoch {} from in flight list", epoch);
                    inflightRequests.remove(fetchResult);
                });
                inflightRequests.add(fetchResult);

                executor.submit(() -> {
                    try
                    {
                        promise.setSuccess(fetchFunction.apply(fetchResult));
                    }
                    catch (Throwable t)
                    {
                        fetchResult.cancel(true);
                        inflightRequests.remove(fetchResult);
                        promise.setFailure(t);
                    }
                });
                return promise;
            }
        }
    }

    @Override
    public boolean isTerminated()
    {
        return executor.isTerminated();
    }

    @Override
    public boolean isShutdown()
    {
        return executor.isShutdown();
    }

    @Override
    public void shutdown()
    {
        executor.shutdown();
    }

    @Override
    public Object shutdownNow()
    {
        return executor.shutdownNow();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit units) throws InterruptedException
    {
        return executor.awaitTermination(timeout, units);
    }

    private static class EpochAwareAsyncPromise<T> extends AsyncPromise<T>
    {
        private final Epoch epoch;
        public EpochAwareAsyncPromise(Epoch epoch)
        {
            this.epoch = epoch;
        }
    }
}
