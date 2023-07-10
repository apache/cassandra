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

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Timer;
import org.apache.cassandra.concurrent.ExecutorFactory;
import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.metrics.TCMMetrics;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.RequestCallbackWithFailure;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.tcm.log.LocalLog;
import org.apache.cassandra.tcm.log.LogState;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Future;

public class PeerLogFetcher
{
    private static final Logger logger = LoggerFactory.getLogger(RemoteProcessor.class);

    private final LocalLog log;
    private final EpochAwareDebounce<ClusterMetadata> epochAwareFetchDebounce = new EpochAwareDebounce<>();

    public PeerLogFetcher(LocalLog log)
    {
        this.log = log;
    }

    /**
     * fetch log entries from the given remote, we have already seen a message from this replica with epoch awaitAtleast.
     *
     * If querying the replica times out we fall back to querying the CMS, and if that also times out we throw
     * IllegalStateException
     */
    public ClusterMetadata fetchLogEntriesAndWait(InetAddressAndPort remote, Epoch awaitAtleast)
    {
        ClusterMetadata metadata = ClusterMetadata.current();
        if (metadata.epoch.isEqualOrAfter(awaitAtleast))
            return metadata;

        try
        {
            return asyncFetchLog(remote, awaitAtleast).get(DatabaseDescriptor.getCmsAwaitTimeout().to(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException("Can not fetch log entries during shutdown", e);
        }
        catch (ExecutionException | TimeoutException e)
        {
            logger.warn("Could not fetch log entries from peer, remote = {}, await = {}", remote, awaitAtleast);
        }
        return metadata;
    }

    public Future<ClusterMetadata> asyncFetchLog(InetAddressAndPort remote, Epoch awaitAtleast)
    {

        return epochAwareFetchDebounce.getAsync(() -> fetchLogEntriesAndWaitInternal(remote, awaitAtleast), awaitAtleast);
    }

    private ClusterMetadata fetchLogEntriesAndWaitInternal(InetAddressAndPort remote, Epoch awaitAtleast)
    {
        try (Timer.Context ctx = TCMMetrics.instance.fetchPeerLogLatency.time())
        {
            Epoch before = ClusterMetadata.current().epoch;
            if (before.isEqualOrAfter(awaitAtleast))
                return ClusterMetadata.current();
            FetchPeerLog fetchLogReq = new FetchPeerLog(before);
            AsyncPromise<LogState> promise = new AsyncPromise<>();
            MessagingService.instance().sendWithCallback(Message.out(Verb.TCM_FETCH_PEER_LOG_REQ, fetchLogReq), remote, new RequestCallbackWithFailure<LogState>()
            {
                @Override
                public void onFailure(InetAddressAndPort from, RequestFailureReason failureReason)
                {
                    promise.setFailure(new RuntimeException(String.format("Unable to fetch log entries from %s: %s", from, failureReason)));
                }

                @Override
                public void onResponse(Message<LogState> msg)
                {
                    logger.debug("Fetched log entries up to {} from {}", msg.payload.latestEpoch(), remote);
                    promise.setSuccess(msg.payload);
                }
            });

            try
            {
                LogState logState = promise.get(DatabaseDescriptor.getReadRpcTimeout(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
                log.append(logState);
                ClusterMetadata fetched = log.waitForHighestConsecutive();
                if (fetched.epoch.isEqualOrAfter(awaitAtleast))
                {
                    TCMMetrics.instance.peerLogEntriesFetched(before, logState.latestEpoch());
                    return fetched;
                }
            }
            catch (InterruptedException | ExecutionException | TimeoutException e)
            {
                logger.warn("Unable to fetch log entries from " + remote, e);
            }
            return ClusterMetadata.current();
        }
    }

    /**
     * When debouncing from a replica we know exactly which epoch we need, so to avoid retries we
     * keep track of which epoch we are currently debouncing, and if a request for a newer epoch
     * comes in, we create a new future. If a request for a newer epoch comes in, we simply
     * swap out the current future reference for a new one which is requesting the newer epoch.
     */
    public static class EpochAwareDebounce<T>
    {
        private final AtomicReference<EpochAwareAsyncPromise<T>> currentFuture = new AtomicReference<>();
        private final ExecutorPlus executor;

        public EpochAwareDebounce()
        {
            // 2 threads since we might start a new debounce for a newer epoch while the old one is executing
            this.executor = ExecutorFactory.Global.executorFactory().pooled("debounce", 2);
        }

        public Future<T> getAsync(Callable<T> get, Epoch epoch)
        {
            while (true)
            {
                EpochAwareAsyncPromise<T> running = currentFuture.get();
                if (running != null && !running.isDone() && running.epoch.isEqualOrAfter(epoch))
                    return running;

                EpochAwareAsyncPromise<T> promise = new EpochAwareAsyncPromise<>(epoch);
                if (currentFuture.compareAndSet(running, promise))
                {
                    executor.submit(() -> {
                        try
                        {
                            promise.setSuccess(get.call());
                        }
                        catch (Throwable t)
                        {
                            promise.setFailure(t);
                        }
                    });
                    return promise;
                }
            }
        }
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
