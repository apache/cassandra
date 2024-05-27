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

import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Timer;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.metrics.TCMMetrics;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.tcm.log.LocalLog;
import org.apache.cassandra.tcm.log.LogState;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.Promise;

public class PeerLogFetcher
{
    private static final Logger logger = LoggerFactory.getLogger(RemoteProcessor.class);
    private final LocalLog log;

    public PeerLogFetcher(LocalLog log)
    {
        this.log = log;
    }

    /**
     * fetch log entries from the given remote, we have already seen a message from this replica with epoch awaitAtleast.
     */
    public ClusterMetadata fetchLogEntriesAndWait(InetAddressAndPort remote, Epoch awaitAtleast)
    {
        ClusterMetadata metadata = ClusterMetadata.current();
        if (metadata.epoch.isEqualOrAfter(awaitAtleast))
            return metadata;

        try
        {
            return asyncFetchLog(remote, awaitAtleast).get(DatabaseDescriptor.getRpcTimeout(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
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
        return EpochAwareDebounce.instance.getAsync(() -> fetchLogEntriesAndWaitInternal(remote, awaitAtleast), awaitAtleast);
    }

    private Future<ClusterMetadata> fetchLogEntriesAndWaitInternal(InetAddressAndPort remote, Epoch awaitAtleast)
    {
        Epoch before = ClusterMetadata.current().epoch;
        if (before.isEqualOrAfter(awaitAtleast))
        {
            Promise<ClusterMetadata> res = new AsyncPromise<>();
            res.setSuccess(ClusterMetadata.current());
            return res;
        }

        Promise<LogState> fetchRes = new AsyncPromise<>();
        logger.info("Fetching log from {}, at least {}", remote, awaitAtleast);
        try (Timer.Context ctx = TCMMetrics.instance.fetchPeerLogLatency.time())
        {
            RemoteProcessor.sendWithCallbackAsync(fetchRes,
                                                  Verb.TCM_FETCH_PEER_LOG_REQ,
                                                  new FetchPeerLog(before),
                                                  new RemoteProcessor.CandidateIterator(Collections.singletonList(remote), false),
                                                  Retry.Deadline.after(DatabaseDescriptor.getCmsAwaitTimeout().to(TimeUnit.NANOSECONDS),
                                                                       new Retry.Jitter(TCMMetrics.instance.fetchLogRetries)));

            return fetchRes.map((logState) -> {
                log.append(logState);
                ClusterMetadata fetched = log.waitForHighestConsecutive();
                if (fetched.epoch.isEqualOrAfter(awaitAtleast))
                {
                    TCMMetrics.instance.peerLogEntriesFetched(before, logState.latestEpoch());
                    return fetched;
                }
                else
                {
                    throw new IllegalStateException(String.format("Queried for epoch %s, but could not catch up", awaitAtleast));
                }
            });

        }
        catch (Throwable t)
        {
            fetchRes.cancel(true);
            JVMStabilityInspector.inspectThrowable(t);

            logger.warn("Unable to fetch log entries from " + remote, t);
            Promise<ClusterMetadata> res = new AsyncPromise<>();
            res.setFailure(new IllegalStateException("Unable to fetch log entries from " + remote, t));
            return res;
        }
    }
}
