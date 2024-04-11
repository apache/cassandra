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

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.locator.EndpointsForRange;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.metrics.TCMMetrics;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageDelivery;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.RequestCallbackWithFailure;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.DistributedMetadataLogKeyspace;
import org.apache.cassandra.tcm.log.Entry;
import org.apache.cassandra.tcm.log.LocalLog;
import org.apache.cassandra.tcm.log.LogState;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Promise;

import static org.apache.cassandra.schema.DistributedMetadataLogKeyspace.tryCommit;

public class PaxosBackedProcessor extends AbstractLocalProcessor
{
    private static final Logger logger = LoggerFactory.getLogger(PaxosBackedProcessor.class);

    public PaxosBackedProcessor(LocalLog log)
    {
        super(log);
    }

    @Override
    protected boolean tryCommitOne(Entry.Id entryId, Transformation transform, Epoch previousEpoch, Epoch nextEpoch)
    {
        return tryCommit(entryId, transform, previousEpoch, nextEpoch);
    }

    @Override
    public ClusterMetadata fetchLogAndWait(Epoch waitFor, Retry.Deadline retryPolicy)
    {
        ClusterMetadata metadata = log.waitForHighestConsecutive();

        // We can perform a local-only read without going through paxos subsystem in case of a single CMS node.
        if (metadata.fullCMSMembers().size() > 1)
        {
            try
            {
                // Attempt to perform a consistent fetch
                log.append(DistributedMetadataLogKeyspace.getLogState(metadata.epoch, true));
                return log.waitForHighestConsecutive();
            }
            catch (Throwable t)
            {
                JVMStabilityInspector.inspectThrowable(t);
                TCMMetrics.instance.fetchCMSLogConsistencyDowngrade.mark();
                logger.warn("Could not perform consistent fetch, downgrading to fetching from CMS peers.", t);
            }
        }

        EndpointsForRange replicas = metadata.fullCMSMembersAsReplicas();

        // We prefer to always perform a consistent fetch (i.e. Paxos read of the distributed log state table).
        // However, in some cases (specifically, during CMS membership changes) this may not be possible, as Paxos
        // relies on matching Participants, and there might be a mismatch during the membership change. In such
        // case, we allow inconsistent fetch. In other words, replay from local log of the majority of the CMS replicas.
        int blockFor = replicas.size() == 1 ? 1 : (replicas.size() / 2) + 1;

        Set<InetAddressAndPort> collected = new HashSet<>(blockFor);
        Set<FetchLogRequest> requests = new HashSet<>();
        AtomicReference<Epoch> highestSeen = new AtomicReference<>(metadata.epoch);

        for (Replica peer : replicas)
            requests.add(new FetchLogRequest(peer, MessagingService.instance(), metadata.epoch));

        while (!retryPolicy.reachedMax())
        {
            Iterator<FetchLogRequest> iter = requests.iterator();
            boolean hasRequestToSelf = false;
            while (iter.hasNext())
            {
                FetchLogRequest request = iter.next();
                if (request.to.isSelf())
                {
                    hasRequestToSelf = true;
                    iter.remove();
                }
                else
                {
                    request.retry();
                }
            }

            // Fire off a blocking request to self only after dispatching requests to other participants
            if (hasRequestToSelf)
            {
                log.append(DistributedMetadataLogKeyspace.getLogState(metadata.epoch, false));
                collected.add(FBUtilities.getBroadcastAddressAndPort());
            }

            iter = requests.iterator();
            long nextTimeout = Math.min(retryPolicy.deadlineNanos, Clock.Global.nanoTime() + DatabaseDescriptor.getRpcTimeout(TimeUnit.NANOSECONDS));
            while (iter.hasNext())
            {
                FetchLogRequest request = iter.next();
                if (request.condition.awaitUninterruptibly(Math.max(0, nextTimeout - Clock.Global.nanoTime()), TimeUnit.NANOSECONDS) &&
                    request.condition.isSuccess())
                {
                    collected.add(request.to.endpoint());
                    LogState logState = unwrap(request.condition);
                    log.append(logState);
                    highestSeen.getAndUpdate(o -> {
                        if (o == null || logState.latestEpoch().isAfter(o))
                            return logState.latestEpoch();
                        return o;
                    });
                    iter.remove();
                }
            }

            if (collected.size() < blockFor)
            {
                retryPolicy.maybeSleep();
                continue;
            }

            Epoch highest = highestSeen.get();
            TCMMetrics.instance.cmsLogEntriesFetched(metadata.epoch, highest);
            assert waitFor == null || highest.isEqualOrAfter(waitFor) : String.format("%s should have been higher than waited for epoch %s", highestSeen, waitFor);
            return log.waitForHighestConsecutive();
        }

        TCMMetrics.instance.cmsLogEntriesFetched(metadata.epoch, highestSeen.get());
        throw new ReadTimeoutException(ConsistencyLevel.QUORUM, blockFor - collected.size(), blockFor, false);
    }

    private static <T> T unwrap(Promise<T> promise)
    {
        if (!promise.isDone() || !promise.isSuccess())
            throw new IllegalStateException("Can only unwrap an already done promise.");
        try
        {
            return promise.get();
        }
        catch (InterruptedException | ExecutionException e)
        {
            throw new IllegalStateException("Promise shoulde not have thrown", e);
        }
    }

    private static class FetchLogRequest implements RequestCallbackWithFailure<LogState>
    {
        private AsyncPromise<LogState> condition = null;
        private final Replica to;
        private final MessageDelivery messagingService;
        private final FetchCMSLog request;

        public FetchLogRequest(Replica to, MessageDelivery messagingService, Epoch lowerBound)
        {
            this.to = to;
            this.messagingService = messagingService;
            this.request = new FetchCMSLog(lowerBound, false);
        }

        @Override
        public void onResponse(Message<LogState> msg)
        {
            condition.trySuccess(msg.payload);
        }

        @Override
        public void onFailure(InetAddressAndPort from, RequestFailureReason failureReason)
        {
            logger.debug("Error response from {} with {}", from, failureReason);
            condition.tryFailure(new TimeoutException(failureReason.toString()));
        }

        public void retry()
        {
            condition = new AsyncPromise<>();
            messagingService.sendWithCallback(Message.out(Verb.TCM_FETCH_CMS_LOG_REQ, request), to.endpoint(), this);
        }
    }


}