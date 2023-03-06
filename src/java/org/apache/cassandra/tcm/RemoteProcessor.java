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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.RequestCallbackWithFailure;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.tcm.Discovery.DiscoveredNodes;
import org.apache.cassandra.tcm.log.Entry;
import org.apache.cassandra.tcm.log.LocalLog;
import org.apache.cassandra.tcm.log.LogState;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Promise;

import static org.apache.cassandra.net.NoPayload.noPayload;
import static org.apache.cassandra.tcm.ClusterMetadataService.State.REMOTE;

public final class RemoteProcessor implements ClusterMetadataService.Processor
{
    private static final Logger logger = LoggerFactory.getLogger(RemoteProcessor.class);
    private final Supplier<Collection<InetAddressAndPort>> discoveryNodes;
    private final LocalLog log;

    RemoteProcessor(LocalLog log, Supplier<Collection<InetAddressAndPort>> discoveryNodes)
    {
        this.log = log;
        this.discoveryNodes = discoveryNodes;
    }

    @Override
    @SuppressWarnings("resource")
    public Commit.Result commit(Entry.Id entryId, Transformation transform, Epoch lastKnown)
    {
        // Replay everything in-flight before attempting a commit
        Epoch highestConsecutive = log.waitForHighestConsecutive().epoch;

        try
        {
            Commit.Result result = sendWithCallback(Verb.TCM_COMMIT_REQ,
                                                    new Commit(entryId, transform, highestConsecutive),
                                                    new CandidateIterator(candidates(false)),
                                                    new Retry.Backoff());

            if (result.isSuccess())
                log.append(result.success().replication.entries());

            return result;
        }
        catch (Exception e)
        {
            return new Commit.Result.Failure(e.getMessage() == null ? e.getClass().toString() : e.getMessage(), false);
        }
    }

    private List<InetAddressAndPort> candidates(boolean allowDiscovery)
    {
        List<InetAddressAndPort> candidates = new ArrayList<>(log.metadata().fullCMSMembers());
        if (candidates.isEmpty())
            candidates.addAll(DatabaseDescriptor.getSeeds());

        if (candidates.isEmpty() && allowDiscovery)
        {
            for (InetAddressAndPort discoveryNode : discoveryNodes.get())
            {
                if (!discoveryNode.equals(FBUtilities.getBroadcastAddressAndPort()))
                    candidates.add(discoveryNode);
            }
        }

        Collections.shuffle(candidates);

        return candidates;
    }

    @Override
    @SuppressWarnings("resource")
    public ClusterMetadata replayAndWait()
    {
        Epoch lastConsecutive = log.replayPersisted();

        LogState replay = sendWithCallback(Verb.TCM_REPLAY_REQ,
                                           new Replay(lastConsecutive, ClusterMetadataService.state() == REMOTE),
                                           new CandidateIterator(candidates(true), false),
                                           new Retry.Backoff());
        if (!replay.isEmpty())
        {
            logger.info("Replay request returned replay data: {}", replay);
            log.append(replay);
        }

        return log.waitForHighestConsecutive();
    }

    public static <REQ, RSP> RSP sendWithCallback(Verb verb, REQ request, CandidateIterator candidates, Retry.Backoff backoff)
    {
        if (!candidates.hasNext())
            throw new IllegalStateException("Could not find a CMS instance " + candidates);

        while (!backoff.reachedMax())
        {
            AsyncPromise<RSP> promise = new AsyncPromise<>();

            MessagingService.instance().sendWithCallback(Message.out(verb, request),
                                                         candidates.next(),
                                                         new RequestCallbackWithFailure<RSP>()
            {
                @Override
                public void onResponse(Message<RSP> msg)
                {
                    promise.trySuccess(msg.payload);
                }

                @Override
                public void onFailure(InetAddressAndPort from, RequestFailureReason reason)
                {
                    if (reason == RequestFailureReason.NOT_CMS)
                    {
                        logger.debug("{} is not a member of the CMS, querying it to discover current membership", from);
                        candidates.notCms(from);
                        DiscoveredNodes cms = tryDiscover(from);
                        candidates.addCandidates(cms);
                        logger.debug("Got CMS from {}: {}, retrying on: {}", from, cms, candidates);
                    }
                    else
                    {
                        candidates.timeout(from);
                        logger.error("Got error from {}: {} when sending {}, retrying on {}", from, reason, verb, candidates);
                    }

                    promise.tryFailure(null);
                }
            });

            try
            {
                return promise.get(DatabaseDescriptor.getCmsAwaitTimeout().to(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
            }
            catch (InterruptedException | ExecutionException | TimeoutException e)
            {
                // retry
                backoff.maybeSleep();
            }
        }
        throw new IllegalStateException(String.format("Could not succeed sending %s to %s after %d tries", verb, candidates, backoff.maxTries));
    }

    private static DiscoveredNodes tryDiscover(InetAddressAndPort ep)
    {
        Promise<DiscoveredNodes> promise = new AsyncPromise<>();
        MessagingService.instance().sendWithCallback(Message.out(Verb.TCM_DISCOVER_REQ, noPayload), ep, new RequestCallbackWithFailure<DiscoveredNodes>()
        {
            @Override
            public void onResponse(Message<DiscoveredNodes> msg)
            {
                promise.setSuccess(msg.payload);
            }

            @Override
            public void onFailure(InetAddressAndPort from, RequestFailureReason failureReason)
            {
                // "success" - this lets us just try the next one in cmsIter
                promise.setSuccess(new DiscoveredNodes(Collections.emptySet(), DiscoveredNodes.Kind.KNOWN_PEERS));
            }
        });
        try
        {
            return promise.get(10, TimeUnit.SECONDS);
        }
        catch (Exception e)
        {
            logger.warn("Could not discover CMS from " + ep, e);
        }
        return new DiscoveredNodes(Collections.emptySet(), DiscoveredNodes.Kind.KNOWN_PEERS);
    }

    public static class CandidateIterator extends AbstractIterator<InetAddressAndPort>
    {
        private final Deque<InetAddressAndPort> candidates;
        private final boolean checkLive;

        @SuppressWarnings("resource")
        public CandidateIterator(Collection<InetAddressAndPort> initialContacts)
        {
            this(initialContacts, true);
        }

        @SuppressWarnings("resource")
        public CandidateIterator(Collection<InetAddressAndPort> initialContacts, boolean checkLive)
        {
            this.candidates = new ArrayDeque<>(initialContacts);
            this.checkLive = checkLive;
        }

        /**
         * called when we get a response from LOG_DISCOVER_CMS_REQ
         *
         * @param discoveredNodes
         */
        public void addCandidates(DiscoveredNodes discoveredNodes)
        {
            if (discoveredNodes.kind() == DiscoveredNodes.Kind.CMS_ONLY)
                discoveredNodes.nodes().forEach(candidates::addFirst);
            else
                discoveredNodes.nodes().forEach(candidates::addLast);
        }

        public void notCms(InetAddressAndPort resp)
        {
            candidates.addLast(resp);
        }

        public void timeout(InetAddressAndPort timedOut)
        {
            candidates.addLast(timedOut);
        }

        public String toString()
        {
            return "CandidateIterator{" +
                   "candidates=" + candidates +
                   ", checkLive=" + checkLive +
                   '}';
        }

        public InetAddressAndPort peekLast()
        {
            return candidates.peekLast();
        }

        @Override
        protected InetAddressAndPort computeNext()
        {
            boolean checkLive = this.checkLive;
            InetAddressAndPort first = null;

            while (!candidates.isEmpty())
            {
                InetAddressAndPort ep = candidates.pop();

                // If we've cycled through all candidates, disable liveness check
                if (first == null)
                    first = ep;
                else if (first.equals(ep))
                    checkLive = false;

                if (checkLive && !FailureDetector.instance.isAlive(ep))
                {
                    if (candidates.isEmpty())
                        return ep;
                    else
                    {
                        candidates.addLast(ep);
                        continue;
                    }
                }
                return ep;
            }
            return endOfData();
        }
    }
}
