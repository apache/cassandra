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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Supplier;

import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.RequestFailure;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Locator;
import org.apache.cassandra.metrics.TCMMetrics;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageDelivery;
import org.apache.cassandra.net.MessageFlag;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.RequestCallbackWithFailure;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.tcm.discovery.Discovery.DiscoveredNodes;
import org.apache.cassandra.tcm.log.Entry;
import org.apache.cassandra.tcm.log.LocalLog;
import org.apache.cassandra.tcm.log.LogState;
import org.apache.cassandra.tcm.membership.Location;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MonotonicClock;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.Promise;

import static org.apache.cassandra.exceptions.ExceptionCode.SERVER_ERROR;
import static org.apache.cassandra.net.NoPayload.noPayload;
import static org.apache.cassandra.tcm.ClusterMetadataService.State.REMOTE;

public final class RemoteProcessor implements Processor
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
    public Commit.Result commit(Entry.Id entryId, Transformation transform, Epoch lastKnown, Retry retryPolicy)
    {
        try
        {
            Commit.Result result = sendWithRetries(Verb.TCM_COMMIT_REQ,
                                                   new Commit(entryId, transform, lastKnown),
                                                   (Commit.Result res) -> {
                                                       if (res.isFailure() && !res.failure().rejected)
                                                           throw new IllegalArgumentException(res.failure().message);
                                                   },
                                                   new CandidateIterator(candidates(false)),
                                                   retryPolicy);

            log.append(result.logState());

            if (result.isSuccess())
            {
                Commit.Result.Success success = result.success();
                log.awaitAtLeast(success.epoch);
            }
            else
            {
                log.waitForHighestConsecutive();
            }

            return result;
        }
        catch (Exception e)
        {
            return Commit.Result.failed(SERVER_ERROR,
                                        e.getMessage() == null
                                        ? e.getClass().toString()
                                        : e.getMessage());
        }
    }

    private List<InetAddressAndPort> candidates(boolean allowDiscovery)
    {
        List<InetAddressAndPort> candidates = new ArrayList<>(log.metadata().fullCMSMembers());
        if (candidates.isEmpty())
            candidates.addAll(DatabaseDescriptor.getSeeds());
        // todo: should we add all other nodes, too?
        if (candidates.isEmpty() && allowDiscovery)
        {
            for (InetAddressAndPort discoveryNode : discoveryNodes.get())
            {
                if (!discoveryNode.equals(FBUtilities.getBroadcastAddressAndPort()))
                    candidates.add(discoveryNode);
            }
        }

        sortCandidates(candidates);

        return candidates;
    }

    private static void sortCandidates(List<InetAddressAndPort> candidates)
    {
        Config.CMSCommitMemberPreferencePolicy policy = DatabaseDescriptor.getCmsCommitMemberPreferencePolicy();
        sortCandidates(candidates, policy, DatabaseDescriptor.getLocator());
    }

    @VisibleForTesting
    static void sortCandidates(List<InetAddressAndPort> candidates,
                               Config.CMSCommitMemberPreferencePolicy policy,
                               Locator locator)
    {
        switch (policy)
        {
            case random:
                Collections.shuffle(candidates);
                break;
            case local_random:
                shuffleLocalDcFirstThenShuffleRest(candidates, locator);
                break;
            case deterministic:
                Collections.sort(candidates);
                break;
            case local_deterministic:
                sortLocalDcFirstThenByAddress(candidates, locator);
                break;
            default:
                throw new IllegalStateException(policy.toString());
        }
    }

    @VisibleForTesting
    static void shuffleLocalDcFirstThenShuffleRest(List<InetAddressAndPort> candidates, Locator locator)
    {
        Location local = locator.local();

        List<InetAddressAndPort> localDc = new ArrayList<>();
        List<InetAddressAndPort> remoteDc = new ArrayList<>();

        for (InetAddressAndPort ep : candidates)
        {
            if (local.sameDatacenter(locator.location(ep)))
                localDc.add(ep);
            else
                remoteDc.add(ep);
        }

        Collections.shuffle(localDc);
        Collections.shuffle(remoteDc);

        candidates.clear();
        candidates.addAll(localDc);
        candidates.addAll(remoteDc);
    }

    @VisibleForTesting
    static void sortLocalDcFirstThenByAddress(List<InetAddressAndPort> candidates, Locator locator)
    {
        Location local = locator.local();

        candidates.sort((a, b) -> {
            boolean aLocal = local.sameDatacenter(locator.location(a));
            boolean bLocal = local.sameDatacenter(locator.location(b));
            if (aLocal != bLocal)
                return aLocal ? -1 : 1;
            return a.compareTo(b);
        });
    }

    @Override
    public ClusterMetadata fetchLogAndWait(Epoch waitFor, Retry retryPolicy)
    {
        // Synchonous, non-debounced call if we are waiting for the highest epoch (without knowing/caring what it is).
        // Should be used sparingly.
        if (waitFor == null)
            return fetchLogAndWait(new CandidateIterator(candidates(true), false), log);

        Future<ClusterMetadata> cmFuture = null;
        try
        {
            Supplier<Future<ClusterMetadata>> fetchFunction = () -> fetchLogAndWaitInternal(new CandidateIterator(candidates(true), false),
                                                                                            log);

            cmFuture = EpochAwareDebounce.instance.getAsync(fetchFunction, waitFor);
            return cmFuture.get(retryPolicy.remainingNanos(), TimeUnit.NANOSECONDS);
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException("Can not replay during shutdown", e);
        }
        catch (ExecutionException | TimeoutException e)
        {
            throw new RuntimeException("Could not replay", e);
        }
    }

    public static ClusterMetadata fetchLogAndWait(CandidateIterator candidateIterator, LocalLog log)
    {
        try
        {
            return fetchLogAndWaitInternal(candidateIterator, log).await().get();
        }
        catch (InterruptedException | ExecutionException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static Future<ClusterMetadata> fetchLogAndWaitInternal(CandidateIterator candidates, LocalLog log)
    {
        try (Timer.Context ctx = TCMMetrics.instance.fetchCMSLogLatency.time())
        {
            Promise<LogState> remoteRequest = new AsyncPromise<>();
            Epoch currentEpoch = log.metadata().epoch;
            sendWithRetries(Verb.TCM_FETCH_CMS_LOG_REQ,
                            new FetchCMSLog(currentEpoch, ClusterMetadataService.state() == REMOTE),
                            remoteRequest,
                            candidates,
                            Retry.withNoTimeLimit(TCMMetrics.instance.fetchLogRetries));
            return remoteRequest.map((replay) -> {
                if (!replay.isEmpty())
                {
                    logger.info("Replay request returned replay data: {}", replay);
                    log.append(replay);
                    TCMMetrics.instance.cmsLogEntriesFetched(currentEpoch, replay.latestEpoch());
                }
                return log.waitForHighestConsecutive();
            });
        }
    }

    public static <REQ, RSP> RSP sendWithRetries(Verb verb, REQ request, Consumer<RSP> check, CandidateIterator candidates, Retry retry)
    {
        Promise<RSP> future = AsyncPromise.uncancellable();
        sendWithRetries(verb, request, check, future, candidates, retry);
        try
        {
            return future.get();
        }
        catch (InterruptedException | ExecutionException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static <REQ, RSP> void sendWithRetries(Verb verb, REQ request, Promise<RSP> future, CandidateIterator candidates, Retry retry)
    {
        sendWithRetries(verb, request, ignore_ -> {}, future, candidates, retry);
    }

    /**
     * Sends a request to given candidate nodes with retries, respecting the retry policy.
     *
     * If request handler expects node to be CMS, handles CMS discovery if a candidate node reports it's not a CMS member.
     */
    public static <REQ, RSP> void sendWithRetries(Verb verb, REQ request, Consumer<RSP> check, Promise<RSP> future, CandidateIterator candidates, Retry retry)
    {
        if (!candidates.hasNext())
        {
            future.setFailure(new MessageDelivery.NoMoreCandidatesException(String.format("Ran out of candidates while sending %s: %s", verb, candidates)));
            return;
        }
        else if (retry.hasExpired())
        {
            future.setFailure(new MessageDelivery.GivingUpException(retry.attempts(), String.format("Could not succeed sending %s to %s; policy %s gave up", verb, candidates, retry)));
            return;
        }

        InetAddressAndPort candidate = candidates.next();
        long msgExpiresAfterNanos = verb.expiresAfterNanos();
        // When committing a STARTUP the deadline on the Retry will be Long.MAX_VALUE i.e. never stop, retry
        // indefinitely. However, we do want to actually expire those specific messages reasonably quickly (i.e.
        // in the order of rpc_timeout) so that sending one to a dead or unreachable address doesn't clog things up
        // for cms_await_timeout (120s by default) and we can move on to the next candidate from the iterator
        if (verb == Verb.TCM_COMMIT_REQ && retry.deadlineNanos != Long.MAX_VALUE)
        {
            long cmsAwaitNanos = DatabaseDescriptor.getCmsAwaitTimeout().to(TimeUnit.NANOSECONDS);
            long remainingNanos = retry.remainingNanos();
            msgExpiresAfterNanos = Math.min(cmsAwaitNanos, remainingNanos);
            logger.debug("Sending {} to {}, per-message expiry {}ms (cmsAwait={}ms, remaining={}ms)",
                         verb, candidate,
                         TimeUnit.NANOSECONDS.toMillis(msgExpiresAfterNanos),
                         TimeUnit.NANOSECONDS.toMillis(cmsAwaitNanos),
                         TimeUnit.NANOSECONDS.toMillis(remainingNanos));
        }

        long msgExpiresAtNanos = MonotonicClock.Global.preciseTime.now() + msgExpiresAfterNanos;
        Message<REQ> msg = Message.outWithFlag(verb, request, MessageFlag.CALL_BACK_ON_FAILURE, msgExpiresAtNanos);
        MessagingService.instance().sendWithCallback(msg, candidate, new RequestCallbackWithFailure<RSP>()
        {
            @Override
            public void onFailure(InetAddressAndPort from, RequestFailure failure)
            {
                switch (failure.reason)
                {
                    case UNKNOWN:
                        candidates.timeout(candidate);
                        logger.warn("Got error from {}: {} when sending {}, retrying on {}", from, failure, verb, candidates);
                        break;
                    case TIMEOUT:
                        candidates.timeout(candidate);
                        logger.warn("Got error from {}: timeout when sending {}, retrying on {}", candidate, verb, candidates);
                        break;
                    case NOT_CMS:
                        logger.debug("{} is not a member of the CMS, querying it to discover current membership", from);
                        DiscoveredNodes cms = tryDiscover(from);
                        candidates.addCandidates(cms);
                        candidates.timeout(from);
                        logger.debug("Got CMS from {}: {}, retrying on: {}", from, cms, candidates);
                        break;
                    default:
                        // Unknown exception - add candidate back and retry
                        candidates.timeout(candidate);
                        logger.warn("Unexpected error ({}) sending {} to {}, retrying", failure, verb, candidate);
                }

                if (Thread.currentThread().isInterrupted()) // preserve interrupt status
                {
                    Thread.currentThread().interrupt();
                    future.setFailure(new InterruptedException("Interrupted while sending " + verb));
                }
                else
                {
                    long waitFor = retry.computeWait();
                    if (waitFor < 0)
                    {
                        // The retry strategy returns a negative wait to signal that its attempt budget is exhausted.
                        future.setFailure(new MessageDelivery.GivingUpException(retry.attempts(), String.format("Could not succeed sending %s to %s; policy %s exhausted attempts", verb, candidates, retry)));
                        return;
                    }
                    ScheduledExecutors.nonPeriodicTasks.schedule(() -> sendWithRetries(verb, request, check, future, candidates, retry), waitFor, TimeUnit.MILLISECONDS);
                }
            }

            @Override
            public void onResponse(Message<RSP> msg)
            {
                try
                {
                    check.accept(msg.payload);
                    future.setSuccess(msg.payload);
                }
                catch (Throwable t)
                {
                    onFailure(msg.from(), new RequestFailure(RequestFailureReason.UNKNOWN, t));
                }
            }
        });
    }

    private static DiscoveredNodes tryDiscover(InetAddressAndPort ep)
    {
        // TODO: there are no retries here
        Promise<DiscoveredNodes> promise = new AsyncPromise<>();
        MessagingService.instance().sendWithCallback(Message.out(Verb.TCM_DISCOVER_REQ, noPayload), ep, new RequestCallbackWithFailure<DiscoveredNodes>()
        {
            @Override
            public void onResponse(Message<DiscoveredNodes> msg)
            {
                promise.setSuccess(msg.payload);
            }

            @Override
            public void onFailure(InetAddressAndPort from, RequestFailure failureReason)
            {
                // "success" - this lets us just try the next one in cmsIter
                promise.setSuccess(new DiscoveredNodes(Collections.emptySet(), DiscoveredNodes.Kind.KNOWN_PEERS));
            }
        });
        try
        {
            return promise.get(DatabaseDescriptor.getCmsAwaitTimeout().to(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
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
            this.candidates = new ConcurrentLinkedDeque<>(initialContacts);
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
