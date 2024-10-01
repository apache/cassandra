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

package org.apache.cassandra.tcm.sequences;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Timer;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
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
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.Retry;
import org.apache.cassandra.tcm.membership.Directory;
import org.apache.cassandra.tcm.membership.Location;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.concurrent.AsyncPromise;

/**
 * ProgressBarrier is responsible for ensuring that epoch visibility plays together with quorum consistency.
 *
 * When bootstrapping a node, streaming will not start until (by default) EACH_QUORUM of nodes has seen the epoch that
 * adds the joining jode to the write replica set.
 *
 * Each subsequent step will be gated by waiting for (by default) EACH_QUORUM of nodes in or proposed to be in the replica set
 * to see the previous epoch.
 *
 * If number of nodes in the cluster is smaller than the number of nodes specified in the replication factor, we will
 * collect only n/2 + 1 nodes to avoid availability issues.
 */
public class ProgressBarrier
{
    private static final Logger logger = LoggerFactory.getLogger(ProgressBarrier.class);
    private static final ConsistencyLevel MIN_CL = DatabaseDescriptor.getProgressBarrierMinConsistencyLevel();
    private static final ConsistencyLevel DEFAULT_CL = DatabaseDescriptor.getProgressBarrierDefaultConsistencyLevel();
    private static final long TIMEOUT_MILLIS = DatabaseDescriptor.getProgressBarrierTimeout(TimeUnit.MILLISECONDS);
    private static final long BACKOFF_MILLIS = DatabaseDescriptor.getProgressBarrierBackoff(TimeUnit.MILLISECONDS);

    public final Epoch waitFor;
    // Location of the affected node; used for LOCAL_QUORUM
    public final Location location;
    public final LockedRanges.AffectedRanges affectedRanges;
    public final MessageDelivery messagingService;
    public final Predicate<InetAddressAndPort> filter;

    public ProgressBarrier(Epoch waitFor, Location location, LockedRanges.AffectedRanges affectedRanges)
    {
        this(waitFor, location, affectedRanges, MessagingService.instance(), (t) -> true);
    }

    public ProgressBarrier(Epoch waitFor, Location location, LockedRanges.AffectedRanges affectedRanges, Predicate<InetAddressAndPort> filter)
    {
        this(waitFor, location, affectedRanges, MessagingService.instance(), filter);
    }

    private ProgressBarrier(Epoch waitFor, Location location, LockedRanges.AffectedRanges affectedRanges, MessageDelivery messagingService, Predicate<InetAddressAndPort> filter)
    {
        this.waitFor = waitFor;
        this.affectedRanges = affectedRanges;
        this.location = location;
        this.messagingService = messagingService;
        this.filter = filter;
    }

    public static ProgressBarrier immediate()
    {
        return new ProgressBarrier(Epoch.EMPTY, null, LockedRanges.AffectedRanges.EMPTY);
    }

    @VisibleForTesting
    public ProgressBarrier withMessagingService(MessageDelivery messagingService)
    {
        return new ProgressBarrier(waitFor, location, affectedRanges, messagingService, filter);
    }

    public boolean await()
    {
        try (Timer.Context ctx = TCMMetrics.instance.progressBarrierLatency.time())
        {
            if (waitFor.is(Epoch.EMPTY))
                return true;

            ConsistencyLevel currentCL = DEFAULT_CL;
            while (!await(currentCL, ClusterMetadata.current()))
            {
                if (currentCL == MIN_CL)
                    return false;

                ConsistencyLevel prev = currentCL;
                currentCL = relaxConsistency(prev);
                logger.info(String.format("Could not collect epoch acknowledgements within %dms for %s. Falling back to %s.", TIMEOUT_MILLIS, prev, currentCL));
            }
            return true;
        }
    }

    @VisibleForTesting
    public boolean await(ConsistencyLevel cl, ClusterMetadata metadata)
    {
        if (waitFor.is(Epoch.EMPTY))
            return true;

        int maxWaitFor = 0;
        Map<ReplicationParams, Set<Range<Token>>> affectedRangesMap = affectedRanges.asMap();
        List<WaitFor> waiters = new ArrayList<>(affectedRangesMap.size());

        Set<InetAddressAndPort> superset = new HashSet<>();

        for (Map.Entry<ReplicationParams, Set<Range<Token>>> e : affectedRangesMap.entrySet())
        {
            ReplicationParams params = e.getKey();
            Set<Range<Token>> ranges = e.getValue();
            for (Range<Token> range : ranges)
            {
                EndpointsForRange writes = metadata.placements.get(params).writes.matchRange(range).get().filter(r -> filter.test(r.endpoint()));
                EndpointsForRange reads = metadata.placements.get(params).reads.matchRange(range).get().filter(r -> filter.test(r.endpoint()));
                reads.stream().map(Replica::endpoint).forEach(superset::add);
                writes.stream().map(Replica::endpoint).forEach(superset::add);

                WaitFor waitFor;
                switch (cl)
                {
                    case ALL:
                        waitFor = new WaitForAll(writes, reads);
                        break;
                    case EACH_QUORUM:
                        waitFor = new WaitForEachQuorum(writes, reads, metadata.directory);
                        break;
                    case LOCAL_QUORUM:
                        waitFor = new WaitForLocalQuorum(writes, reads, metadata.directory, location);
                        break;
                    case QUORUM:
                        waitFor = new WaitForQuorum(writes, reads);
                        break;
                    case ONE:
                        waitFor = new WaitForOne(writes, reads);
                        break;
                    case NODE_LOCAL:
                        waitFor = new WaitForNone();
                        break;
                    default:
                        throw new IllegalArgumentException("Progress barrier only supports ALL, EACH_QUORUM, LOCAL_QUORUM, QUORUM, ONE and NODE_LOCAL, but not " + cl);
                }

                maxWaitFor = Math.max(waitFor.waitFor(), maxWaitFor);
                waiters.add(waitFor);
            }
        }

        Set<InetAddressAndPort> collected = new HashSet<>();
        Set<WatermarkRequest> requests = new HashSet<>();
        for (InetAddressAndPort peer : superset)
            requests.add(new WatermarkRequest(peer, messagingService, waitFor));

        long start = Clock.Global.nanoTime();
        Retry.Deadline deadline = Retry.Deadline.after(TimeUnit.MILLISECONDS.toNanos(TIMEOUT_MILLIS),
                                                      new Retry.Backoff(DatabaseDescriptor.getCmsDefaultRetryMaxTries(),
                                                                        (int) BACKOFF_MILLIS,
                                                                        TCMMetrics.instance.progressBarrierRetries));
        while (!deadline.reachedMax())
        {
            for (WatermarkRequest request : requests)
                request.retry();
            long nextTimeout = Clock.Global.nanoTime() + DatabaseDescriptor.getRpcTimeout(TimeUnit.NANOSECONDS);
            Iterator<WatermarkRequest> iter = requests.iterator();
            while (iter.hasNext())
            {
                WatermarkRequest request = iter.next();
                if (request.condition.awaitUninterruptibly(Math.max(0, nextTimeout - Clock.Global.nanoTime()), TimeUnit.NANOSECONDS) &&
                    request.condition.isSuccess())
                {
                    collected.add(request.to);
                    iter.remove();
                }
            }

            // No need to try processing until we collect enough nodes to pass all conditions
            if (collected.size() < maxWaitFor)
            {
                deadline.maybeSleep();
                continue;
            }

            boolean match = true;
            for (WaitFor waiter : waiters)
            {
                if (!waiter.satisfiedBy(collected))
                {
                    match = false;
                    break;
                }
            }
            if (match)
            {
                logger.info("Collected acknowledgements from {} of nodes for a progress barrier for epoch {} at {}",
                            collected, waitFor, cl);
                return true;
            }
        }

        Set<InetAddressAndPort> remaining = new HashSet<>(superset);
        remaining.removeAll(collected);
        logger.warn("Could not collect {} of nodes for a progress barrier for epoch {} to finish within {}ms. Nodes that have not responded: {}. {}",
                    cl, waitFor, TimeUnit.NANOSECONDS.toMillis(deadline.deadlineNanos - start), remaining, deadline);
        return false;
    }

    public static ConsistencyLevel relaxConsistency(ConsistencyLevel cl)
    {
        logger.debug("Relaxing ProgressBarrier consistency level {}", cl);
        TCMMetrics.instance.progressBarrierCLRelax.mark();
        switch (cl)
        {
            case ALL:
                return ConsistencyLevel.EACH_QUORUM;
            case EACH_QUORUM:
                return ConsistencyLevel.QUORUM;
            case QUORUM:
                return ConsistencyLevel.LOCAL_QUORUM;
            case LOCAL_QUORUM:
                return ConsistencyLevel.ONE;
            case ONE:
                return ConsistencyLevel.NODE_LOCAL;
            default:
                throw new IllegalArgumentException(cl.toString());
        }
    }

    public static class WaitForNone implements WaitFor
    {
        public boolean satisfiedBy(Set<InetAddressAndPort> responded)
        {
            return true;
        }

        public int waitFor()
        {
            return 0;
        }
    }

    public static class WaitForOne implements WaitFor
    {
        final Set<InetAddressAndPort> nodes;

        public WaitForOne(EndpointsForRange writes, EndpointsForRange reads)
        {
            this.nodes = Sets.newHashSetWithExpectedSize(reads.size() + 1);
            writes.forEach(r -> nodes.add(r.endpoint()));
            reads.forEach(r -> nodes.add(r.endpoint()));
        }

        public boolean satisfiedBy(Set<InetAddressAndPort> responded)
        {
            for (InetAddressAndPort node : nodes)
            {
                if (responded.contains(node))
                    return true;
            }

            return false;
        }

        public int waitFor()
        {
            return 1;
        }

        public String toString()
        {
            return "WaitForOne{" +
                   "nodes=" + nodes +
                   '}';
        }
    }

    public static class WaitForQuorum implements WaitFor
    {
        final Set<InetAddressAndPort> nodes;
        final int waitFor;

        public WaitForQuorum(EndpointsForRange writes, EndpointsForRange reads)
        {
            this.nodes = Sets.newHashSetWithExpectedSize(reads.size() + 1);
            writes.forEach(r -> nodes.add(r.endpoint()));
            reads.forEach(r -> nodes.add(r.endpoint()));
            this.waitFor = nodes.size() / 2 + 1;
        }

        public boolean satisfiedBy(Set<InetAddressAndPort> responded)
        {
            int collected = 0;
            for (InetAddressAndPort node : nodes)
            {
                if (responded.contains(node))
                    collected++;
            }

            return collected >= waitFor;
        }

        public int waitFor()
        {
            return waitFor;
        }

        public String toString()
        {
            return "WaitForQuorum{" +
                   "nodes=" + nodes +
                   ", waitFor=" + waitFor +
                   '}';
        }
    }

    public static class WaitForLocalQuorum implements WaitFor
    {
        final Set<InetAddressAndPort> nodesInOurDc;
        final int waitFor;

        public WaitForLocalQuorum(EndpointsForRange writes, EndpointsForRange reads, Directory directory, Location local)
        {
            this.nodesInOurDc = Sets.newHashSetWithExpectedSize(reads.size() + 1);
            writes.forEach(r -> addNode(r, directory, local));
            reads.forEach(r -> addNode(r, directory, local));
            this.waitFor = nodesInOurDc.size() / 2 + 1;
        }

        private void addNode(Replica r, Directory directory, Location local)
        {
            InetAddressAndPort endpoint = r.endpoint();
            String dc = directory.location(directory.peerId(endpoint)).datacenter;
            if (dc.equals(local.datacenter))
                this.nodesInOurDc.add(endpoint);
        }

        public boolean satisfiedBy(Set<InetAddressAndPort> responded)
        {
            int collected = 0;
            for (InetAddressAndPort addr : responded)
            {
                if (nodesInOurDc.contains(addr))
                    collected++;
            }

            return collected >= waitFor;
        }

        public int waitFor()
        {
            return waitFor;
        }

        public String toString()
        {
            return "WaitForLocalQuorum{" +
                   "nodes=" + nodesInOurDc +
                   ", waitFor=" + waitFor +
                   '}';
        }
    }

    /**
     * Probably you do not want to use this in production, but this is still quite useful for testing purposes,
     * when you also use a CL ALL in combination with this, and make sure writes propagate where they're
     * supposed to propagate, alongside with streaming.
     */
    public static class WaitForAll implements WaitFor
    {
        final Set<InetAddressAndPort> nodes;
        final int waitFor;

        public WaitForAll(EndpointsForRange writes, EndpointsForRange reads)
        {
            this.nodes = Sets.newHashSetWithExpectedSize(reads.size() + 1);
            writes.forEach(r -> nodes.add(r.endpoint()));
            reads.forEach(r -> nodes.add(r.endpoint()));
            this.waitFor = nodes.size();
        }

        public boolean satisfiedBy(Set<InetAddressAndPort> responded)
        {
            int collected = 0;
            for (InetAddressAndPort node : nodes)
            {
                if (responded.contains(node))
                    collected++;
            }

            return collected >= waitFor;
        }

        public int waitFor()
        {
            return waitFor;
        }

        public String toString()
        {
            return "WaitForLocalQuorum{" +
                   "nodes=" + nodes +
                   ", waitFor=" + waitFor +
                   '}';
        }
    }

    public static class WaitForEachQuorum implements WaitFor
    {
        final Map<String, Set<InetAddressAndPort>> nodesByDc;
        final Map<String, Integer> waitForByDc;
        final int waitForTotal;

        public WaitForEachQuorum(EndpointsForRange writes, EndpointsForRange reads, Directory directory)
        {
            nodesByDc = Maps.newHashMapWithExpectedSize(directory.knownDatacenters().size());
            writes.forEach((r) -> addToDc(r, directory));
            reads.forEach((r) -> addToDc(r, directory));
            waitForByDc = Maps.newHashMapWithExpectedSize(nodesByDc.size());
            int total = 0;
            for (Map.Entry<String, Set<InetAddressAndPort>> e : nodesByDc.entrySet())
            {
                int waitFor = e.getValue().size() / 2 + 1;
                waitForByDc.put(e.getKey(), waitFor);
                total += waitFor;
            }
            this.waitForTotal = total;
        }

        private void addToDc(Replica r, Directory directory)
        {
            InetAddressAndPort endpoint = r.endpoint();
            String dc = directory.location(directory.peerId(endpoint)).datacenter;
            nodesByDc.computeIfAbsent(dc, (dc_) -> Sets.newHashSetWithExpectedSize(3))
                     .add(endpoint);
        }

        public boolean satisfiedBy(Set<InetAddressAndPort> responded)
        {
            for (Map.Entry<String, Set<InetAddressAndPort>> e : nodesByDc.entrySet())
            {
                int waitFor = waitForByDc.get(e.getKey());
                int collected = 0;
                for (InetAddressAndPort node : e.getValue())
                {
                    if (responded.contains(node))
                        collected++;
                }
                if (collected < waitFor)
                    return false;
            }
            return true;
        }

        public int waitFor()
        {
            return waitForTotal;
        }

        public String toString()
        {
            return "WaitForEachQuorum{" +
                   "nodesByDc=" + nodesByDc +
                   ", waitForByDc=" + waitForByDc +
                   ", waitForTotal=" + waitForTotal +
                   '}';
        }
    }

    public interface WaitFor
    {
        boolean satisfiedBy(Set<InetAddressAndPort> responded);
        int waitFor();
    }

    private static class WatermarkRequest implements RequestCallbackWithFailure<Epoch>
    {
        private AsyncPromise<Void> condition = null;
        private final InetAddressAndPort to;
        private final MessageDelivery messagingService;
        private final Epoch waitFor;

        public WatermarkRequest(InetAddressAndPort to, MessageDelivery messagingService, Epoch waitFor)
        {
            this.to = to;
            this.messagingService = messagingService;
            this.waitFor = waitFor;
        }

        @Override
        public void onResponse(Message<Epoch> msg)
        {
            Epoch remote = msg.payload;
            if (remote.isEqualOrAfter(waitFor))
            {
                logger.debug("Received watermark response from {} with epoch {}", msg.from(), remote);
                condition.trySuccess(null);
            }
            else
            {
                condition.tryFailure(new TimeoutException(String.format("Watermark request returned epoch %s while least %s was expected.", remote, waitFor)));
            }
        }

        @Override
        public void onFailure(InetAddressAndPort from, RequestFailureReason failureReason)
        {
            logger.debug("Error response from {} with {}", from, failureReason);
            condition.tryFailure(new TimeoutException(String.format("Watermark request did returned %s.", failureReason)));
        }

        public void retry()
        {
            condition = new AsyncPromise<>();
            messagingService.sendWithCallback(Message.out(Verb.TCM_CURRENT_EPOCH_REQ, ClusterMetadata.current().epoch), to, this);
        }

        @Override
        public String toString()
        {
            return "WatermarkRequest{" +
                   "condition=" + condition +
                   ", to=" + to +
                   ", messagingService=" + messagingService +
                   ", waitFor=" + waitFor +
                   '}';
        }
    }

    @Override
    public String toString()
    {
        return "ProgressBarrier{" +
               "epoch=" + waitFor +
               ", affectedPeers=" + affectedRanges +
               '}';
    }

    @VisibleForTesting
    public static void propagateLast(LockedRanges.AffectedRanges ranges)
    {
        ClusterMetadata metadata = ClusterMetadata.current();
        new ProgressBarrier(metadata.epoch, metadata.directory.location(metadata.myNodeId()), ranges).await();
    }
}
