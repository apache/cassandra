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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.locator.EndpointsForRange;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageDelivery;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.net.RequestCallback;
import org.apache.cassandra.net.RequestCallbackWithFailure;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.membership.Directory;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.CountDownLatch;

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
    private static final long TIMEOUT_MILLIS = CassandraRelevantProperties.TCM_PROGRESS_BARRIER_TIMEOUT_MILLIS.getLong();
    private static final long BACKOFF_MILLIS = CassandraRelevantProperties.TCM_PROGRESS_BARRIER_BACKOFF_MILLIS.getLong();

    public final Epoch waitFor;
    public final LockedRanges.AffectedRanges affectedRanges;
    public final MessageDelivery messagingService;
    public final Predicate<InetAddressAndPort> filter;

    public ProgressBarrier(Epoch waitFor, LockedRanges.AffectedRanges affectedRanges)
    {
        this(waitFor, affectedRanges, MessagingService.instance(), (t) -> true);
    }

    public ProgressBarrier(Epoch waitFor, LockedRanges.AffectedRanges affectedRanges, Predicate<InetAddressAndPort> filter)
    {
        this(waitFor, affectedRanges, MessagingService.instance(), filter);
    }

    private ProgressBarrier(Epoch waitFor, LockedRanges.AffectedRanges affectedRanges, MessageDelivery messagingService, Predicate<InetAddressAndPort> filter)
    {
        this.waitFor = waitFor;
        this.affectedRanges = affectedRanges;
        this.messagingService = messagingService;
        this.filter = filter;
    }

    public static ProgressBarrier immediate()
    {
        return new ProgressBarrier(Epoch.EMPTY, LockedRanges.AffectedRanges.EMPTY);
    }

    @VisibleForTesting
    public ProgressBarrier withMessagingService(MessageDelivery messagingService)
    {
        return new ProgressBarrier(waitFor, affectedRanges, messagingService, filter);
    }

    public boolean await()
    {
        if (waitFor.is(Epoch.EMPTY))
            return true;

        ConsistencyLevel currentCL = ConsistencyLevel.EACH_QUORUM;
        while (!await(currentCL, ClusterMetadata.current()))
        {
            if (currentCL != DatabaseDescriptor.getProgressBarrierLowestAcceptableConsistencyLevel())
            {
                ConsistencyLevel prev = currentCL;
                currentCL = relaxConsistency(prev);
                logger.info(String.format("Could not collect epoch acknowlegements within %dms for %s. Falling back to %s.", TIMEOUT_MILLIS, prev, currentCL));
            }
            else
                return false;
        }
        return true;
    }

    @VisibleForTesting
    public boolean await(ConsistencyLevel cl, ClusterMetadata metadata)
    {
        if (waitFor.is(Epoch.EMPTY))
            return true;

        int maxWaitFor = 0;
        long deadline = Clock.Global.nanoTime() + TimeUnit.MILLISECONDS.toNanos(TIMEOUT_MILLIS);

        Map<ReplicationParams, Set<Range<Token>>> affectedRangesMap = affectedRanges.asMap();
        Map<ReplicationParams, WaitFor> waiters = new HashMap<>(affectedRangesMap.size());

        Set<InetAddressAndPort> superset = new HashSet<>();
        for (Map.Entry<ReplicationParams, Set<Range<Token>>> e : affectedRangesMap.entrySet())
        {
            ReplicationParams params = e.getKey();
            Set<Range<Token>> ranges = e.getValue();
            for (Range<Token> range : ranges)
            {
                EndpointsForRange writes = metadata.placements.get(params).writes.forRange(range).filter(r -> filter.test(r.endpoint()));
                EndpointsForRange reads = metadata.placements.get(params).reads.forRange(range).filter(r -> filter.test(r.endpoint()));
                reads.stream().map(Replica::endpoint).forEach(superset::add);
                writes.stream().map(Replica::endpoint).forEach(superset::add);

                WaitFor waitFor;
                switch (cl)
                {
                    case EACH_QUORUM:
                        waitFor = new WaitForEachQuorum(writes, reads, metadata.directory);
                        break;
                    case LOCAL_QUORUM:
                        waitFor = new WaitForLocalQuorum(writes, reads, metadata.directory);
                        break;
                    case QUORUM:
                        waitFor = new WaitForQuorum(writes, reads);
                        break;
                    case ONE:
                        waitFor = new WaitForOne(writes, reads);
                        break;
                    default:
                        throw new IllegalArgumentException("Progress barrier only supports EACH_QUORUM, LOCAL_QUORUM, QUORUM, and ONE, but not " + cl);
                }

                maxWaitFor = Math.max(waitFor.waitFor(), maxWaitFor);
                waiters.put(params, waitFor);
            }
        }

        AtomicBoolean active = new AtomicBoolean(true);
        Set<InetAddressAndPort> collected = new ConcurrentSkipListSet<>();
        CountDownLatch latch = CountDownLatch.newCountDownLatch(maxWaitFor);
        for (InetAddressAndPort peer : superset)
        {
            sendRequest(peer, active, () -> {
                collected.add(peer);
                latch.decrement();
            });
        }

        boolean signalled = latch.awaitUninterruptibly(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        int lastCheckedSize = -1;
        if (signalled)
        {
            while (Clock.Global.nanoTime() < deadline)
            {
                int currentSize = collected.size();
                if (lastCheckedSize != currentSize)
                {
                    boolean match = true;
                    for (WaitFor waiter : waiters.values())
                    {
                        if (!waiter.satisfiedBy(collected))
                        {
                            match = false;
                            break;
                        }
                    }
                    if (match)
                    {
                        active.set(false);
                        return true;
                    }
                    // It could happen that the size have changed between we have called size()
                    // and started iteration, but this is fine, since in this case we will just
                    // come around once again.
                    lastCheckedSize = currentSize;
                }

                Uninterruptibles.sleepUninterruptibly(BACKOFF_MILLIS, TimeUnit.MILLISECONDS);
            }
        }

        Set<InetAddressAndPort> remaining = new HashSet<>(superset);
        remaining.removeAll(collected);
        logger.warn("Could not collect {} of nodes for a progress barrier for epoch {} to finish within {}ms. Nodes that have not responded: {}",
                    cl, waitFor, TIMEOUT_MILLIS, remaining);

        active.set(false);
        return false;
    }

    public static ConsistencyLevel relaxConsistency(ConsistencyLevel cl)
    {
        switch (cl)
        {
            case EACH_QUORUM:
                return ConsistencyLevel.QUORUM;
            case QUORUM:
                return ConsistencyLevel.LOCAL_QUORUM;
            case LOCAL_QUORUM:
            case ONE:
                return ConsistencyLevel.ONE;
            default:
                throw new IllegalArgumentException(cl.toString());
        }
    }

    public static class WaitForOne implements WaitFor
    {
        final Set<InetAddressAndPort> nodes;

        public WaitForOne(EndpointsForRange writes, EndpointsForRange reads)
        {
            this.nodes = new HashSet<>(reads.size() + 1);
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
            return "WaitForAny{" +
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
            this.nodes = new HashSet<>(reads.size() + 1);
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
        final Set<InetAddressAndPort> nodes;
        final int waitFor;

        public WaitForLocalQuorum(EndpointsForRange writes, EndpointsForRange reads, Directory directory)
        {

            this.nodes = new HashSet<>(reads.size() + 1);
            writes.forEach(r -> addNode(r, directory));
            reads.forEach(r -> addNode(r, directory));
            this.waitFor = nodes.size() / 2 + 1;
        }

        private void addNode(Replica r, Directory directory)
        {
            InetAddressAndPort endpoint = r.endpoint();
            String dc = directory.location(directory.peerId(endpoint)).datacenter;
            String localDc = DatabaseDescriptor.getLocalDataCenter();
            if (dc.equals(localDc))
                this.nodes.add(endpoint);
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
            this.nodesByDc = new HashMap<>(directory.knownDatacenters().size());
            writes.forEach((r) -> addToDc(r, directory));
            reads.forEach((r) -> addToDc(r, directory));
            this.waitForByDc = new HashMap<>(nodesByDc.size());
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
            nodesByDc.computeIfAbsent(dc, (dc_) -> new HashSet<>(3))
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

    private void sendRequest(InetAddressAndPort to, AtomicBoolean active, Runnable collected)
    {
        logger.debug("Sending watermark request to {}, expecting at least {}", to, waitFor);
        RequestCallback<NoPayload> callback = new RequestCallbackWithFailure<NoPayload>()
        {
            @Override
            public void onResponse(Message<NoPayload> msg)
            {
                Epoch remote = msg.epoch();
                logger.debug("Received watermark response from {} with epoch {}", msg.from(), remote);
                if (remote.isEqualOrAfter(waitFor))
                    collected.run();
                else
                    retry(to, active);
            }

            @Override
            public void onFailure(InetAddressAndPort from, RequestFailureReason failureReason)
            {
                logger.debug("Watermark failure from {} with reason {}", from, failureReason);
                retry(to, active);
            }

            private void retry(InetAddressAndPort to, AtomicBoolean active)
            {
                FBUtilities.sleepQuietly(BACKOFF_MILLIS);
                sendRequest(to, active, collected);
            }
        };

        if (active.get())
            messagingService.sendWithCallback(Message.out(Verb.TCM_CURRENT_EPOCH_REQ, NoPayload.noPayload), to, callback);
    }

    @Override
    public String toString()
    {
        return "ProgressBarrier{" +
               "epoch=" + waitFor +
               ", affectedPeers=" + affectedRanges +
               '}';
    }
}
