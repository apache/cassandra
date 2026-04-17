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

package accord.topology;

import java.util.IdentityHashMap;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.AsyncExecutor;
import accord.api.TopologyListener;
import accord.api.Timeouts;
import accord.api.TopologyService;
import accord.api.TopologySorter;
import accord.api.VisibleForImplementation;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.TimeService;
import accord.primitives.Ranges;
import accord.primitives.TxnId;
import accord.topology.TopologyCollector.BestFastPath;
import accord.topology.TopologyCollector.Simple;
import accord.topology.TopologyCollector.SupportsPrivilegedFastPath;
import accord.utils.Invariants;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;
import accord.utils.async.NestedAsyncResult;

import static accord.primitives.AbstractRanges.UnionMode.MERGE_ADJACENT;
import static accord.primitives.Routables.Slice.Minimal;

/**
 * Manages topology state changes and update bookkeeping
 *
 * Each time the topology changes we need to:
 * * confirm previous owners of ranges we replicate are aware of the new config
 * * learn of any outstanding operations for ranges we replicate
 * * clean up obsolete data
 *
 * Assumes a topology service that won't report epoch n without having n-1 etc also available
 *
 * TODO (desired, efficiency/clarity): make TopologyManager a Topologies and copy-on-write update to it,
 *  so we can always just take a reference for transactions instead of copying every time (and index into it by the txnId.epoch)
 */
public class TopologyManager
{
    private static final Logger logger = LoggerFactory.getLogger(TopologyManager.class);
    private static final PendingEpoch SUCCESS;

    static
    {
        SUCCESS = new PendingEpoch(-1L, null);
        SUCCESS.setActive();
    }

    final TopologySorter.Supplier sorter;
    final Simple liveCollector, allCollector;
    final BestFastPath bestFastPath;
    final SupportsPrivilegedFastPath supportsPrivilegedFastPath;
    final Node node;
    final TopologyService topologyService;
    final TimeService time;
    final Timeouts timeouts;
    private volatile ActiveEpochs active;
    private final PendingEpochs pending;
    private final CopyOnWriteArrayList<TopologyListener> listeners = new CopyOnWriteArrayList<>();

    public TopologyManager(TopologySorter.Supplier sorter, Node node, TopologyService topologyService, TimeService time, Timeouts timeouts)
    {
        this.sorter = sorter;
        this.liveCollector = new Simple(sorter, SelectShards.LIVE);
        this.allCollector = new Simple(sorter, SelectShards.ALL);
        this.bestFastPath = new BestFastPath(node.id());
        this.supportsPrivilegedFastPath = new SupportsPrivilegedFastPath(node.id());
        this.node = node;
        this.time = time;
        this.timeouts = timeouts;
        this.topologyService = topologyService;
        this.active = new ActiveEpochs(this, new ActiveEpoch[0], -1);
        this.pending = new PendingEpochs(this);
    }

    public void onReadyToCoordinate(Id node, long epoch)
    {
        synchronized (this)
        {
            if (epoch >= active.minEpoch())
                active.onReadyToCoordinate(node, epoch);
            if (epoch > active.currentEpoch)
                pending.remoteReadyToCoordinate(node, epoch);
        }
        for (TopologyListener listener : listeners)
            listener.onRemoteReadyToCoordinate(node, epoch);
    }

    public void onEpochClosed(Ranges ranges, long epoch)
    {
        onEpochClosed(ranges, epoch, null);
    }

    public void onEpochClosed(Ranges ranges, TxnId txnId)
    {
        onEpochClosed(ranges, txnId.epoch(), txnId);
    }

    private void onEpochClosed(Ranges ranges, long epoch, @Nullable TxnId txnId)
    {
        Topology topology = null;
        synchronized (this)
        {
            ActiveEpoch e = active.ifExists(epoch);
            if (txnId != null)
            {
                if (e != null)
                {
                    ranges = ranges.without(e.addedRanges);
                    if (ranges.isEmpty())
                        return;
                }
                e = active.ifExists(--epoch);
            }

            if (e != null)
                topology = e.all();

            if (epoch > active.currentEpoch)
                ranges = pending.closed(ranges, epoch);
            ranges = active.closed(ranges, epoch);
        }
        if (!ranges.isEmpty())
        {
            for (TopologyListener listener : listeners)
                listener.onEpochClosed(ranges, epoch, topology);
        }
    }

    public void onEpochRetired(Ranges ranges, long epoch)
    {
        onEpochRetired(ranges, epoch, null);
    }

    public void onEpochRetired(Ranges ranges, TxnId txnId)
    {
        onEpochRetired(ranges, txnId.epoch(), txnId);
    }

    private void onEpochRetired(Ranges ranges, long epoch, @Nullable TxnId txnId)
    {
        Topology topology = null;
        synchronized (this)
        {
            ActiveEpoch e = active.ifExists(epoch);
            if (txnId != null)
            {
                if (e != null)
                {
                    ranges = ranges.without(e.addedRanges);
                    if (ranges.isEmpty())
                        return;
                }

                // if we're retiring only ranges that are no longer live, we can retire the declaration epoch; otherwise we only retire the prior epoch
                if (e == null || e.live.ranges.intersects(ranges))
                    e = active.ifExists(--epoch);
            }

            if (e != null)
                topology = e.all;

            if (epoch > active.currentEpoch)
                ranges = pending.retired(ranges, epoch);
            ranges = active.retired(ranges, epoch);
        }
        if (!ranges.isEmpty())
        {
            for (TopologyListener listener : listeners)
                listener.onEpochRetired(ranges, epoch, topology);
        }
    }

    public synchronized void truncateTopologiesUntil(long epoch)
    {
        ActiveEpochs current = active;
        Invariants.requireArgument(current.epoch() >= epoch, "Unable to truncate; epoch %d is > current epoch %d", epoch, current.epoch());

        if (current.minEpoch() >= epoch)
            return;

        int newLen = current.epochs.length - (int) (epoch - current.minEpoch());
        Invariants.require(current.epochs[newLen - 1].isQuorumReady(), "Epoch %d is not ready to coordinate", current.epochs[newLen - 1].epoch());

        ActiveEpoch[] nextEpochs = new ActiveEpoch[newLen];
        System.arraycopy(current.epochs, 0, nextEpochs, 0, newLen);
        active = new ActiveEpochs(this, nextEpochs, current.firstNonEmptyEpoch);
    }

    public TopologySorter.Supplier sorter()
    {
        return sorter;
    }

    public Topology current()
    {
        return active.current();
    }

    public Topology currentLocal()
    {
        return active.currentLocal();
    }

    public boolean isEmpty()
    {
        return active.isEmpty() && pending.isEmpty();
    }

    public long epoch()
    {
        return current().epoch;
    }

    @VisibleForImplementation
    public synchronized long pendingEpoch()
    {
        return pending.maxEpoch();
    }

    // TODO (desired): add tests for epoch GC and tracking
    @VisibleForImplementation
    public long firstNonEmpty()
    {
        return active.firstNonEmptyEpoch;
    }

    public long minEpoch()
    {
        ActiveEpochs epochs = this.active;
        return epochs.minEpoch();
    }

    // TODO (testing): test all of these methods when asking for epochs that have been cleaned up (and other code paths)

    public ActiveEpochs active()
    {
        return active;
    }

    public void addListener(TopologyListener listener)
    {
        listeners.add(listener);
    }

    public void removeListener(TopologyListener listener)
    {
        listeners.remove(listener);
    }

    protected Executor executor()
    {
        return Runnable::run;
    }

    public void reportTopology(Topology topology)
    {
        PendingEpoch e;
        synchronized (this)
        {
            long epoch = topology.epoch;
            // if active is empty, treat the earliest pending epoch as our low bound to avoid race conditions where we begin updating active but discover an earlier epoch
            long currentEpoch = !active.isEmpty() ? active.currentEpoch : !pending.isEmpty() ? pending.atIndex(0).epoch - 1 : 0;
            if (epoch <= currentEpoch)
            {
                logger.debug("Ignoring topology for epoch {} which is behind our latest epoch {}", epoch, currentEpoch);
                return;
            }

            e = pending.getOrCreate(epoch);
            e.setTopology(topology);
        }

        logger.debug("Epoch {} received", topology.epoch());
        for (TopologyListener listener : listeners)
            listener.onReceived(topology);

        updateActive();
    }

    public static class RegainingEpochRange
    {
        public final long epoch;
        public final Ranges ranges;

        public RegainingEpochRange(long epoch, Ranges ranges)
        {
            this.epoch = epoch;
            this.ranges = ranges;
        }

        public long epoch()
        {
            return epoch;
        }

        public Ranges ranges()
        {
            return ranges;
        }
    }

    @Nullable
    public RegainingEpochRange computeRegaining(Topology current, Topology next)
    {
        Map<Id, Ranges> additions = Topology.computeNodeAdditions(current, next);
        long greatestEpoch = -1;
        Ranges ranges = Ranges.EMPTY;

        ActiveEpochs active = this.active;
        for (Map.Entry<Id, Ranges> entry : additions.entrySet())
        {
            Ranges addingForNode = entry.getValue();
            for (ActiveEpoch e : active)
            {
                addingForNode = addingForNode.without(e.removedRanges).without(e.retired());
                if (addingForNode.isEmpty())
                    break;

                Ranges existingForNode = e.all().rangesForNode(entry.getKey());
                Ranges regainingForNode = addingForNode.slice(existingForNode, Minimal);
                if (!regainingForNode.isEmpty())
                {
                    greatestEpoch = Math.max(greatestEpoch, e.epoch());
                    ranges = ranges.union(MERGE_ADJACENT, regainingForNode);
                    addingForNode = addingForNode.without(regainingForNode);
                }
                addingForNode = addingForNode.without(e.addedRanges);
            }
        }

        if (greatestEpoch != -1)
            return new RegainingEpochRange(greatestEpoch, ranges);

        return null;
    }

    private final AtomicBoolean updatingActive = new AtomicBoolean();
    private void updateActive()
    {
        if (!updatingActive.compareAndSet(false, true))
            return;

        try
        {
            while (true)
            {
                Topology topology;
                PendingEpoch pending;
                synchronized (this)
                {
                    if (this.pending.isEmpty() || (!this.active.isEmpty() && this.pending.atIndex(0).epoch > 1 + current().epoch()))
                        return;

                    pending = this.pending.atIndex(0);
                    topology = pending.topology();
                    if (topology == null)
                        return;
                }

                Supplier<EpochReady> bootstrap = node.commandStores().updateTopology(node, topology);
                AsyncResult.Settable<EpochReady> whenSetup = new AsyncResults.SettableWithDescription<>("Publishing Active Epoch");
                EpochReady epochReady = new EpochReady(topology.epoch,
                                                       NestedAsyncResult.flatMap(whenSetup, ignore -> AsyncResults.success(null)),
                                                       NestedAsyncResult.flatMap(whenSetup, EpochReady::coordinate),
                                                       NestedAsyncResult.flatMap(whenSetup, EpochReady::data),
                                                       NestedAsyncResult.flatMap(whenSetup, EpochReady::reads));

                if (!this.active.isEmpty())
                {
                    ActiveEpoch prev = this.active.epochs[0];
                    Invariants.require(prev.epoch() == topology.epoch - 1);
                    epochReady = orderReporting(prev.epochReady(), epochReady);
                }

                ActiveEpoch active = new ActiveEpoch(node.id(), topology, epochReady, sorter.get(topology), this.active.current().ranges);

                synchronized (this)
                {
                    active.recordClosed(pending.closed);
                    active.recordRetired(pending.retired);
                    pending.ready.forEach(active::onReadyToCoordinate);

                    ActiveEpochs prev = this.active;
                    ActiveEpoch[] next = new ActiveEpoch[prev.epochs.length + 1];
                    System.arraycopy(prev.epochs, 0, next, 1, prev.epochs.length);
                    next[0] = active;

                    if (!prev.isEmpty() && !prev.epochs[0].all.hardRemoved.containsAll(topology.hardRemoved))
                    {
                        IdentityHashMap<Shard, Shard> cache = new IdentityHashMap<>();
                        for (int i = next.length - 1 ; i >= 0 ; --i)
                        {
                            ActiveEpoch e = next[i];
                            Topology newGlobal = next[i].all.withHardRemoved(topology.hardRemoved, cache);
                            if (newGlobal != e.all)
                            {
                                next[i] = new ActiveEpoch(node.id(), newGlobal, e.shardQuorumReady, e.receivedNodeReady, e.quorumReadyTracker,
                                                          e.addedRanges, e.removedRanges, e.epochReady(), e.quorumReady(), e.closed(), e.retired());
                            }
                        }
                    }

                    this.active = new ActiveEpochs(this, next, prev.firstNonEmptyEpoch);
                    this.pending.removeFirst(topology.epoch);
                }

                EpochReady innerReady = bootstrap(bootstrap);
                whenSetup.setSuccess(innerReady);

                pending.setActive();
                listeners.forEach(listener -> listener.onActive(active));

                long epoch = topology.epoch;
                Node.Id self = node.id();
                innerReady.coordinate.invokeIfSuccess(() -> {
                    listeners.forEach(listener -> listener.onReadyToCoordinate(topology));
                    onReadyToCoordinate(self, epoch);
                });
            }
        }
        finally
        {
            updatingActive.set(false);
        }
    }

    @VisibleForTesting
    protected EpochReady bootstrap(Supplier<EpochReady> bootstrap)
    {
        return bootstrap.get();
    }

    private static EpochReady orderReporting(EpochReady previous, EpochReady next)
    {
        if (previous.epoch + 1 != next.epoch)
            throw new IllegalArgumentException("Attempted to order epochs but they are not next to each other... previous=" + previous.epoch + ", next=" + next.epoch);
        if (previous.coordinate.isDone() && previous.data.isDone() && previous.reads.isDone())
            return next;

        return new EpochReady(next.epoch,
                              next.active,
                              NestedAsyncResult.flatMap(previous.coordinate, ignore -> next.coordinate),
                              NestedAsyncResult.flatMap(previous.data, ignore -> next.data),
                              NestedAsyncResult.flatMap(previous.reads, ignore -> next.reads)
        );
    }

    public AsyncChain<Void> await(long epoch, @Nullable AsyncExecutor ifAsync)
    {
        PendingEpoch pendingEpoch;
        boolean fetch;
        synchronized (this)
        {
            if (epoch <= active.currentEpoch)
                return AsyncChains.success(null);

            pendingEpoch = pending.getOrCreate(epoch);
            fetch = pendingEpoch.fetching == null;
        }

        node.agent().systemEvents().onWaitingForEpoch(epoch);
        AsyncChain<Void> result = pendingEpoch.whenActive().chainImmediatelyElse(ifAsync);
        if (fetch)
        {
            while (true)
            {
                fetch(pendingEpoch);
                --epoch;
                synchronized (this)
                {
                    if (epoch <= active.currentEpoch)
                        break;

                    pendingEpoch = pending.getOrCreate(epoch);
                    if (pendingEpoch.fetching != null)
                        break;
                }
            }
        }
        return result;
    }

    private void fetch(PendingEpoch pending)
    {
        synchronized (this)
        {
            if (pending.topology() != null || pending.epoch < active.currentEpoch)
                return;

            if (pending.fetching != null && !pending.fetching.isDone())
                return;
            
            pending.fetching = topologyService.fetchTopologyForEpoch(pending.epoch);
        }

        pending.fetching.invoke((success, fail) -> {
            if (fail == null) reportTopology(success);
            else if (active.currentEpoch < pending.epoch && pending.topology() == null)
            {
                // TODO (expected): special casing of TopologyRetiredException?
                logger.warn("Failed to fetch epoch {}. Retrying.", pending.epoch, fail);
                node.agent().onException(fail, "Fetch epoch " + pending.epoch);
                long retryInMicros = node.agent().retryTopologyDelay(node, 1 + ++pending.fetchAttempts, TimeUnit.MICROSECONDS);
                node.scheduler().once(() -> fetch(pending), retryInMicros, TimeUnit.MICROSECONDS);
            }
        });
    }

    @VisibleForImplementation
    public AsyncResult<Void> epochReady(long epoch, Function<EpochReady, AsyncResult<Void>> get)
    {
        // synchronized for state.ready visibility
        synchronized (this)
        {
            if (active.hasAtLeastEpoch(epoch))
            {
                if (!active.hasEpoch(epoch))
                    return get.apply(EpochReady.done(epoch));
                return get.apply(active.getKnown(epoch).epochReady());
            }

            return pending.getOrCreate(epoch).whenActive().get().flatMap(r -> get.apply(active.epochReady(epoch)));
        }
    }

    @VisibleForTesting
    ActiveEpoch unsafeGetActiveEpoch(long epoch)
    {
        return active.getKnown(epoch);
    }

    @VisibleForTesting
    public void unsafeSetActive(ActiveEpochs newActive)
    {
        active = newActive;
    }

    @VisibleForTesting
    public Ranges unsafeQuorumReady(long epoch)
    {
        ActiveEpoch e = active.ifExists(epoch);
        return e == null ? Ranges.EMPTY : e.quorumReady();
    }

    public boolean unsafeIsQuorumReady(long epoch)
    {
        ActiveEpoch e = active.ifExists(epoch);
        return e != null && e.isQuorumReady();
    }

    public TopologyService topologyService()
    {
        return topologyService;
    }
}
