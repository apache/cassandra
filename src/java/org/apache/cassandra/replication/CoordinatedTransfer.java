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

package org.apache.cassandra.replication;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.streaming.CassandraOutgoingFile;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.RequestFailure;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.net.RequestCallbackWithFailure;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.streaming.OutgoingStream;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.streaming.StreamPlan;
import org.apache.cassandra.streaming.StreamResultFuture;
import org.apache.cassandra.streaming.StreamState;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.concurrent.AsyncFuture;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.FutureCombiner;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;

/**
 * A transfer for a single replica set.
 *
 * REVIEW: Right now for simplicity, streaming from coordinator to itself instead of copying files. This has some
 * perks: (1) it allows us to import out-of-range SSTables using the same paths, and (2) it uses the
 * existing lifecycle management to handle crash-safety, so don't need to deal with atomic multi-file copy.
 */
public class CoordinatedTransfer
{
    private static final Logger logger = LoggerFactory.getLogger(CoordinatedTransfer.class);

    String logPrefix()
    {
        return String.format("[CoordinatedTransfer #%s]", transferId);
    }

    final TimeUUID transferId = TimeUUID.Generator.nextTimeUUID();

    // TODO(expected): Add epoch at time of creation
    final String keyspace;
    public final Range<Token> range;

    // Map peer to streaming planId if successful stream completed
    final ConcurrentMap<InetAddressAndPort, SingleTransferResult> streams;

    // Acknowledged activations
    private enum ActivationState
    {
        SENT,
        COMPLETED
    }

    final ConcurrentMap<InetAddressAndPort, ActivationState> activations = new ConcurrentHashMap<>();

    final Collection<SSTableReader> sstables;

    final ConsistencyLevel cl;

    final Supplier<MutationId> getActivationId;
    volatile MutationId activationId = null;

    CoordinatedTransfer(String keyspace, Range<Token> range, Participants participants, Collection<SSTableReader> sstables, ConsistencyLevel cl, Supplier<MutationId> getActivationId)
    {
        this.keyspace = keyspace;
        this.range = range;
        this.sstables = sstables;
        this.cl = cl;
        this.getActivationId = getActivationId;

        ClusterMetadata cm = ClusterMetadata.current();
        this.streams = new ConcurrentHashMap<>(participants.size());
        for (int i = 0; i < participants.size(); i++)
        {
            InetAddressAndPort addr = cm.directory.getNodeAddresses(new NodeId(participants.get(i))).broadcastAddress;
            this.streams.put(addr, SingleTransferResult.Unknown());
        }
    }

    void execute()
    {
        logger.debug("Executing tracked bulk transfer {}", this);
        LocalTransfers.instance().save(this);
        stream();
    }

    private void stream()
    {
        // TODO: Don't stream multiple copies over the WAN, send one copy and indicate forwarding
        List<Future<Void>> streaming = new ArrayList<>(streams.size());
        for (InetAddressAndPort to : streams.keySet())
            streaming.add(stream(to));

        Future<List<Void>> future = FutureCombiner.allOf(streaming);
        future.awaitUninterruptibly();
        future.rethrowIfFailed();
    }

    private boolean sufficient()
    {
        AbstractReplicationStrategy ars = Keyspace.open(keyspace).getReplicationStrategy();
        int blockFor = cl.blockFor(ars);
        int responses = 0;
        for (Map.Entry<InetAddressAndPort, SingleTransferResult> entry : streams.entrySet())
        {
            if (entry.getValue().complete())
                responses++;
        }
        return responses >= blockFor;
    }

    Future<Void> stream(InetAddressAndPort to)
    {
        return streamTask(to).andThenAsync(result -> streamComplete(to, result));
    }

    private Future<Void> streamComplete(InetAddressAndPort to, SingleTransferResult result)
    {
        streams.put(to, result);
        logger.info("{} Completed streaming to {}, {}", logPrefix(), to, this);
        return maybeActivate();
    }

    synchronized Future<Void> maybeActivate()
    {
        /* TODO
        If topology has changed after streaming, need to ensure new topology doesn't break consistency of completed
        streams.
        */

        logger.debug("maybeActivate {} {}", streams, activations);

        // If any activations have already been sent out, send new activations to any received plans that have not yet
        // been activated
        if (activations.containsValue(ActivationState.COMPLETED))
        {
            Set<InetAddressAndPort> peers = new HashSet<>();
            for (Map.Entry<InetAddressAndPort, SingleTransferResult> entry : streams.entrySet())
            {
                if (entry.getValue().complete())
                    peers.add(entry.getKey());
            }

            peers.removeAll(activations.keySet());

            if (!peers.isEmpty())
            {
                logger.debug("{} Transfer already activated on peers {}, sending activations to {}", logPrefix(), activations, peers);
                return activateOn(peers);
            }
        }

        // If no activations have been sent out, check whether we have enough planIds back to meet the required CL
        else if (sufficient())
        {
            Set<InetAddressAndPort> peers = new HashSet<>();
            for (Map.Entry<InetAddressAndPort, SingleTransferResult> entry : streams.entrySet())
            {
                InetAddressAndPort peer = entry.getKey();
                if (entry.getValue().activate() && !activations.containsKey(peer))
                    peers.add(peer);
            }
            logger.debug("{} Transfer meets consistency level {}, sending activations to {}", logPrefix(), cl, peers);
            return activateOn(peers);
        }

        logger.debug("Nothing to activate");
        return ImmediateFuture.success(null);
    }

    synchronized Future<Void> activateOn(Collection<InetAddressAndPort> peers)
    {
        Preconditions.checkState(!peers.isEmpty());

        if (activationId == null)
        {
            activationId = getActivationId.get();
            logger.info("{} Assigned activationId {}", logPrefix(), activationId);
        }
        LocalTransfers.instance().activating(this);

        // First phase is dryRun to ensure data is present on disk, then second phase does the actual import. This
        // ensures that if something goes wrong (like a topology change during import), we don't have divergence.

        class AllRespond extends AsyncFuture<Void> implements RequestCallbackWithFailure<NoPayload>
        {
            final ConcurrentHashMap<InetAddressAndPort, InetAddressAndPort> responses = new ConcurrentHashMap<>(peers.size());

            public AllRespond()
            {
                for (InetAddressAndPort peer : peers)
                    responses.put(peer, peer);
            }

            @Override
            public void onResponse(Message<NoPayload> msg)
            {
                logger.debug("{} Got response from: {}", logPrefix(), msg.from());
                responses.remove(msg.from());
                if (responses.isEmpty())
                    trySuccess(null);
            }

            @Override
            public void onFailure(InetAddressAndPort from, RequestFailure failure)
            {
                logger.debug("{} Got failure {} from {}", logPrefix(), failure, from);
                tryFailure(null);
            }
        }

        AllRespond allRespond = new AllRespond();
        for (InetAddressAndPort peer : peers)
        {
            TransferActivation activation = new TransferActivation(this, peer, true);
            Message<TransferActivation> msg = Message.out(Verb.TRACKED_TRANSFER_ACTIVATE_REQ, activation);
            logger.debug("{} Sending {} to peer {}", logPrefix(), activation, peer);
            MessagingService.instance().sendWithCallback(msg, peer, allRespond);
            CoordinatedTransfer.this.activations.put(msg.from(), ActivationState.SENT);
        }
        allRespond.awaitUninterruptibly();
        logger.debug("{} Dry run complete for {}", logPrefix(), peers);

        // Acknowledgement of activation is equivalent to a remote write acknowledgement. The imported SSTables
        // are now part of the live set, visible to reads

        class Callback extends AsyncFuture<Void> implements RequestCallbackWithFailure<Void>
        {
            final ConcurrentHashMap<InetAddressAndPort, InetAddressAndPort> acks = new ConcurrentHashMap<>();

            private Callback(Collection<InetAddressAndPort> acks)
            {
                for (InetAddressAndPort ack : acks)
                    this.acks.put(ack, ack);
            }

            @Override
            public void onResponse(Message<Void> msg)
            {
                logger.debug("Activation successfully applied on {}", msg.from());
                ActivationState existing = CoordinatedTransfer.this.activations.put(msg.from(), ActivationState.COMPLETED);
                // Preconditions.checkState(existing == ActivationState.SENT);
                logger.debug("Activation prior state {}", existing);

                MutationTrackingService.instance.receivedActivationAck(CoordinatedTransfer.this, msg.from());
                acks.remove(msg.from());
                if (acks.isEmpty())
                    trySuccess(null);
            }

            @Override
            public void onFailure(InetAddressAndPort from, RequestFailure failure)
            {
                logger.error("Failed activation on {} due to {}", from, failure);
                // TODO(expected): should fail if we don't meet requested CL, even though individual failures are fine
                // tryFailure(new RuntimeException(String.format("Failed activation on %s due to %s", from, failure)));
                acks.remove(from);
                if (acks.isEmpty())
                    trySuccess(null);
            }
        }

        Callback callback = new Callback(peers);
        for (InetAddressAndPort peer : peers)
        {
            TransferActivation activation = new TransferActivation(this, peer, false);
            Message<TransferActivation> msg = Message.out(Verb.TRACKED_TRANSFER_ACTIVATE_REQ, activation);

            logger.debug("{} Sending {} to peer {}", logPrefix(), activation, peer);
            MessagingService.instance().sendWithCallback(msg, peer, callback);
        }

        return callback;
    }

    static class SingleTransferResult
    {
        private final boolean complete;
        private final TimeUUID planId;

        private SingleTransferResult(boolean complete, TimeUUID planId)
        {
            this.complete = complete;
            this.planId = planId;
        }

        private static SingleTransferResult Complete(TimeUUID planId)
        {
            Preconditions.checkArgument(planId != null);
            return new SingleTransferResult(true, planId);
        }

        private static SingleTransferResult Noop()
        {
            return new SingleTransferResult(true, null);
        }

        private static SingleTransferResult Unknown()
        {
            return new SingleTransferResult(false, null);
        }

        public boolean activate()
        {
            return complete && planId != null;
        }

        public TimeUUID planId()
        {
            Preconditions.checkState(planId != null);
            return planId;
        }

        public boolean complete()
        {
            return complete;
        }

        public boolean noop()
        {
            return complete && planId == null;
        }

        @Override
        public String toString()
        {
            return "SingleTransferResult{" +
                   (noop() ? "Noop()" : complete() ? String.format("Complete(%s)", planId) : "Unknown()") +
                   '}';
        }
    }

    private Future<SingleTransferResult> streamTask(InetAddressAndPort to)
    {
        Callable<SingleTransferResult> callable = () -> {
            StreamPlan plan = new StreamPlan(StreamOperation.IMPORT);

            // No need to flush, only using non-live SSTables already on disk
            plan.flushBeforeTransfer(false);

            for (SSTableReader sstable : sstables)
            {
                List<Range<Token>> ranges = Collections.singletonList(range);
                List<SSTableReader.PartitionPositionBounds> positions = sstable.getPositionsForRanges(ranges);
                long estimatedKeys = sstable.estimatedKeysForRanges(ranges);
                OutgoingStream stream = new CassandraOutgoingFile(StreamOperation.IMPORT, sstable.ref(), positions, ranges, estimatedKeys);
                plan.transferStreams(to, Collections.singleton(stream));
            }

            logger.info("{} Starting streaming transfer {} to peer {}", logPrefix(), this, to);
            StreamResultFuture execute = plan.execute();
            StreamState state;
            try
            {
                state = execute.get();
                logger.debug("{} Completed streaming transfer {} to peer {}", logPrefix(), this, to);
            }
            catch (InterruptedException | ExecutionException e)
            {
                logger.error("Stream session failed with error", e);
                return SingleTransferResult.Unknown();
            }

            if (state.hasFailedSession() || state.hasAbortedSession())
            {
                logger.error("Stream failed due to failed or aborted sessions: {}", state.sessions());
                return SingleTransferResult.Unknown();
            }

            // If the SSTable doesn't contain any rows in the provided range, no streams delivered, nothing to activate
            if (state.sessions().isEmpty())
                return SingleTransferResult.Noop();

            return SingleTransferResult.Complete(plan.planId());
        };
        return LocalTransfers.instance().executor.submit(callable);
    }

    @Override
    public String toString()
    {
        return "CoordinatedTransfer{" +
               "transferId=" + transferId +
               ", range=" + range +
               ", streams=" + streams +
               ", sstables=" + sstables +
               ", activationId=" + activationId +
               '}';
    }

    public static final VerbHandler verbHandler = new VerbHandler();

    // move to LocalTransfers?
    public static class VerbHandler implements IVerbHandler<NoPayload>
    {
        @Override
        public void doVerb(Message<NoPayload> message) throws IOException
        {
            LocalTransfers.instance().executor.submit(() -> {
                MutationTrackingService.instance.streamUnreconciledTransfers(message.from());
                MessagingService.instance().respond(NoPayload.noPayload, message);
            }).rethrowIfFailed();
        }
    }
}
