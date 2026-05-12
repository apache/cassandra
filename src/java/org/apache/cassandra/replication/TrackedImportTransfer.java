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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.streaming.CassandraOutgoingFile;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.RequestFailure;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.RequestCallbackWithFailure;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.streaming.OutgoingStream;
import org.apache.cassandra.streaming.StreamException;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.streaming.StreamPlan;
import org.apache.cassandra.streaming.StreamResultFuture;
import org.apache.cassandra.streaming.StreamState;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.concurrent.AsyncFuture;
import org.apache.cassandra.utils.concurrent.Future;

import static org.apache.cassandra.replication.ActivationRequest.Phase;
import static org.apache.cassandra.replication.CoordinatedTransfer.SingleTransferResult.State.COMMITTED;
import static org.apache.cassandra.replication.CoordinatedTransfer.SingleTransferResult.State.COMMITTING;
import static org.apache.cassandra.replication.CoordinatedTransfer.SingleTransferResult.State.PREPARE_FAILED;
import static org.apache.cassandra.replication.CoordinatedTransfer.SingleTransferResult.State.STREAM_COMPLETE;

/**
 * Tracked imports mostly follow the default coordination process. They are notable in two ways. The first is that 
 * {@link #prepare} strictly verifies that streaming has completed on replicas. The second is that activation is based
 * on the {@link #bounds} generated from {@link #sstables} rather than a sync range (as is the case with repair).
 * <p>
 * For simplicity, the coordinator streams to itself rather than using direct file copy. This ensures we can use the
 * same lifecycle management for crash-safety and atomic add.
 */
public class TrackedImportTransfer extends CoordinatedTransfer
{
    private static final Logger logger = LoggerFactory.getLogger(TrackedImportTransfer.class);

    final Collection<SSTableReader> sstables;
    private final ConsistencyLevel cl;
    final Map<SSTableReader, List<SSTableReader.PartitionPositionBounds>> positionForSSTables;

    @VisibleForTesting
    TrackedImportTransfer(Range<Token> range, MutationId id)
    {
        super(id, null, range);
        this.sstables = Collections.emptyList();
        this.positionForSSTables = Collections.emptyMap();
        this.cl = null;
    }

    TrackedImportTransfer(String keyspace, Range<Token> range, Participants participants, Collection<SSTableReader> sstables, Map<SSTableReader, List<SSTableReader.PartitionPositionBounds>> positionForSSTables, ConsistencyLevel cl, Supplier<MutationId> nextId)
    {
        super(nextId.get(), participants, keyspace, range);
        this.sstables = sstables;
        this.positionForSSTables = positionForSSTables;
        this.cl = cl;

        ClusterMetadata cm = ClusterMetadata.current();

        for (int i = 0; i < participants.size(); i++)
        {
            InetAddressAndPort addr = cm.directory.getNodeAddresses(new NodeId(participants.get(i))).broadcastAddress;
            this.streamResults.put(Pair.create(FBUtilities.getBroadcastAddressAndPort(), addr), SingleTransferResult.Init());
        }
    }

    @Override
    Bounds<Token> bounds()
    {
        return ActivatedTransfers.covering(sstables);
    }

    void execute()
    {
        logger.debug("{} Executing tracked import transfer {}", logPrefix(), this);
        TransferTrackingService.instance().save(this);
        stream();
    }

    private void stream()
    {
        // TODO: Don't stream multiple copies over the WAN, send one copy and indicate forwarding
        List<Future<Void>> streaming = new ArrayList<>(streamResults.size());
        for (Pair<InetAddressAndPort, InetAddressAndPort> pair : streamResults.keySet())
        {
            Future<Void> stream = TransferTrackingService.instance().executor.submit(() -> {
                stream(pair);
                return null;
            });
            streaming.add(stream);
        }

        // Wait for all streams to complete, so we can clean up after failures. If we exit at the first failure, a
        // future stream can complete.
        LinkedList<Throwable> failures = null;
        for (Future<Void> stream : streaming)
        {
            try
            {
                stream.get();
            }
            catch (InterruptedException | ExecutionException e)
            {
                if (failures == null)
                    failures = new LinkedList<>();
                failures.add(e);
                logger.error("{} Failed transfer due to", logPrefix(), e);
            }
        }

        if (failures != null && !failures.isEmpty())
        {
            Throwable failure = failures.element();
            Throwable cause = failure instanceof ExecutionException ? failure.getCause() : failure;
            maybeCleanupFailedStreams(cause);

            String msg = String.format("Failed streaming on %s instance(s): %s", failures.size(), failures);
            throw new RuntimeException(msg, Throwables.unchecked(cause));
        }

        logger.info("{} All streaming completed successfully", logPrefix());
    }

    private boolean sufficient()
    {
        AbstractReplicationStrategy ars = Keyspace.open(keyspace).getReplicationStrategy();
        int blockFor = cl.blockFor(ars);
        int responses = 0;
        for (SingleTransferResult result : streamResults.values())
        {
            if (result.state == STREAM_COMPLETE)
                responses++;
        }
        return responses >= blockFor;
    }

    void stream(Pair<InetAddressAndPort, InetAddressAndPort> pair)
    {
        SingleTransferResult result;
        try
        {
            result = streamTask(pair.right);
        }
        catch (StreamException | ExecutionException | InterruptedException | TimeoutException e)
        {
            Throwable cause = e instanceof ExecutionException ? e.getCause() : e;
            markStreamFailure(pair, cause);
            throw Throwables.unchecked(cause);
        }

        try
        {
            streamComplete(pair, result);
        }
        catch (ExecutionException | InterruptedException | TimeoutException e)
        {
            Throwable cause = e instanceof ExecutionException ? e.getCause() : e;
            throw Throwables.unchecked(cause);
        }
    }

    private void markStreamFailure(Pair<InetAddressAndPort, InetAddressAndPort> pair, Throwable cause)
    {
        TimeUUID planId;
        if (cause instanceof StreamException)
            planId = ((StreamException) cause).finalState.planId;
        else
            planId = null;
        streamResults.computeIfPresent(pair, (p, result) -> result.streamFailed(planId));
    }

    /**
     * This shouldn't throw an exception, even if we fail to notify peers of the streaming failure.
     */
    private void maybeCleanupFailedStreams(Throwable cause)
    {
        try
        {
            boolean purgeable = TransferTrackingService.instance().purger.test(this);
            if (!purgeable)
                return;

            TransferTrackingService.instance().scheduleCleanup();
            notifyFailure();
        }
        catch (Throwable t)
        {
            if (cause != null)
                t.addSuppressed(cause);
            logger.error("{} Failed to notify peers of stream failure", logPrefix(), t);
        }
    }

    private void streamComplete(Pair<InetAddressAndPort, InetAddressAndPort> pair, SingleTransferResult result) throws ExecutionException, InterruptedException, TimeoutException
    {
        streamResults.put(pair, result);
        logger.info("{} Completed streaming for pair {}, {}", logPrefix(), pair, this);
        maybeActivate();
    }

    synchronized void maybeActivate()
    {
        // If any activations have already been sent out, send new activations to any received plans that have not yet
        // been activated
        boolean anyActivated = false;
        Set<Pair<InetAddressAndPort, InetAddressAndPort>> awaitingActivation = new HashSet<>();
        for (Map.Entry<Pair<InetAddressAndPort, InetAddressAndPort>, SingleTransferResult> entry : streamResults.entrySet())
        {
            Pair<InetAddressAndPort, InetAddressAndPort> peer = entry.getKey();
            SingleTransferResult result = entry.getValue();
            if (result.state == COMMITTING || result.state == COMMITTED)
            {
                anyActivated = true;
            }
            else if (result.state == STREAM_COMPLETE)
                awaitingActivation.add(peer);
        }
        if (anyActivated && !awaitingActivation.isEmpty())
        {
            logger.debug("{} Transfer already activated on some peers, sending activations to remaining: {}", logPrefix(), awaitingActivation);
            activate(awaitingActivation);
            return;
        }
        // If no activations have been sent out, check whether we have enough planIds back to meet the required CL
        else if (sufficient())
        {
            Set<Pair<InetAddressAndPort, InetAddressAndPort>> pairs = new HashSet<>();
            for (Map.Entry<Pair<InetAddressAndPort, InetAddressAndPort>, SingleTransferResult> entry : streamResults.entrySet())
            {
                Pair<InetAddressAndPort, InetAddressAndPort> pair = entry.getKey();
                SingleTransferResult result = entry.getValue();
                if (result.state == STREAM_COMPLETE)
                    pairs.add(pair);
            }
            logger.debug("{} Transfer meets consistency level {}, sending activations to {}", logPrefix(), cl, pairs);
            activate(pairs);
            return;
        }

        logger.debug("{} Nothing to activate", logPrefix());
    }

    protected void prepare(Collection<Pair<InetAddressAndPort, InetAddressAndPort>> targets)
    {
        // First phase ensures data is present on disk, then second phase does the actual import. This ensures that if
        // something goes wrong (like a topology change during import), we don't have divergence.
        class Prepare extends AsyncFuture<Void> implements RequestCallbackWithFailure<ActivationResponse>
        {
            final AtomicInteger responses = new AtomicInteger(0);

            public Prepare()
            {
                responses.addAndGet(targets.size());
            }

            @Override
            public void onResponse(Message<ActivationResponse> msg)
            {
                logger.debug("{} Got prepare response from: {}", logPrefix(), msg.from());
                if (responses.decrementAndGet() == 0)
                    trySuccess(null);
            }

            @Override
            public void onFailure(InetAddressAndPort from, RequestFailure failure)
            {
                logger.debug("{} Got prepare failure {} from {}", logPrefix(), failure, from);

                // Any failure fails the whole transfer here, so marking all results for streams to this remote as
                // failed is harmless and allows the purging logic to clean up doomed transfer artifacts. 
                streamResults.forEach((pair, result) -> {
                    if (pair.right.equals(from) && result.canTransition(PREPARE_FAILED))
                        streamResults.put(pair, result.prepareFailed());
                });

                tryFailure(new RuntimeException("Tracked transfer failed during PREPARE on " + from + " due to " + failure.reason));
            }
        }

        Prepare prepare = new Prepare();
        for (Pair<InetAddressAndPort, InetAddressAndPort> target : targets)
        {
            ActivationRequest activation = createActivation(target, Phase.PREPARE);
            Message<ActivationRequest> msg = Message.out(Verb.MT_TRANSFER_ACTIVATE_REQ, activation);
            logger.debug("{} Sending prepare {} to peer {}", logPrefix(), activation, target.right);
            MessagingService.instance().sendWithCallback(msg, target.right, prepare);
            streamResults.computeIfPresent(target, (peer0, result) -> result.preparing());
        }
        try
        {
            prepare.get();
        }
        catch (InterruptedException | ExecutionException e)
        {
            Throwable cause = e instanceof ExecutionException ? e.getCause() : e;
            throw Throwables.unchecked(cause);
        }
    }

    @Override
    protected ActivationRequest createActivation(Pair<InetAddressAndPort, InetAddressAndPort> pair, Phase phase)
    {
        return new ActivationRequest(StreamOperation.IMPORT, pair, phase, id(), ClusterMetadata.current().myNodeId(), range, keyspace, streamResults.get(pair).planId());
    }

    private SingleTransferResult streamTask(InetAddressAndPort to) throws StreamException, ExecutionException, InterruptedException, TimeoutException
    {
        StreamPlan plan = new StreamPlan(StreamOperation.IMPORT);

        // No need to flush, only using non-live SSTables already on disk
        plan.flushBeforeTransfer(false);

        for (SSTableReader sstable : sstables)
        {
            List<Range<Token>> ranges = Collections.singletonList(range);
            List<SSTableReader.PartitionPositionBounds> positions = positionForSSTables.get(sstable);
            long estimatedKeys = sstable.estimatedKeysForRanges(ranges);
            OutgoingStream stream = new CassandraOutgoingFile(StreamOperation.IMPORT, sstable.ref(), positions, ranges, estimatedKeys);
            plan.transferStreams(to, Collections.singleton(stream));
        }

        long timeout = DatabaseDescriptor.getStreamTransferTaskTimeout().toMilliseconds();

        logger.info("{} Starting streaming transfer {} to peer {}", logPrefix(), this, to);
        StreamResultFuture execute = plan.execute();
        StreamState state;
        try
        {
            state = execute.get(timeout, TimeUnit.MILLISECONDS);
            logger.debug("{} Completed streaming transfer {} to peer {}", logPrefix(), this, to);
        }
        catch (InterruptedException | ExecutionException | TimeoutException e)
        {
            logger.error("Stream session failed with error", e);
            throw e;
        }

        if (state.hasFailedSession() || state.hasAbortedSession())
            throw new StreamException(state, "Stream failed due to failed or aborted sessions");

        // If the SSTable doesn't contain any rows in the provided range, no streams delivered, nothing to activate
        if (state.sessions().isEmpty())
            return SingleTransferResult.Noop();

        return SingleTransferResult.StreamComplete(plan.planId());
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == null || getClass() != o.getClass()) return false;
        TrackedImportTransfer that = (TrackedImportTransfer) o;
        return Objects.equals(keyspace, that.keyspace) && Objects.equals(range, that.range) && cl == that.cl && Objects.equals(streamResults, that.streamResults);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(keyspace, range, cl, streamResults);
    }

    @Override
    public String toString()
    {
        return "TrackedImportTransfer{" +
               "keyspace='" + keyspace + '\'' +
               ", range=" + range +
               ", cl=" + cl +
               ", streamResults=" + streamResults +
               ", sstables=" + sstables +
               ", streamResults=" + streamResults +
               '}';
    }
}
