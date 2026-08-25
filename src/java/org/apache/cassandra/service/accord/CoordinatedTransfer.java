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

package org.apache.cassandra.service.accord;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.local.Node;
import accord.local.Node.Id;
import accord.primitives.Ranges;
import accord.primitives.Txn;
import accord.topology.Topology;
import accord.utils.Invariants;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.streaming.CassandraOutgoingFile;
import org.apache.cassandra.dht.NormalizedRanges;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.RequestFailure;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.net.RequestCallbackWithFailure;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.accord.api.TokenKey;
import org.apache.cassandra.service.accord.serializers.TableMetadatas;
import org.apache.cassandra.service.accord.serializers.TableMetadatasAndKeys;
import org.apache.cassandra.service.accord.topology.AccordEndpointMapper;
import org.apache.cassandra.service.accord.txn.TxnQuery;
import org.apache.cassandra.service.accord.txn.TxnRead;
import org.apache.cassandra.service.accord.txn.TxnResult;
import org.apache.cassandra.streaming.OutgoingStream;
import org.apache.cassandra.streaming.StreamException;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.streaming.StreamPlan;
import org.apache.cassandra.streaming.StreamResultFuture;
import org.apache.cassandra.streaming.StreamState;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.concurrent.AsyncFuture;

import static accord.primitives.Txn.Kind.Read;

/**
 * Orchestrates the lifecycle of an Accord bulk data transfer for a shard where the current instance is
 * coordinating the transfer.
 * <p>
 * The transfer proceeds through these phases:
 * <ol>
 *   <li>
 *       <b>Streaming</b>
 *       The coordinator streams SSTables to all replicas in parallel. Replicas store received data in a "pending"
 *       location where it's persisted to disk but not yet visible to reads. Once all replicas in the shard have received
 *       their streams, the SSTables are activated with an ImportTxn, making them part of the live set and visible to reads.
 *   </li>
 *   <li>
 *       <b>ImportTxn</b>
 *       The coordinator creates an ImportTxn, whose semantics are equivalent to an Accord range read, but with special
 *       logic to perform the activation of the streamed SSTables. This means during the duration of the ImportTxn, concurrent
 *       read txn's may see inconsistent results. However, there is no data inconsistency, as read txn's are properly
 *       ordered against write txn's.
 *   </li>
 *   <li>
 *       <b>Garbage Collection of SSTables</b>
 *       Garbage collection of SSTables happens in a couple of different ways. We need to garbage collect any pending SSTables
 *       and also in memory state for LocalTransfers.
 *
 *       In the case where we are unable to complete the streaming phase, the coordinator coordinates garbage collection for all the replicas
 *       and sends out a TransferFailed message. If a node dies prior to receiving the garbage collection message from the
 *       coordinator, if streaming failed it will be cleaned up by removeUnfinishedLeftovers on startup. Otherwise, in the case where the full
 *       stream succeeded or the node is partitioned and did not get the TransferFailed message we rely on nodetool clean up to clean up the
 *       files in the pending directory.
 *
 *       In the case that the ImportTxn succeeds, we schedule the cleanup of any pending SSTables after we activate them, and then we remove
 *       the metadata from LocalTransfers.
 *
 *       If the ImportTxn fails, we do not know whether it has succeeded globally, so we rely on nodetool clean up to clean up the
 *       files in the pending directory after the fact.
 *   </li>
 * </ol>
 */
public class CoordinatedTransfer
{
    private static final Logger logger = LoggerFactory.getLogger(CoordinatedTransfer.class);

    String logPrefix()
    {
        return String.format("[CoordinatedTransfer #%s]", importID.toString());
    }

    private final UUID importID;
    private final TableMetadata tableMetadata;
    private final long streamingEpoch;
    private final boolean copyData;
    private final TokenRange allSSTableRanges;

    final Map<InetAddressAndPort, NodeStreamingMetadata> nodeStreamingContext;
    SingleTransferResult streamResult = SingleTransferResult.Init();

    public CoordinatedTransfer(UUID importID, TableMetadata tableMetadata, Map<InetAddressAndPort, NodeStreamingMetadata> nodeStreamingContext, long streamingEpoch, boolean copyData, TokenRange allSSTableRanges)
    {
        this.importID = importID;
        this.tableMetadata = tableMetadata;
        this.nodeStreamingContext = nodeStreamingContext;
        this.streamingEpoch = streamingEpoch;
        this.allSSTableRanges = allSSTableRanges;
        this.copyData = copyData;
    }

    public UUID importID()
    {
        return importID;
    }

    void execute()
    {
        logger.debug("{} Executing Accord bulk transfer {}", logPrefix(), this);
        LocalTransfers.instance().save(this);
        stream();

        try
        {
            performImportTxn();
        }
        catch (Exception e)
        {
            throw new RuntimeException("SSTable import failed locally; however the operation may still be applied by the recovery coordinator", e);
        }
    }

    private void performImportTxn()
    {
        TableMetadatas tables = TableMetadatas.of(tableMetadata);
        // TODO: The ImportTxn takes dependencies over the min and max keys of all the SSTables imported, which is an overapproximation
        TxnRead read = TxnRead.createImport(tables, allSSTableRanges, importID, streamResult.planId, streamingEpoch, copyData);
        TableMetadatasAndKeys tablesAndKeys = new TableMetadatasAndKeys(tables, read.keys());
        Txn txn = new Txn.InMemory(Read, read.keys(), read, TxnQuery.NONE, null, tablesAndKeys);
        IAccordService.IAccordResult<TxnResult> accordResult = AccordService.instance().coordinateAsync(tableMetadata.epoch.getEpoch(), txn, ConsistencyLevel.ALL, Dispatcher.RequestTime.forImmediateExecution());
        accordResult.awaitAndGet();
    }

    private void stream()
    {
        Future<Void> stream = LocalTransfers.instance().executor.submit(() -> {
            SingleTransferResult result;
            try
            {
                result = streamTask();
            }
            catch (StreamException | ExecutionException | InterruptedException | TimeoutException e)
            {
                Throwable cause = e instanceof ExecutionException ? e.getCause() : e;
                streamResult = SingleTransferResult.streamFailed(streamResult, streamResult.planId);
                throw Throwables.unchecked(cause);
            }

            streamResult = result;
            logger.info("{} Completed streaming to all nodes", logPrefix());
            return null;
        });

        try
        {
            stream.get();
            logger.info("{} All streaming completed successfully", logPrefix());
        }
        catch (InterruptedException | ExecutionException e)
        {
            logger.error("{} Failed transfer due to", logPrefix(), e);
            Throwable cause = e instanceof ExecutionException ? e.getCause() : e;
            maybeCleanupFailedStreams(cause);
            throw new RuntimeException("Failed streaming", Throwables.unchecked(cause));
        }
    }

    private SingleTransferResult streamTask() throws StreamException, ExecutionException, InterruptedException, TimeoutException
    {
        StreamPlan plan = new StreamPlan(StreamOperation.ACCORD_SSTABLE_IMPORT);

        // No need to flush, only using non-live SSTables already on disk
        plan.flushBeforeTransfer(false);

        for (Map.Entry<InetAddressAndPort, NodeStreamingMetadata> entry : nodeStreamingContext.entrySet())
        {
            InetAddressAndPort to = entry.getKey();
            NodeStreamingMetadata sstablesForNode = entry.getValue();
            List<Range<Token>> ranges = sstablesForNode.ranges;

            for (Map.Entry<SSTableReader, List<SSTableReader.PartitionPositionBounds>> positionsForSSTables : sstablesForNode.positionsForSSTables.entrySet())
            {
                SSTableReader sstable = positionsForSSTables.getKey();
                List<SSTableReader.PartitionPositionBounds> positions = positionsForSSTables.getValue();
                long estimatedKeys = sstable.estimatedKeysForRanges(ranges);
                OutgoingStream stream = new CassandraOutgoingFile(StreamOperation.ACCORD_SSTABLE_IMPORT, sstable.ref(), positions, ranges, estimatedKeys);
                plan.transferStreams(to, Collections.singleton(stream));
            }
        }

        long timeout = DatabaseDescriptor.getStreamTransferTaskTimeout().toMilliseconds();

        logger.info("{} Starting streaming transfer", logPrefix());
        streamResult = SingleTransferResult.streaming(streamResult, plan.planId());
        StreamResultFuture execute = plan.execute();
        StreamState state;
        try
        {
            state = execute.get(timeout, TimeUnit.MILLISECONDS);
            logger.debug("{} Completed streaming transfer", logPrefix());
        }
        catch (InterruptedException | ExecutionException | TimeoutException e)
        {
            logger.error("Stream session failed with error", e);
            throw e;
        }

        if (state.hasFailedSession() || state.hasAbortedSession())
            throw new StreamException(state, "Stream failed due to failed or aborted sessions");

        return SingleTransferResult.streamComplete(streamResult, plan.planId());
    }

    /**
     * This shouldn't throw an exception, even if we fail to notify peers of the streaming failure.
     */
    private void maybeCleanupFailedStreams(Throwable cause)
    {
        try
        {
            notifyFailure();
            LocalTransfers.instance().schedulePendingLocalTransferCleanup(streamResult.planId());
            LocalTransfers.instance().scheduleCoordinatedTransferCleanup(this);
        }
        catch (Throwable t)
        {
            if (cause != null)
                t.addSuppressed(cause);
            logger.error("{} Failed to notify peers of stream failure", logPrefix(), t);
        }
    }

    private void notifyFailure() throws ExecutionException, InterruptedException
    {
        class NotifyFailure extends AsyncFuture<Void> implements RequestCallbackWithFailure<NoPayload>
        {
            final Set<InetAddressAndPort> responses = ConcurrentHashMap.newKeySet(nodeStreamingContext.size());

            @Override
            public void onResponse(Message<NoPayload> msg)
            {
                responses.remove(msg.from());
                if (responses.isEmpty())
                    trySuccess(null);
            }

            @Override
            public void onFailure(InetAddressAndPort from, RequestFailure failure)
            {
                // Log but don't fail - best effort cleanup
                logger.warn("{} Failed to notify {} of transfer failure: {}", logPrefix(), from, failure);
                responses.remove(from);
                if (responses.isEmpty())
                    trySuccess(null);
            }
        }

        Map<InetAddressAndPort, Message<TransferFailed>> msgs = new HashMap<>();
        NotifyFailure notifyFailure = new NotifyFailure();

        TimeUUID planId = streamResult.planId;
        Invariants.require(planId != null);

        for (InetAddressAndPort to : nodeStreamingContext.keySet())
        {
            // Coordinator cleans up CoordinatedTransfer and PendingLocalTransfer separately, does not need to notify
            if (FBUtilities.getBroadcastAddressAndPort().equals(to))
                continue;

            logger.debug("{}, Notifying {} of transfer failure for plan {}", logPrefix(), to, streamResult.planId);
            notifyFailure.responses.add(to);
            msgs.put(to, Message.out(Verb.ACCORD_TRANSFER_FAILED_REQ, new TransferFailed(streamResult.planId)));
        }

        for (Map.Entry<InetAddressAndPort,Message<TransferFailed>> entry : msgs.entrySet())
            MessagingService.instance().sendWithCallback(entry.getValue(), entry.getKey(), notifyFailure);

        if (!msgs.isEmpty())
            notifyFailure.get();
    }

    public static class NodeStreamingMetadata
    {
        final Node.Id id;
        final List<Range<Token>> ranges;
        final Map<SSTableReader, List<SSTableReader.PartitionPositionBounds>> positionsForSSTables;

        public NodeStreamingMetadata(Node.Id id, Map<SSTableReader, List<SSTableReader.PartitionPositionBounds>> positionsForSSTables, List<Range<Token>> ranges)
        {
            this.id = id;
            this.positionsForSSTables = positionsForSSTables;
            this.ranges = ranges;
        }
    }

    static class SingleTransferResult
    {
        enum State
        {
            INIT,
            STREAMING,
            STREAM_FAILED,
            STREAM_COMPLETE;
        }

        final State state;
        private final TimeUUID planId;

        @VisibleForTesting
        SingleTransferResult(State state, TimeUUID planId)
        {
            this.state = state;
            this.planId = planId;
        }

        public static SingleTransferResult Init()
        {
            return new SingleTransferResult(State.INIT, null);
        }

        public static SingleTransferResult streaming(SingleTransferResult from, TimeUUID planId)
        {
            Invariants.require(from.state == State.INIT);
            return new SingleTransferResult(State.STREAMING, planId);
        }

        public static SingleTransferResult streamComplete(SingleTransferResult from, TimeUUID planId)
        {
            Invariants.require(from.state == State.STREAMING);
            return new SingleTransferResult(State.STREAM_COMPLETE, planId);
        }

        public static SingleTransferResult streamFailed(SingleTransferResult from, TimeUUID planId)
        {
            Invariants.require(from.state == State.STREAMING);
            return new SingleTransferResult(State.STREAM_FAILED, planId);
        }

        public TimeUUID planId()
        {
            return planId;
        }

        @Override
        public String toString()
        {
            return "SingleTransferResult{" +
                   "state=" + state +
                   ", planId=" + planId +
                   '}';
        }
    }

    public static TokenRange getTokenRangeSpanningSSTables(Collection<SSTableReader> sstables, TableMetadata metadata)
    {
        TokenKey minTokenKey = null;
        TokenKey maxTokenKey = null;
        for (SSTableReader sstable : sstables)
        {
            TokenKey startTokenKey = TokenKey.before(metadata.id, sstable.getFirst().getToken());
            TokenKey endTokenKey = new TokenKey(metadata.id, sstable.getLast().getToken());
            if (minTokenKey == null)
                minTokenKey = startTokenKey;
            else if (minTokenKey.compareTo(startTokenKey) > 0)
                minTokenKey = startTokenKey;

            if (maxTokenKey == null)
                maxTokenKey = endTokenKey;
            else if (maxTokenKey.compareTo(endTokenKey) < 0)
                maxTokenKey = endTokenKey;
        }

        return new TokenRange(minTokenKey, maxTokenKey);
    }

    public static Map<InetAddressAndPort, NodeStreamingMetadata> getNodeStreamingContext(Collection<SSTableReader> sstables, Topology topology, AccordEndpointMapper endpointMapper)
    {
        Map<InetAddressAndPort, NodeStreamingMetadata> nodeStreamingContext = new HashMap<>();

        for (Id nodeId : topology.nodes())
        {
            // Transform the ranges that each node owns to an input that can be used by
            // getPositionsForRanges
            Ranges rangesForNode = topology.rangesForNode(nodeId);
            List<Range<Token>> ranges = new ArrayList<>();
            for (accord.primitives.Range range : rangesForNode)
                ranges.add(((TokenRange) range).toKeyspaceRange());

            // Map from SSTables to the portion of the SSTable that the node owns
            Map<SSTableReader, List<SSTableReader.PartitionPositionBounds>> positionsForSSTables = new HashMap<>();

            for (SSTableReader sstable : sstables)
            {
                List<SSTableReader.PartitionPositionBounds> partition = sstable.getPositionsForRanges(ranges);
                if (!partition.isEmpty())
                    positionsForSSTables.put(sstable, partition);
            }

            if (!positionsForSSTables.isEmpty())
            {
                InetAddressAndPort endpoint = endpointMapper.mappedEndpointOrNull(nodeId);
                if (endpoint == null)
                    throw new RuntimeException("No endpoint for " + nodeId);
                nodeStreamingContext.put(endpoint, new NodeStreamingMetadata(nodeId, positionsForSSTables, NormalizedRanges.normalizedRanges(ranges)));
            }
        }

        return nodeStreamingContext;
    }
}
