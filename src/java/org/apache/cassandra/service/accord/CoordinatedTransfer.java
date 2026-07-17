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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.streaming.CassandraOutgoingFile;
import org.apache.cassandra.dht.NormalizedRanges;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.RequestFailure;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
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
 * Orchestrates the lifecycle of a bulk data transfer for Accord. Bulk data transfers only work on a single shard,
 * where the current instance is coordinating the transfer. This means that the node that the bulk data transfer
 * request is submitted to will only perform the transfer for the ranges of the SSTables that it owns.
 * <p>
 * The transfer proceeds through the following two phases:
 * <ol>
 *   <li>
 *       <b>Streaming</b>
 *       The coordinator streams SSTables to all replicas in parallel. Replicas store received data in a "pending"
 *       location where it's persisted to disk but not yet visible to reads. Once all replicas have received
 *       their streams, the SSTables are activated using a two-phase
 *       commit protocol, making them part of the live set and visible to reads.
 *   </li>
 *   <li>
 *       <b>ImportTxn</b>
 *       Once all replicas have received their streams, the SSTables are activated by creating an ImportTxn. An
 *       ImportTxn is equivalent to a range read txn that atomically activates the SSTables making them part of the live set
 *       and visible to reads. Note that because this is done with a range read txn and not a range write txn, clients
 *       performing concurrent reads against any nodes involved in the SSTable import can see inconsistent reads for
 *       a window of time because read txns can occur concurrently with other read txns. However, there will
 *       not be any data inconsistency, because read txns can not be interleaved with other write txns. In cases where
 *       the coordinator fails when performing the ImportTxn, the recovery protocol for Accord txn's is performed and
 *       a recovery coordinator will push the import to completion.
 *   </li>
 * </ol>
 *
 * For simplicity, the coordinator streams to itself rather than using direct file copy. This ensures we can use the
 * same lifecycle management for crash-safety and atomic add.
 * <p>
 * In cases where there is a topology change that is concurrent with the ImportTxn. We do not import the SSTables, as
 * the node with the streamed SSTables may no longer own those ranges and we rely on the client to reperform the import
 * operation.
 */
public class CoordinatedTransfer
{
    private static final Logger logger = LoggerFactory.getLogger(CoordinatedTransfer.class);

    String logPrefix()
    {
        return String.format("[CoordinatedTransfer #%s]", id);
    }

    private final Long id;
    private final String keyspace;
    private final TableMetadata tableMetadata;
    private final long streamingEpoch;
    private final TokenRange allSSTableRanges;

    final Map<InetAddressAndPort, SSTablesForNode> nodeStreamingContext;
    final ConcurrentMap<InetAddressAndPort, SingleTransferResult> streamResults;

    public CoordinatedTransfer(Long id, String keyspace, TableMetadata tableMetadata, Map<InetAddressAndPort, SSTablesForNode> nodeStreamingContext, long streamingEpoch, TokenRange allSSTableRanges)
    {
        this.id = id;
        this.keyspace = keyspace;
        this.tableMetadata = tableMetadata;
        this.nodeStreamingContext = nodeStreamingContext;
        this.streamingEpoch = streamingEpoch;
        this.allSSTableRanges = allSSTableRanges;

        this.streamResults = new ConcurrentHashMap<>(nodeStreamingContext.size());
        for (InetAddressAndPort ip: nodeStreamingContext.keySet())
            this.streamResults.put(ip, SingleTransferResult.Init());
    }

    public long id()
    {
        return id;
    }

    void execute()
    {
        logger.debug("{} Executing Accord bulk transfer {}", logPrefix(), this);
        LocalTransfers.instance().save(this);

        stream();

        AbstractReplicationStrategy ars = Keyspace.open(keyspace).getReplicationStrategy();
        int blockFor = ConsistencyLevel.ALL.blockFor(ars);
        int responses = 0;
        for (Map.Entry<InetAddressAndPort, SingleTransferResult> entry : streamResults.entrySet())
        {
            if (entry.getValue().state == SingleTransferResult.State.STREAM_COMPLETE)
                responses++;
        }

        Invariants.require(responses == blockFor);
        performImportTxn();
        LocalTransfers.instance().scheduleCoordinatedTransferCleanup(this);
    }

    private void performImportTxn()
    {
        TimeUUID[] planIds = streamResults.values().stream()
                                          .filter(result -> result.planId != null)
                                          .map(result -> result.planId)
                                          .toArray(TimeUUID[]::new);

        TableMetadatas tables = TableMetadatas.of(tableMetadata);
        TxnRead read = TxnRead.createImport(tables, allSSTableRanges, planIds, streamingEpoch);
        TableMetadatasAndKeys tablesAndKeys = new TableMetadatasAndKeys(tables, read.keys());
        Txn txn = new Txn.InMemory(Read, read.keys(), read, TxnQuery.NONE, null, tablesAndKeys);
        IAccordService.IAccordResult<TxnResult> accordResult = AccordService.instance().coordinateAsync(tableMetadata.epoch.getEpoch(), txn, ConsistencyLevel.ALL, Dispatcher.RequestTime.forImmediateExecution());
        accordResult.awaitAndGet();
    }

    private void stream()
    {
        List<Future<Void>> streaming = new ArrayList<>(streamResults.size());
        for (InetAddressAndPort to : streamResults.keySet())
        {
            Future<Void> stream = LocalTransfers.instance().executor.submit(() -> {
                SingleTransferResult result;
                try
                {
                    result = streamTask(to);
                }
                catch (StreamException | ExecutionException | InterruptedException | TimeoutException e)
                {
                    Throwable cause = e instanceof ExecutionException ? e.getCause() : e;
                    markStreamFailure(to, cause);
                    throw Throwables.unchecked(cause);
                }

                streamResults.put(to, result);
                logger.info("{} Completed streaming to {}, {}", logPrefix(), to, this);
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

    private SingleTransferResult streamTask(InetAddressAndPort to) throws StreamException, ExecutionException, InterruptedException, TimeoutException
    {
        StreamPlan plan = new StreamPlan(StreamOperation.ACCORD_SSTABLE_IMPORT);

        // No need to flush, only using non-live SSTables already on disk
        plan.flushBeforeTransfer(false);

        SSTablesForNode sstablesForNode = nodeStreamingContext.get(to);
        List<Range<Token>> ranges = nodeStreamingContext.get(to).ranges;

        for (Map.Entry<SSTableReader, List<SSTableReader.PartitionPositionBounds>> entry : sstablesForNode.positionsForSSTables.entrySet())
        {
            SSTableReader sstable = entry.getKey();
            List<SSTableReader.PartitionPositionBounds> positions = entry.getValue();
            long estimatedKeys = sstable.estimatedKeysForRanges(ranges);
            OutgoingStream stream = new CassandraOutgoingFile(StreamOperation.ACCORD_SSTABLE_IMPORT, sstable.ref(), positions, ranges, estimatedKeys);
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

    /**
     * This shouldn't throw an exception, even if we fail to notify peers of the streaming failure.
     */
    private void maybeCleanupFailedStreams(Throwable cause)
    {
        try
        {
            notifyFailure();
            LocalTransfers.instance().scheduleCoordinatedTransferCleanup(this);
        }
        catch (Throwable t)
        {
            if (cause != null)
                t.addSuppressed(cause);
            logger.error("{} Failed to notify peers of stream failure", logPrefix(), t);
        }
    }

    private void markStreamFailure(InetAddressAndPort to, Throwable cause)
    {
        TimeUUID planId;
        if (cause instanceof StreamException)
            planId = ((StreamException) cause).finalState.planId;
        else
            planId = null;
        streamResults.computeIfPresent(to, (peer, result) -> result.streamFailed(planId));
    }

    private void notifyFailure() throws ExecutionException, InterruptedException
    {
        class NotifyFailure extends AsyncFuture<Void> implements RequestCallbackWithFailure<NoPayload>
        {
            final Set<InetAddressAndPort> responses = ConcurrentHashMap.newKeySet(streamResults.size());

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

        for (Map.Entry<InetAddressAndPort, SingleTransferResult> entry : streamResults.entrySet())
        {
            InetAddressAndPort to = entry.getKey();
            // Coordinator cleans up CoordinatedTransfer and PendingLocalTransfer separately, does not need to notify
            if (FBUtilities.getBroadcastAddressAndPort().equals(to))
                continue;

            SingleTransferResult result = entry.getValue();
            if (result.planId == null)
            {
                logger.warn("{} Skipping notification of transfer failure to {} due to unknown planId", logPrefix(), to);
                continue;
            }

            logger.debug("{}, Notifying {} of transfer failure for plan {}", logPrefix(), to, result.planId);
            notifyFailure.responses.add(to);
            msgs.put(to, Message.out(Verb.TRACKED_TRANSFER_FAILED_REQ, new TransferFailed(result.planId)));
        }

        for (Map.Entry<InetAddressAndPort,Message<TransferFailed>> entry : msgs.entrySet())
            MessagingService.instance().sendWithCallback(entry.getValue(), entry.getKey(), notifyFailure);

        notifyFailure.get();
    }

    public static class SSTablesForNode
    {
        final Node.Id id;
        final List<Range<Token>> ranges;
        final Map<SSTableReader, List<SSTableReader.PartitionPositionBounds>> positionsForSSTables;

        public SSTablesForNode(Node.Id id, Map<SSTableReader, List<SSTableReader.PartitionPositionBounds>> positionsForSSTables, List<Range<Token>> ranges)
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
            STREAM_NOOP,
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

        @VisibleForTesting
        static SingleTransferResult StreamComplete(TimeUUID planId)
        {
            return new SingleTransferResult(State.STREAM_COMPLETE, planId);
        }

        @VisibleForTesting
        static SingleTransferResult Noop()
        {
            return new SingleTransferResult(State.STREAM_NOOP, null);
        }

        public SingleTransferResult streamFailed(TimeUUID planId)
        {
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

    public static Map<InetAddressAndPort, SSTablesForNode> getNodeStreamingContext(Collection<SSTableReader> sstables, Topology topology, AccordEndpointMapper endpointMapper)
    {
        Map<InetAddressAndPort, SSTablesForNode> nodeStreamingContext = new HashMap<>();

        for (Id nodeId : topology.nodes())
        {
            Ranges rangesForNode = topology.rangesForNode(nodeId);
            List<Range<Token>> ranges = new ArrayList<>();
            for (accord.primitives.Range range : rangesForNode)
                ranges.add(((TokenRange) range).toKeyspaceRange());

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
                nodeStreamingContext.put(endpoint, new SSTablesForNode(nodeId, positionsForSSTables, NormalizedRanges.normalizedRanges(ranges)));
            }
        }

        return nodeStreamingContext;
    }
}
