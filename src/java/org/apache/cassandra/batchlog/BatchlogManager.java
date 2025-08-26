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
package org.apache.cassandra.batchlog;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutorPlus;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.UntypedResultSet.Row;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.RetryOnDifferentSystemException;
import org.apache.cassandra.exceptions.WriteFailureException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.hints.Hint;
import org.apache.cassandra.hints.HintsService;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaLayout;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.locator.Replicas;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageFlag;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.PreserveTimestamp;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.WriteResponseHandler;
import org.apache.cassandra.service.accord.IAccordService.IAccordResult;
import org.apache.cassandra.service.accord.txn.TxnResult;
import org.apache.cassandra.service.consensus.migration.ConsensusMigrationMutationHelper;
import org.apache.cassandra.service.consensus.migration.ConsensusMigrationMutationHelper.SplitMutations;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.ExecutorUtils;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MBeanWrapper;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.concurrent.Future;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.config.CassandraRelevantProperties.BATCHLOG_REPLAY_TIMEOUT_IN_MS;
import static org.apache.cassandra.cql3.QueryProcessor.executeInternal;
import static org.apache.cassandra.cql3.QueryProcessor.executeInternalWithPaging;
import static org.apache.cassandra.hints.HintsService.RETRY_ON_DIFFERENT_SYSTEM_UUID;
import static org.apache.cassandra.net.Verb.MUTATION_REQ;
import static org.apache.cassandra.service.accord.txn.TxnResult.Kind.retry_new_protocol;
import static org.apache.cassandra.service.consensus.migration.ConsensusMigrationMutationHelper.mutateWithAccordAsync;
import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;

public class BatchlogManager implements BatchlogManagerMBean
{
    public static final String MBEAN_NAME = "org.apache.cassandra.db:type=BatchlogManager";
    static final int DEFAULT_PAGE_SIZE = 128;

    private static final Logger logger = LoggerFactory.getLogger(BatchlogManager.class);
    public static final BatchlogManager instance = new BatchlogManager();
    public static final long BATCHLOG_REPLAY_TIMEOUT = BATCHLOG_REPLAY_TIMEOUT_IN_MS.getLong(DatabaseDescriptor.getWriteRpcTimeout(MILLISECONDS) * 2);

    private volatile long totalBatchesReplayed = 0; // no concurrency protection necessary as only written by replay thread.
    private volatile TimeUUID lastReplayedUuid = TimeUUID.minAtUnixMillis(0);

    // Single-thread executor service for scheduling and serializing log replay.
    private final ScheduledExecutorPlus batchlogTasks;

    private final RateLimiter rateLimiter = RateLimiter.create(Double.MAX_VALUE);

    private final AtomicBoolean isBatchlogReplayPaused = new AtomicBoolean(false);

    public BatchlogManager()
    {
        batchlogTasks = executorFactory().scheduled(false, "BatchlogTasks");
    }

    public void start()
    {
        MBeanWrapper.instance.registerMBean(this, MBEAN_NAME);

        batchlogTasks.scheduleWithFixedDelay(this::replayFailedBatches,
                                             StorageService.RING_DELAY_MILLIS,
                                             CassandraRelevantProperties.BATCHLOG_REPLAY_INTERVAL_MS.getLong(),
                                             MILLISECONDS);
    }

    public void shutdownAndWait(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException
    {
        ExecutorUtils.shutdownAndWait(timeout, unit, batchlogTasks);
    }

    public static void remove(TimeUUID id)
    {
        new Mutation(PartitionUpdate.fullPartitionDelete(SystemKeyspace.Batches,
                                                         id.toBytes(),
                                                         FBUtilities.timestampMicros(),
                                                         FBUtilities.nowInSeconds()))
            .apply();
    }

    public static void store(Batch batch)
    {
        store(batch, true);
    }

    public static void store(Batch batch, boolean durableWrites)
    {
        List<ByteBuffer> mutations = new ArrayList<>(batch.encodedMutations.size() + batch.decodedMutations.size());
        mutations.addAll(batch.encodedMutations);

        for (Mutation mutation : batch.decodedMutations)
        {
            try (DataOutputBuffer buffer = new DataOutputBuffer())
            {
                Mutation.serializer.serialize(mutation, buffer, MessagingService.current_version);
                mutations.add(buffer.buffer());
            }
            catch (IOException e)
            {
                // shouldn't happen
                throw new AssertionError(e);
            }
        }

        PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(SystemKeyspace.Batches, batch.id);
        builder.row()
               .timestamp(batch.creationTime)
               .add("version", MessagingService.current_version)
               .appendAll("mutations", mutations);

        builder.buildAsMutation().apply(durableWrites);
    }

    @VisibleForTesting
    public int countAllBatches()
    {
        String query = String.format("SELECT count(*) FROM %s.%s", SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.BATCHES);
        UntypedResultSet results = executeInternal(query);
        if (results == null || results.isEmpty())
            return 0;

        return (int) results.one().getLong("count");
    }

    public long getTotalBatchesReplayed()
    {
        return totalBatchesReplayed;
    }

    public void forceBatchlogReplay() throws Exception
    {
        logger.debug("Forcing batchlog replay");
        startBatchlogReplay().get();
        logger.debug("Finished forcing batchlog replay");
    }

    public Future<?> startBatchlogReplay()
    {
        // If a replay is already in progress this request will be executed after it completes.
        return batchlogTasks.submit(this::replayFailedBatches);
    }

    public void pauseReplay()
    {
        logger.debug("Paused batchlog replay");
        isBatchlogReplayPaused.set(true);
    }

    public void resumeReplay()
    {
        logger.debug("Resumed batchlog replay");
        isBatchlogReplayPaused.set(false);
    }

    private void replayFailedBatches()
    {
        if (isBatchlogReplayPaused.get())
        {
            logger.debug("Batch log replay is paused, skipping replay");
            return;
        }
        logger.trace("Started replayFailedBatches");

        // rate limit is in bytes per second. Uses Double.MAX_VALUE if disabled (set to 0 in cassandra.yaml).
        // max rate is scaled by the number of nodes in the cluster (same as for HHOM - see CASSANDRA-5272).
        int endpointsCount = ClusterMetadata.current().directory.allJoinedEndpoints().size();
        if (endpointsCount <= 0)
        {
            logger.trace("Replay cancelled as there are no peers in the ring.");
            return;
        }
        setRate(DatabaseDescriptor.getBatchlogReplayThrottleInKiB());

        TimeUUID limitUuid = TimeUUID.maxAtUnixMillis(currentTimeMillis() - getBatchlogTimeout());
        ColumnFamilyStore store = Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME).getColumnFamilyStore(SystemKeyspace.BATCHES);
        int pageSize = calculatePageSize(store);
        // There cannot be any live content where token(id) <= token(lastReplayedUuid) as every processed batch is
        // deleted, but the tombstoned content may still be present in the tables. To avoid walking over it we specify
        // token(id) > token(lastReplayedUuid) as part of the query.
        String query = String.format("SELECT id, mutations, version FROM %s.%s WHERE token(id) > token(?) AND token(id) <= token(?)",
                                     SchemaConstants.SYSTEM_KEYSPACE_NAME,
                                     SystemKeyspace.BATCHES);
        UntypedResultSet batches = executeInternalWithPaging(query, pageSize, lastReplayedUuid, limitUuid);

        processBatchlogEntries(batches, pageSize, rateLimiter);
        lastReplayedUuid = limitUuid;
        logger.trace("Finished replayFailedBatches");
    }

    /**
     * Sets the rate for the current rate limiter. When {@code throttleInKB} is 0, this sets the rate to
     * {@link Double#MAX_VALUE} bytes per second.
     *
     * @param throttleInKB throughput to set in KiB per second
     */
    public void setRate(final int throttleInKB)
    {
        int endpointsCount = ClusterMetadata.current().directory.allAddresses().size();
        if (endpointsCount > 0)
        {
            int endpointThrottleInKiB = throttleInKB / endpointsCount;
            double throughput = endpointThrottleInKiB == 0 ? Double.MAX_VALUE : endpointThrottleInKiB * 1024.0;
            if (rateLimiter.getRate() != throughput)
            {
                logger.debug("Updating batchlog replay throttle to {} KB/s, {} KB/s per endpoint", throttleInKB, endpointThrottleInKiB);
                rateLimiter.setRate(throughput);
            }
        }
    }

    // read less rows (batches) per page if they are very large
    static int calculatePageSize(ColumnFamilyStore store)
    {
        double averageRowSize = store.getMeanPartitionSize();
        if (averageRowSize <= 0)
            return DEFAULT_PAGE_SIZE;

        return (int) Math.max(1, Math.min(DEFAULT_PAGE_SIZE, 4 * 1024 * 1024 / averageRowSize));
    }

    private void processBatchlogEntries(UntypedResultSet batches, int pageSize, RateLimiter rateLimiter)
    {
        int positionInPage = 0;
        ArrayList<ReplayingBatch> unfinishedBatches = new ArrayList<>(pageSize);

        Set<UUID> hintedNodes = new HashSet<>();
        Set<TimeUUID> replayedBatches = new HashSet<>();
        Exception caughtException = null;
        int skipped = 0;

        // Sending out batches for replay without waiting for them, so that one stuck batch doesn't affect others
        for (UntypedResultSet.Row row : batches)
        {
            TimeUUID id = row.getTimeUUID("id");
            int version = row.getInt("version");
            try
            {
                dispatchBatch(rateLimiter, row, id, version, hintedNodes, unfinishedBatches);
            }
            catch (IOException e)
            {
                logger.warn("Skipped batch replay of {} due to {}", id, e.getMessage());
                caughtException = e;
                remove(id);
                ++skipped;
            }

            if (++positionInPage == pageSize)
            {
                // We have reached the end of a batch. To avoid keeping more than a page of mutations in memory,
                // finish processing the page before requesting the next row.
                finishAndClearBatches(unfinishedBatches, hintedNodes, replayedBatches);
                positionInPage = 0;
            }
        }

        // finalize the incomplete last page of batches
        if (positionInPage > 0)
            finishAndClearBatches(unfinishedBatches, hintedNodes, replayedBatches);
        else
            logger.trace("Had no batches to replay");

        if (caughtException != null)
            logger.warn(String.format("Encountered %d unexpected exceptions while sending out batches", skipped), caughtException);

        // to preserve batch guarantees, we must ensure that hints (if any) have made it to disk, before deleting the batches
        HintsService.instance.flushAndFsyncBlockingly(hintedNodes);

        // once all generated hints are fsynced, actually delete the batches
        replayedBatches.forEach(BatchlogManager::remove);
    }

    private void dispatchBatch(RateLimiter rateLimiter, Row row, TimeUUID id, int version, Set<UUID> hintedNodes, ArrayList<ReplayingBatch> unfinishedBatches) throws IOException
    {
        while (true)
        {
            ClusterMetadata cm = ClusterMetadata.current();
            try
            {
                ReplayingBatch batch = new ReplayingBatch(id, version, row.getList("mutations", BytesType.instance), cm);
                if (batch.replay(rateLimiter, hintedNodes))
                {
                    unfinishedBatches.add(batch);
                }
                else
                {
                    remove(id); // no write mutations were sent (either expired or all CFs involved truncated).
                    ++totalBatchesReplayed;
                }
            }
            catch (RetryOnDifferentSystemException e)
            {
                // Self apply can throw retry on different system
                // Barring bugs we should already have the latest cluster metadata needed to correctly
                // split the batch and retry since that is what was used to generate the exception
                continue;
            }
            break;
        }
    }

    private void finishAndClearBatches(ArrayList<ReplayingBatch> batches, Set<UUID> hintedNodes, Set<TimeUUID> replayedBatches)
    {
        // schedule hints for timed out deliveries
        for (ReplayingBatch batch : batches)
        {
            batch.finish(hintedNodes);
            replayedBatches.add(batch.id);
        }

        totalBatchesReplayed += batches.size();
        batches.clear();
    }

    public static long getBatchlogTimeout()
    {
        return BATCHLOG_REPLAY_TIMEOUT; // enough time for the actual write + BM removal mutation
    }

    private static class ReplayingBatch
    {
        private final TimeUUID id;
        private final long writtenAt;
        private final int unsplitGcGs;
        private final List<Mutation> normalMutations;
        private final List<Mutation> accordMutations;
        private final int replayedBytes;
        private final ClusterMetadata cm;

        private List<ReplayWriteResponseHandler<Mutation>> replayHandlers = ImmutableList.of();
        private IAccordResult<TxnResult> accordResult;
        @Nullable
        private Dispatcher.RequestTime accordTxnStart;

        ReplayingBatch(TimeUUID id, int version, List<ByteBuffer> serializedMutations, ClusterMetadata cm) throws IOException
        {
            this.id = id;
            this.writtenAt = id.unix(MILLISECONDS);
            List<Mutation> unsplitMutations = new ArrayList<>(serializedMutations.size());
            this.replayedBytes = addMutations(unsplitMutations, writtenAt, version, serializedMutations);
            unsplitGcGs = gcgs(unsplitMutations);
            SplitMutations<Mutation> splitMutations = ConsensusMigrationMutationHelper.splitMutationsIntoAccordAndNormal(cm, unsplitMutations);
            logger.trace("Replaying batch with Accord {} and normal {}", splitMutations.accordMutations(), splitMutations.normalMutations());
            normalMutations = splitMutations.normalMutations();
            accordMutations = splitMutations.accordMutations();
            if (accordMutations != null)
                accordTxnStart = new Dispatcher.RequestTime(Clock.Global.nanoTime());
            this.cm = cm;
        }

        public boolean replay(RateLimiter rateLimiter, Set<UUID> hintedNodes) throws IOException
        {
            logger.trace("Replaying batch {}", id);

            if ((normalMutations == null || normalMutations.isEmpty()) && (accordMutations == null || accordMutations.isEmpty()))
                return false;

            if (MILLISECONDS.toSeconds(writtenAt) + unsplitGcGs <= FBUtilities.nowInSeconds())
                return false;

            if (accordMutations != null)
            {
                accordTxnStart = accordTxnStart.withStartedAt(Clock.Global.nanoTime());
                accordResult = accordMutations != null ? mutateWithAccordAsync(cm, accordMutations, null, accordTxnStart, PreserveTimestamp.yes) : null;
            }

            if (normalMutations != null)
                replayHandlers = sendReplays(normalMutations, writtenAt, hintedNodes);

            rateLimiter.acquire(replayedBytes); // acquire afterwards, to not mess up ttl calculation.

            return replayHandlers.size() > 0 || accordMutations != null;
        }

        public void finish(Set<UUID> hintedNodes)
        {
            Throwable failure = null;
            // Check if the Accord mutations succeeded asynchronously
            try
            {
                if (accordResult != null)
                {
                    TxnResult.Kind kind = accordResult.awaitAndGet().kind();
                    if (kind == retry_new_protocol)
                        throw new RetryOnDifferentSystemException();
                }
            }
            catch (WriteTimeoutException | WriteFailureException | RetryOnDifferentSystemException e )
            {
                logger.trace("Failed replaying a batched mutation on Accord, will write a hint");
                logger.trace("Failure was : {}", e.getMessage());
                writeHintsForUndeliveredAccordTxns(hintedNodes);
            }
            catch (Exception e)
            {
                failure = Throwables.merge(failure, e);
            }

            try
            {
                for (int i = 0; i < replayHandlers.size(); i++)
                {
                    ReplayWriteResponseHandler<Mutation> handler = replayHandlers.get(i);
                    try
                    {
                        handler.get();
                    }
                    catch (WriteTimeoutException|WriteFailureException|RetryOnDifferentSystemException e)
                    {
                        logger.trace("Failed replaying a batched mutation to a node, will write a hint");
                        logger.trace("Failure was : {}", e.getMessage());
                        // writing hints for the rest to hints, starting from i
                        writeHintsForUndeliveredEndpoints(i, hintedNodes);
                        break;
                    }
                }
            }
            catch (Exception e)
            {
                logger.debug("Unexpected batchlog replay exception", e);
                failure = Throwables.merge(failure, e);
            }

            if (failure != null)
                throw Throwables.unchecked(failure);
        }

        private static int addMutations(List<Mutation> unsplitMutations, long writtenAt, int version, List<ByteBuffer> serializedMutations) throws IOException
        {
            int ret = 0;
            for (ByteBuffer serializedMutation : serializedMutations)
            {
                ret += serializedMutation.remaining();
                try (DataInputBuffer in = new DataInputBuffer(serializedMutation, true))
                {
                    addMutation(unsplitMutations, writtenAt, Mutation.serializer.deserialize(in, version));
                }
            }

            return ret;
        }

        // Remove CFs that have been truncated since. writtenAt and SystemTable#getTruncatedAt() both return millis.
        // We don't abort the replay entirely b/c this can be considered a success (truncated is same as delivered then
        // truncated.
        private static void addMutation(List<Mutation> unsplitMutations, long writtenAt, Mutation mutation)
        {
            for (TableId tableId : mutation.getTableIds())
                if (writtenAt <= SystemKeyspace.getTruncatedAt(tableId))
                    mutation = mutation.without(tableId);

            if (mutation != null)
                unsplitMutations.add(mutation);
        }

        // Write the hint assuming that when it is replayed it will probably be replayed
        // as an Accord transaction so no reason to record per endpoint hints for all the endpoints
        // Hints will still have to split and re-route on replay
        private void writeHintsForUndeliveredAccordTxns(Set<UUID> hintedNodes)
        {
            if (accordMutations == null)
                return;

            int gcgs = gcgs(accordMutations);

            // expired
            if (MILLISECONDS.toSeconds(writtenAt) + gcgs <= FBUtilities.nowInSeconds())
                return;

            for (Mutation m : accordMutations)
                HintsService.instance.write(ImmutableList.of(RETRY_ON_DIFFERENT_SYSTEM_UUID), Hint.create(m, writtenAt));
            hintedNodes.add(RETRY_ON_DIFFERENT_SYSTEM_UUID);
        }

        private void writeHintsForUndeliveredEndpoints(int startFrom, Set<UUID> hintedNodes)
        {
            if (normalMutations == null)
                return;

            int gcgs = gcgs(normalMutations);

            // expired
            if (MILLISECONDS.toSeconds(writtenAt) + gcgs <= FBUtilities.nowInSeconds())
                return;

            Set<UUID> nodesToHint = new HashSet<>();
            for (int i = startFrom; i < replayHandlers.size(); i++)
            {
                ReplayWriteResponseHandler<Mutation> handler = replayHandlers.get(i);
                Mutation undeliveredMutation = normalMutations.get(i);

                if (handler != null)
                {
                    for (InetAddressAndPort address : handler.undelivered)
                    {
                        UUID hostId = StorageService.instance.getHostIdForEndpoint(address);
                        if (null != hostId)
                            nodesToHint.add(hostId);
                    }
                    if (!nodesToHint.isEmpty())
                        HintsService.instance.write(nodesToHint, Hint.create(undeliveredMutation, writtenAt));
                    hintedNodes.addAll(nodesToHint);
                    nodesToHint.clear();
                }
            }
        }

        private static List<ReplayWriteResponseHandler<Mutation>> sendReplays(List<Mutation> mutations,
                                                                              long writtenAt,
                                                                              Set<UUID> hintedNodes)
        {
            List<ReplayWriteResponseHandler<Mutation>> handlers = new ArrayList<>(mutations.size());
            for (Mutation mutation : mutations)
            {
                ReplayWriteResponseHandler<Mutation> handler = sendSingleReplayMutation(mutation, writtenAt, hintedNodes);
                handlers.add(handler);
            }
            return handlers;
        }

        /**
         * We try to deliver the mutations to the replicas ourselves if they are alive and only resort to writing hints
         * when a replica is down or a write request times out.
         *
         * @return direct delivery handler to wait on
         */
        private static ReplayWriteResponseHandler<Mutation> sendSingleReplayMutation(final Mutation mutation,
                                                                                     long writtenAt,
                                                                                     Set<UUID> hintedNodes)
        {
            String ks = mutation.getKeyspaceName();
            Token tk = mutation.key().getToken();
            ClusterMetadata metadata = ClusterMetadata.current();
            KeyspaceMetadata keyspaceMetadata = metadata.schema.getKeyspaceMetadata(ks);

            // TODO: this logic could do with revisiting at some point, as it is unclear what its rationale is
            // we perform a local write, ignoring errors and inline in this thread (potentially slowing replay down)
            // effectively bumping CL for locally owned writes and also potentially stalling log replay if an error occurs
            // once we decide how it should work, it can also probably be simplified, and avoid constructing a ReplicaPlan directly
            ReplicaLayout.ForTokenWrite allReplias = ReplicaLayout.forTokenWriteLiveAndDown(metadata, keyspaceMetadata, tk);
            ReplicaPlan.ForWrite replicaPlan = forReplayMutation(metadata, Keyspace.open(ks), tk);

            Replica selfReplica = allReplias.all().selfIfPresent();
            if (selfReplica != null)
                mutation.apply();

            for (Replica replica : allReplias.all())
            {
                if (replica == selfReplica || replicaPlan.liveAndDown().contains(replica))
                    continue;

                UUID hostId = metadata.directory.peerId(replica.endpoint()).toUUID();
                if (null != hostId)
                {
                    HintsService.instance.write(hostId, Hint.create(mutation, writtenAt));
                    hintedNodes.add(hostId);
                }
            }

            ReplayWriteResponseHandler<Mutation> handler = new ReplayWriteResponseHandler<>(replicaPlan, mutation, Dispatcher.RequestTime.forImmediateExecution());
            Message<Mutation> message = Message.outWithFlag(MUTATION_REQ, mutation, MessageFlag.CALL_BACK_ON_FAILURE);
            for (Replica replica : replicaPlan.liveAndDown())
                MessagingService.instance().sendWriteWithCallback(message, replica, handler);
            return handler;
        }

        public static ReplicaPlan.ForWrite forReplayMutation(ClusterMetadata metadata, Keyspace keyspace, Token token)
        {
            ReplicaLayout.ForTokenWrite liveAndDown = ReplicaLayout.forTokenWriteLiveAndDown(metadata, keyspace.getMetadata(), token);
            Replicas.temporaryAssertFull(liveAndDown.all()); // TODO in CASSANDRA-14549

            Replica selfReplica = liveAndDown.all().selfIfPresent();
            ReplicaLayout.ForTokenWrite liveRemoteOnly = liveAndDown.filter(r -> FailureDetector.isReplicaAlive.test(r) && r != selfReplica);

            return new ReplicaPlan.ForWrite(keyspace, liveAndDown.replicationStrategy(),
                                            ConsistencyLevel.ONE, liveRemoteOnly.pending(), liveRemoteOnly.all(), liveRemoteOnly.all(), liveRemoteOnly.all(),
                                            (cm) -> forReplayMutation(cm, keyspace, token),
                                            metadata.epoch);
        }
        private static int gcgs(Collection<Mutation> mutations)
        {
            int gcgs = Integer.MAX_VALUE;
            for (Mutation mutation : mutations)
                gcgs = Math.min(gcgs, mutation.smallestGCGS());
            return gcgs;
        }

        /**
         * A wrapper of WriteResponseHandler that stores the addresses of the endpoints from
         * which we did not receive a successful response.
         */
        private static class ReplayWriteResponseHandler<T> extends WriteResponseHandler<T>
        {
            private final Set<InetAddressAndPort> undelivered = Collections.newSetFromMap(new ConcurrentHashMap<>());

            // TODO: should we be hinting here, since presumably batch log will retry? Maintaining historical behaviour for the moment.
            ReplayWriteResponseHandler(ReplicaPlan.ForWrite replicaPlan, Supplier<Mutation> hintOnFailure, Dispatcher.RequestTime requestTime)
            {
                super(replicaPlan, null, WriteType.UNLOGGED_BATCH, hintOnFailure, requestTime);
                Iterables.addAll(undelivered, replicaPlan.contacts().endpoints());
            }

            @Override
            protected int blockFor()
            {
                return this.replicaPlan.contacts().size();
            }

            @Override
            public void onResponse(Message<T> m)
            {
                boolean removed = undelivered.remove(m == null ? FBUtilities.getBroadcastAddressAndPort() : m.from());
                assert removed;
                super.onResponse(m);
            }
        }
    }
}
