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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Collections2;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.DebuggableScheduledThreadPoolExecutor;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.dht.Token;
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
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.WriteResponseHandler;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MBeanWrapper;
import org.apache.cassandra.utils.UUIDGen;

import static com.google.common.collect.Iterables.transform;
import static org.apache.cassandra.cql3.QueryProcessor.executeInternal;
import static org.apache.cassandra.cql3.QueryProcessor.executeInternalWithPaging;

public class BatchlogManager implements BatchlogManagerMBean
{
    public static final String MBEAN_NAME = "org.apache.cassandra.db:type=BatchlogManager";
    private static final long REPLAY_INTERVAL = 10 * 1000; // milliseconds
    static final int DEFAULT_PAGE_SIZE = 128;

    private static final Logger logger = LoggerFactory.getLogger(BatchlogManager.class);
    public static final BatchlogManager instance = new BatchlogManager();
    public static final long BATCHLOG_REPLAY_TIMEOUT = Long.getLong("cassandra.batchlog.replay_timeout_in_ms", DatabaseDescriptor.getWriteRpcTimeout() * 2);

    private volatile long totalBatchesReplayed = 0; // no concurrency protection necessary as only written by replay thread.
    private volatile UUID lastReplayedUuid = UUIDGen.minTimeUUID(0);

    // Single-thread executor service for scheduling and serializing log replay.
    private final ScheduledExecutorService batchlogTasks;

    private final RateLimiter rateLimiter = RateLimiter.create(Double.MAX_VALUE);

    public BatchlogManager()
    {
        ScheduledThreadPoolExecutor executor = new DebuggableScheduledThreadPoolExecutor("BatchlogTasks");
        executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        batchlogTasks = executor;
    }

    public void start()
    {
        MBeanWrapper.instance.registerMBean(this, MBEAN_NAME);

        batchlogTasks.scheduleWithFixedDelay(this::replayFailedBatches,
                                             StorageService.RING_DELAY,
                                             REPLAY_INTERVAL,
                                             TimeUnit.MILLISECONDS);
    }

    public void shutdown() throws InterruptedException
    {
        batchlogTasks.shutdown();
        batchlogTasks.awaitTermination(60, TimeUnit.SECONDS);
    }

    public static void remove(UUID id)
    {
        new Mutation(PartitionUpdate.fullPartitionDelete(SystemKeyspace.Batches,
                                                         UUIDType.instance.decompose(id),
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
        startBatchlogReplay().get();
    }

    public Future<?> startBatchlogReplay()
    {
        // If a replay is already in progress this request will be executed after it completes.
        return batchlogTasks.submit(this::replayFailedBatches);
    }

    void performInitialReplay() throws InterruptedException, ExecutionException
    {
        // Invokes initial replay. Used for testing only.
        batchlogTasks.submit(this::replayFailedBatches).get();
    }

    private void replayFailedBatches()
    {
        logger.trace("Started replayFailedBatches");

        // rate limit is in bytes per second. Uses Double.MAX_VALUE if disabled (set to 0 in cassandra.yaml).
        // max rate is scaled by the number of nodes in the cluster (same as for HHOM - see CASSANDRA-5272).
        int endpointsCount = StorageService.instance.getTokenMetadata().getSizeOfAllEndpoints();
        if (endpointsCount <= 0)
        {
            logger.trace("Replay cancelled as there are no peers in the ring.");
            return;
        }
        setRate(DatabaseDescriptor.getBatchlogReplayThrottleInKB());

        UUID limitUuid = UUIDGen.maxTimeUUID(System.currentTimeMillis() - getBatchlogTimeout());
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
     * @param throttleInKB throughput to set in KB per second
     */
    public void setRate(final int throttleInKB)
    {
        int endpointsCount = StorageService.instance.getTokenMetadata().getSizeOfAllEndpoints();
        if (endpointsCount > 0)
        {
            int endpointThrottleInKB = throttleInKB / endpointsCount;
            double throughput = endpointThrottleInKB == 0 ? Double.MAX_VALUE : endpointThrottleInKB * 1024.0;
            if (rateLimiter.getRate() != throughput)
            {
                logger.debug("Updating batchlog replay throttle to {} KB/s, {} KB/s per endpoint", throttleInKB, endpointThrottleInKB);
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

        Set<InetAddressAndPort> hintedNodes = new HashSet<>();
        Set<UUID> replayedBatches = new HashSet<>();
        Exception caughtException = null;
        int skipped = 0;

        // Sending out batches for replay without waiting for them, so that one stuck batch doesn't affect others
        for (UntypedResultSet.Row row : batches)
        {
            UUID id = row.getUUID("id");
            int version = row.getInt("version");
            try
            {
                ReplayingBatch batch = new ReplayingBatch(id, version, row.getList("mutations", BytesType.instance));
                if (batch.replay(rateLimiter, hintedNodes) > 0)
                {
                    unfinishedBatches.add(batch);
                }
                else
                {
                    remove(id); // no write mutations were sent (either expired or all CFs involved truncated).
                    ++totalBatchesReplayed;
                }
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

        finishAndClearBatches(unfinishedBatches, hintedNodes, replayedBatches);

        if (caughtException != null)
            logger.warn(String.format("Encountered %d unexpected exceptions while sending out batches", skipped), caughtException);

        // to preserve batch guarantees, we must ensure that hints (if any) have made it to disk, before deleting the batches
        HintsService.instance.flushAndFsyncBlockingly(transform(hintedNodes, StorageService.instance::getHostIdForEndpoint));

        // once all generated hints are fsynced, actually delete the batches
        replayedBatches.forEach(BatchlogManager::remove);
    }

    private void finishAndClearBatches(ArrayList<ReplayingBatch> batches, Set<InetAddressAndPort> hintedNodes, Set<UUID> replayedBatches)
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
        private final UUID id;
        private final long writtenAt;
        private final List<Mutation> mutations;
        private final int replayedBytes;

        private List<ReplayWriteResponseHandler<Mutation>> replayHandlers;

        ReplayingBatch(UUID id, int version, List<ByteBuffer> serializedMutations) throws IOException
        {
            this.id = id;
            this.writtenAt = UUIDGen.unixTimestamp(id);
            this.mutations = new ArrayList<>(serializedMutations.size());
            this.replayedBytes = addMutations(version, serializedMutations);
        }

        public int replay(RateLimiter rateLimiter, Set<InetAddressAndPort> hintedNodes) throws IOException
        {
            logger.trace("Replaying batch {}", id);

            if (mutations.isEmpty())
                return 0;

            int gcgs = gcgs(mutations);
            if (TimeUnit.MILLISECONDS.toSeconds(writtenAt) + gcgs <= FBUtilities.nowInSeconds())
                return 0;

            replayHandlers = sendReplays(mutations, writtenAt, hintedNodes);

            rateLimiter.acquire(replayedBytes); // acquire afterwards, to not mess up ttl calculation.

            return replayHandlers.size();
        }

        public void finish(Set<InetAddressAndPort> hintedNodes)
        {
            for (int i = 0; i < replayHandlers.size(); i++)
            {
                ReplayWriteResponseHandler<Mutation> handler = replayHandlers.get(i);
                try
                {
                    handler.get();
                }
                catch (WriteTimeoutException|WriteFailureException e)
                {
                    logger.trace("Failed replaying a batched mutation to a node, will write a hint");
                    logger.trace("Failure was : {}", e.getMessage());
                    // writing hints for the rest to hints, starting from i
                    writeHintsForUndeliveredEndpoints(i, hintedNodes);
                    return;
                }
            }
        }

        private int addMutations(int version, List<ByteBuffer> serializedMutations) throws IOException
        {
            int ret = 0;
            for (ByteBuffer serializedMutation : serializedMutations)
            {
                ret += serializedMutation.remaining();
                try (DataInputBuffer in = new DataInputBuffer(serializedMutation, true))
                {
                    addMutation(Mutation.serializer.deserialize(in, version));
                }
            }

            return ret;
        }

        // Remove CFs that have been truncated since. writtenAt and SystemTable#getTruncatedAt() both return millis.
        // We don't abort the replay entirely b/c this can be considered a success (truncated is same as delivered then
        // truncated.
        private void addMutation(Mutation mutation)
        {
            for (TableId tableId : mutation.getTableIds())
                if (writtenAt <= SystemKeyspace.getTruncatedAt(tableId))
                    mutation = mutation.without(tableId);

            if (!mutation.isEmpty())
                mutations.add(mutation);
        }

        private void writeHintsForUndeliveredEndpoints(int startFrom, Set<InetAddressAndPort> hintedNodes)
        {
            int gcgs = gcgs(mutations);

            // expired
            if (TimeUnit.MILLISECONDS.toSeconds(writtenAt) + gcgs <= FBUtilities.nowInSeconds())
                return;

            for (int i = startFrom; i < replayHandlers.size(); i++)
            {
                ReplayWriteResponseHandler<Mutation> handler = replayHandlers.get(i);
                Mutation undeliveredMutation = mutations.get(i);

                if (handler != null)
                {
                    hintedNodes.addAll(handler.undelivered);
                    HintsService.instance.write(Collections2.transform(handler.undelivered, StorageService.instance::getHostIdForEndpoint),
                                                Hint.create(undeliveredMutation, writtenAt));
                }
            }
        }

        private static List<ReplayWriteResponseHandler<Mutation>> sendReplays(List<Mutation> mutations,
                                                                              long writtenAt,
                                                                              Set<InetAddressAndPort> hintedNodes)
        {
            List<ReplayWriteResponseHandler<Mutation>> handlers = new ArrayList<>(mutations.size());
            for (Mutation mutation : mutations)
            {
                ReplayWriteResponseHandler<Mutation> handler = sendSingleReplayMutation(mutation, writtenAt, hintedNodes);
                if (handler != null)
                    handlers.add(handler);
            }
            return handlers;
        }

        /**
         * We try to deliver the mutations to the replicas ourselves if they are alive and only resort to writing hints
         * when a replica is down or a write request times out.
         *
         * @return direct delivery handler to wait on or null, if no live nodes found
         */
        private static ReplayWriteResponseHandler<Mutation> sendSingleReplayMutation(final Mutation mutation,
                                                                                     long writtenAt,
                                                                                     Set<InetAddressAndPort> hintedNodes)
        {
            String ks = mutation.getKeyspaceName();
            Keyspace keyspace = Keyspace.open(ks);
            Token tk = mutation.key().getToken();

            // TODO: this logic could do with revisiting at some point, as it is unclear what its rationale is
            // we perform a local write, ignoring errors and inline in this thread (potentially slowing replay down)
            // effectively bumping CL for locally owned writes and also potentially stalling log replay if an error occurs
            // once we decide how it should work, it can also probably be simplified, and avoid constructing a ReplicaPlan directly
            ReplicaLayout.ForTokenWrite liveAndDown = ReplicaLayout.forTokenWriteLiveAndDown(keyspace, tk);
            Replicas.temporaryAssertFull(liveAndDown.all()); // TODO in CASSANDRA-14549

            Replica selfReplica = liveAndDown.all().selfIfPresent();
            if (selfReplica != null)
                mutation.apply();

            ReplicaLayout.ForTokenWrite liveRemoteOnly = liveAndDown.filter(
                    r -> FailureDetector.isReplicaAlive.test(r) && r != selfReplica);

            for (Replica replica : liveAndDown.all())
            {
                if (replica == selfReplica || liveRemoteOnly.all().contains(replica))
                    continue;
                hintedNodes.add(replica.endpoint());
                HintsService.instance.write(StorageService.instance.getHostIdForEndpoint(replica.endpoint()),
                        Hint.create(mutation, writtenAt));
            }

            ReplicaPlan.ForTokenWrite replicaPlan = new ReplicaPlan.ForTokenWrite(keyspace, ConsistencyLevel.ONE,
                    liveRemoteOnly.pending(), liveRemoteOnly.all(), liveRemoteOnly.all(), liveRemoteOnly.all());
            ReplayWriteResponseHandler<Mutation> handler = new ReplayWriteResponseHandler<>(replicaPlan, System.nanoTime());
            MessageOut<Mutation> message = mutation.createMessage();
            for (Replica replica : liveRemoteOnly.all())
                MessagingService.instance().sendWriteRR(message, replica, handler, false);
            return handler;
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
         * which we did not receive a successful reply.
         */
        private static class ReplayWriteResponseHandler<T> extends WriteResponseHandler<T>
        {
            private final Set<InetAddressAndPort> undelivered = Collections.newSetFromMap(new ConcurrentHashMap<>());

            ReplayWriteResponseHandler(ReplicaPlan.ForTokenWrite replicaPlan, long queryStartNanoTime)
            {
                super(replicaPlan, null, WriteType.UNLOGGED_BATCH, queryStartNanoTime);
                Iterables.addAll(undelivered, replicaPlan.contacts().endpoints());
            }

            @Override
            protected int blockFor()
            {
                return this.replicaPlan.contacts().size();
            }

            @Override
            public void response(MessageIn<T> m)
            {
                boolean removed = undelivered.remove(m == null ? FBUtilities.getBroadcastAddressAndPort() : m.from);
                assert removed;
                super.response(m);
            }
        }
    }
}
