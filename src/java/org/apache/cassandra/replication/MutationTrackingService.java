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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.IntSupplier;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import org.agrona.collections.IntArrayList;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.reads.tracked.ReadReconciliations;
import org.apache.cassandra.tcm.ClusterMetadata;

// TODO (expected): persistence (handle restarts)
// TODO (expected): handle topology changes
public class MutationTrackingService
{
    private final ReadReconciliations reconciliations = new ReadReconciliations();
    private final ConcurrentHashMap<String, KeyspaceShards> shards = new ConcurrentHashMap<>();

    public static final MutationTrackingService instance = new MutationTrackingService();

    private MutationTrackingService() {}

    // TODO (expected): implement a TCM ChangeListener
    public void start()
    {
        // if we need to grab it earliier, go to tcm.Startup and add afterReplay() callbacks
        ClusterMetadata metadata = ClusterMetadata.current();

        for (KeyspaceMetadata keyspace : metadata.schema.getKeyspaces())
            if (keyspace.useMutationTracking())
                shards.put(keyspace.name, KeyspaceShards.make(keyspace, metadata, this::nextHostLogId));
    }

    public void shutdownBlocking() throws InterruptedException
    {
        reconciliations.shutdownBlocking();
    }

    public ReadReconciliations reconciliations()
    {
        return reconciliations;
    }

    MutationId nextMutationId(String keyspace, Token token)
    {
        return getOrCreate(keyspace).nextMutationId(token);
    }

    public void witnessedLocalMutation(Mutation mutation)
    {
        getOrCreate(mutation.getKeyspaceName()).witnessedLocalMutation(mutation);
    }

    public PendingWrite startWrite(Mutation mutation)
    {
        Preconditions.checkArgument(!mutation.id().isNone());
        return getOrCreate(mutation.getKeyspaceName()).startWrite(mutation);
    }

    public PendingRead startRead(ReadCommand command)
    {
        //noinspection DataFlowIssue
        Preconditions.checkArgument(Schema.instance.getKeyspaceMetadata(command.metadata().keyspace).useMutationTracking());
        return getOrCreate(command.metadata().keyspace).startRead(command);
    }

    public MutationSummary summaryForKey(TableId tableId, DecoratedKey key)
    {
        return getOrCreate(tableId).summaryForKey(tableId, key);
    }

    public MutationSummary summaryForRange(TableId tableId, AbstractBounds<PartitionPosition> range)
    {
        return getOrCreate(tableId).summaryForRange(tableId, range);
    }

    public MutationSummary summaryForRange(TableId tableId, Range<Token> range)
    {
        return summaryForRange(tableId, Range.makeRowRange(range));
    }

    private KeyspaceShards getOrCreate(TableId tableId)
    {
        //noinspection DataFlowIssue
        return getOrCreate(Schema.instance.getTableMetadata(tableId).keyspace);
    }

    private KeyspaceShards getOrCreate(String keyspace)
    {
        KeyspaceShards ks = shards.get(keyspace);
        if (ks != null)
            return ks;

        ClusterMetadata csm = ClusterMetadata.current();
        KeyspaceMetadata ksm = csm.schema.getKeyspaceMetadata(keyspace);
        return shards.computeIfAbsent(keyspace, ignore -> KeyspaceShards.make(ksm, csm, this::nextHostLogId));
    }

    // TODO (expected): durability
    int nextHostLogId()
    {
        return nextHostLogId.incrementAndGet();
    }
    private final AtomicInteger nextHostLogId = new AtomicInteger();

    private static class KeyspaceShards
    {
        private final String keyspace;
        private final Map<Range<Token>, Shard> shards;

        private transient final Map<Range<PartitionPosition>, Shard> ppShards;

        // TODO: do not hold onto mutation objects - extract the relevant fields, or fetch from journal if needed
        private final Map<MutationId, Mutation> pendingMutations = new ConcurrentHashMap<>();
        private final Set<ListeningPendingRead> pendingReads = Sets.newConcurrentHashSet();

        static KeyspaceShards make(KeyspaceMetadata keyspace, ClusterMetadata cluster, IntSupplier logIdProvider)
        {
            Map<Range<Token>, Shard> shards = new HashMap<>();
            cluster.placements.get(keyspace.params.replication).writes.forEach((tokenRange, forRange) -> {
               IntArrayList participants = new IntArrayList(forRange.size(), IntArrayList.DEFAULT_NULL_VALUE);
               for (InetAddressAndPort endpoint : forRange.endpoints())
                   participants.add(cluster.directory.peerId(endpoint).id());
               Shard shard = new Shard(keyspace.name, tokenRange, cluster.myNodeId().id(), new Participants(participants), forRange.lastModified(), logIdProvider);
               shards.put(tokenRange, shard);
            });
            return new KeyspaceShards(keyspace.name, shards);
        }

        KeyspaceShards(String keyspace, Map<Range<Token>, Shard> shards)
        {
            this.keyspace = keyspace;
            this.shards = shards;

            this.ppShards = new HashMap<>();
            shards.forEach((range, shard) -> ppShards.put(Range.makeRowRange(range), shard));
        }

        MutationId nextMutationId(Token token)
        {
            return lookUp(token).nextId();
        }

        void witnessedLocalMutation(Mutation mutation)
        {
            lookUp(mutation.key().getToken()).witnessedLocalMutation(mutation);
        }

        PendingWrite startWrite(Mutation mutation)
        {
            pendingMutations.put(mutation.id(), mutation);
            pendingReads.forEach(read -> read.onNewWrite(mutation));
            return () -> pendingMutations.remove(mutation.id());
        }

        PendingRead startRead(ReadCommand command)
        {
            ListeningPendingRead pendingRead = new ListeningPendingRead(command, pendingReads);
            pendingReads.add(pendingRead);
            pendingMutations.values().forEach(pendingRead::onNewWrite);
            return pendingRead;
        }

        MutationSummary summaryForKey(TableId tableId, DecoratedKey key)
        {
            MutationSummary.Builder builder = new MutationSummary.Builder(tableId);
            lookUp(key.getToken()).addSummaryForKey(builder, key.getToken());
            return builder.build();
        }

        MutationSummary summaryForRange(TableId tableId, AbstractBounds<PartitionPosition> range)
        {
            MutationSummary.Builder builder = new MutationSummary.Builder(tableId);
            forEachIntersectingShard(range, shard -> shard.addSummaryForRange(builder, range));
            return builder.build();
        }

        void forEachIntersectingShard(AbstractBounds<PartitionPosition> bounds, Consumer<Shard> consumer)
        {
            ppShards.forEach((range, shard) -> {
                if (range.intersects(bounds))
                    consumer.accept(shard);
            });
        }

        Shard lookUp(Token token)
        {
            ClusterMetadata csm = ClusterMetadata.current();
            KeyspaceMetadata ksm = csm.schema.getKeyspaceMetadata(keyspace);
            Range<Token> range = ClusterMetadata.current().placements.get(ksm.params.replication).writes.forRange(token).range();
            return shards.get(range);
        }
    }

    public interface PendingWrite extends AutoCloseable
    {
        PendingWrite NOOP = () -> {};

        @Override
        void close();
    }

    public interface PendingRead extends AutoCloseable
    {
        PendingRead NOOP = (iterator, summary) -> iterator;

        @Override
        default void close()
        {
        }

        /**
         * Returns mutations contained in the mutation summary that may not have been
         * applied to the memtable when it was read.
         */
        UnfilteredPartitionIterator augmentResponseWithPendingWrites(UnfilteredPartitionIterator iterator, MutationSummary summary);
    }

    public static class ListeningPendingRead implements PendingRead
    {
        private final ReadCommand command;
        private final Set<ListeningPendingRead> pendingReads;

        // TODO: do not hold onto mutation objects - extract the relevant fields, or fetch from journal if needed
        private final Map<MutationId, Mutation> pendingWrites = new ConcurrentHashMap<>();

        private ListeningPendingRead(ReadCommand command, Set<ListeningPendingRead> pendingReads)
        {
            this.command = command;
            this.pendingReads = pendingReads;
        }

        public void onNewWrite(Mutation mutation)
        {
            if (command.readsMutationContents(mutation))
                pendingWrites.put(mutation.id(), mutation);
        }

        @Override
        public void close()
        {
            pendingReads.remove(this);
        }

        public Set<MutationId> mutationIds()
        {
            return pendingWrites.keySet();
        }

        @Override
        public UnfilteredPartitionIterator augmentResponseWithPendingWrites(UnfilteredPartitionIterator iterator, MutationSummary summary)
        {
            if (pendingWrites.isEmpty() || summary.isEmpty())
                return iterator;

            List<Mutation> augmentingMutations = new ArrayList<>(pendingWrites.size());
            for (Mutation mutation : pendingWrites.values())
                if (summary.contains(mutation.id()))
                    augmentingMutations.add(mutation);
            return command.augmentResultWithMutations(iterator, augmentingMutations);
        }
    }
}
