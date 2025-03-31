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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.IntSupplier;

import org.agrona.collections.IntArrayList;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.PartitionPosition;
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
    public static final MutationTrackingService instance = new MutationTrackingService();

    private final ReadReconciliations reconciliations = new ReadReconciliations();
    private final ConcurrentHashMap<String, KeyspaceShards> shards = new ConcurrentHashMap<>();

    private volatile boolean started = false;

    private MutationTrackingService() {}

    // TODO (expected): implement a TCM ChangeListener
    public synchronized void start(ClusterMetadata metadata)
    {
        if (started)
            return;

        for (KeyspaceMetadata keyspace : metadata.schema.getKeyspaces())
            if (keyspace.useMutationTracking())
                shards.put(keyspace.name, KeyspaceShards.make(keyspace, metadata, this::nextHostLogId));
        started = true;
    }

    public synchronized boolean isStarted()
    {
        return started;
    }

    public void shutdownBlocking() throws InterruptedException
    {
        reconciliations.shutdownBlocking();
    }

    public ReadReconciliations reconciliations()
    {
        return reconciliations;
    }

    public MutationId nextMutationId(String keyspace, Token token)
    {
        return getOrCreate(keyspace).nextMutationId(token);
    }

    public void witnessedRemoteMutation(String keyspace, Token token, MutationId mutationId, InetAddressAndPort onHost)
    {
        getOrCreate(keyspace).witnessedRemoteMutation(token, mutationId, onHost);
    }

    public void startWriting(Mutation mutation)
    {
        getOrCreate(mutation.getKeyspaceName()).startWriting(mutation);
    }

    public void finishWriting(Mutation mutation)
    {
        getOrCreate(mutation.getKeyspaceName()).finishWriting(mutation);
    }

    public MutationSummary createSummaryForKey(DecoratedKey key, TableId tableId, boolean includePending)
    {
        return getOrCreate(tableId).createSummaryForKey(key, tableId, includePending);
    }

    public MutationSummary createSummaryForRange(AbstractBounds<PartitionPosition> range, TableId tableId, boolean includePending)
    {
        return getOrCreate(tableId).createSummaryForRange(range, tableId, includePending);
    }

    public MutationSummary createSummaryForRange(Range<Token> range, TableId tableId, boolean includePending)
    {
        return createSummaryForRange(Range.makeRowRange(range), tableId, includePending);
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

        void witnessedRemoteMutation(Token token, MutationId mutationId, InetAddressAndPort onHost)
        {
            lookUp(token).witnessedRemoteMutation(mutationId, onHost);
        }

        void startWriting(Mutation mutation)
        {
            lookUp(mutation).startWriting(mutation);
        }

        void finishWriting(Mutation mutation)
        {
            lookUp(mutation).finishWriting(mutation);
        }

        MutationSummary createSummaryForKey(DecoratedKey key, TableId tableId, boolean includePending)
        {
            MutationSummary.Builder builder = new MutationSummary.Builder(tableId);
            lookUp(key.getToken()).addSummaryForKey(key.getToken(), includePending, builder);
            return builder.build();
        }

        MutationSummary createSummaryForRange(AbstractBounds<PartitionPosition> range, TableId tableId, boolean includePending)
        {
            MutationSummary.Builder builder = new MutationSummary.Builder(tableId);
            forEachIntersectingShard(range, shard -> shard.addSummaryForRange(range, includePending, builder));
            return builder.build();
        }

        private void forEachIntersectingShard(AbstractBounds<PartitionPosition> bounds, Consumer<Shard> consumer)
        {
            ppShards.forEach((range, shard) -> {
                // TODO (expected): partial workaround - is there a better way to do this?
                //  SELECT * statements create Bounds[min,min], (PartitionKeyRestrictions.java:L174) not Range(min,min],
                //  which Ranges generally won't intersect with (Range.java:L148), so contains is used here to make it work
                if (bounds.contains(range.right) || range.intersects(bounds))
                    consumer.accept(shard);
            });
        }

        Shard lookUp(Mutation mutation)
        {
            return lookUp(mutation.key());
        }

        Shard lookUp(DecoratedKey key)
        {
            return lookUp(key.getToken());
        }

        Shard lookUp(Token token)
        {
            ClusterMetadata csm = ClusterMetadata.current();
            KeyspaceMetadata ksm = csm.schema.getKeyspaceMetadata(keyspace);
            Range<Token> range = ClusterMetadata.current().placements.get(ksm.params.replication).writes.forRange(token).range();
            return shards.get(range);
        }
    }
}
