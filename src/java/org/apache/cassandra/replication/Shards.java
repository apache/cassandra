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
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.tcm.ClusterMetadata;

// TODO (expected): persistence (handle restarts)
// TODO (expected): handle topology changes
public class Shards
{
    public static final Shards instance = new Shards();

    private final ConcurrentHashMap<String, KeyspaceShards> shards = new ConcurrentHashMap<>();

    public void load(ClusterMetadata metadata)
    {
        // TODO (expected): if tracking enabled (schema integration), replicated only
        // TODO (expected): implement a TCM ChangeListener
        for (KeyspaceMetadata keyspace : metadata.schema.getKeyspaces())
            shards.put(keyspace.name, KeyspaceShards.make(keyspace, metadata, this::nextHostLogId));
    }

    public Shard lookUp(String keyspace, Range<Token> range)
    {
        return getOrCreate(keyspace).lookUp(range);
    }

    public Shard lookUp(String keyspace, Token token)
    {
        return getOrCreate(keyspace).lookUp(token);
    }

    public void forEachIntersectingShard(String keyspace, AbstractBounds<PartitionPosition> range, Consumer<Shard> consumer)
    {
        getOrCreate(keyspace).forEachIntersectingShard(range, consumer);
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
            cluster.placements.get(keyspace.params.replication).writes.forEach((tokenRange, forRange) ->
            {
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

        Shard lookUp(Range<Token> range)
        {
            return shards.get(range);
        }

        Shard lookUp(Token token)
        {
            ClusterMetadata csm = ClusterMetadata.current();
            KeyspaceMetadata ksm = csm.schema.getKeyspaceMetadata(keyspace);
            Range<Token> range = ClusterMetadata.current().placements.get(ksm.params.replication).writes.forRange(token).range();
            return shards.get(range);
        }

        public void forEachIntersectingShard(AbstractBounds<PartitionPosition> bounds, Consumer<Shard> consumer)
        {
            ppShards.forEach((range, shard) -> {
                if (range.intersects(bounds))
                    consumer.accept(shard);
            });
        }
    }
}
