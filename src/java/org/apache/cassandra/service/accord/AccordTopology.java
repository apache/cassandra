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

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import accord.primitives.Ranges;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import accord.local.Node;
import accord.topology.Shard;
import accord.topology.Topology;
import accord.utils.Invariants;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.*;
import org.apache.cassandra.service.accord.api.AccordRoutingKey.SentinelKey;
import org.apache.cassandra.service.accord.api.AccordRoutingKey.TokenKey;
import org.apache.cassandra.service.accord.fastpath.FastPathStrategy;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.membership.Directory;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.ownership.DataPlacement;
import org.apache.cassandra.tcm.ownership.DataPlacements;
import org.apache.cassandra.tcm.ownership.VersionedEndpoints;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Deterministically computes accord topology from a ClusterMetadata instance
 */
public class AccordTopology
{
    public static Node.Id tcmIdToAccord(NodeId nodeId)
    {
        return new Node.Id(nodeId.id());
    }

    private static class ShardLookup extends HashMap<accord.primitives.Range, Shard>
    {
        private Shard createOrReuse(accord.primitives.Range range, List<Node.Id> nodes, Set<Node.Id> fastPathElectorate, Set<Node.Id> joining)
        {
            Shard prev = get(range);
            if (prev != null
                && Objects.equals(prev.nodes, nodes)
                && Objects.equals(prev.fastPathElectorate, fastPathElectorate)
                && Objects.equals(prev.joining, joining))
                return prev;

            return new Shard(range, nodes, fastPathElectorate, joining);
        }
    }

    static class KeyspaceShard
    {
        private final KeyspaceMetadata keyspace;
        private final Range<Token> range;
        private final List<Node.Id> nodes;
        private final Set<Node.Id> pending;

        private KeyspaceShard(KeyspaceMetadata keyspace, Range<Token> range, List<Node.Id> nodes, Set<Node.Id> pending)
        {
            this.keyspace = keyspace;
            this.range = range;
            this.nodes = nodes;
            this.pending = pending;
        }

        // return the keyspace fast path strategy if the inherit keyspace strategy is used
        private FastPathStrategy strategyFor(TableMetadata metadata)
        {
            FastPathStrategy tableStrategy = metadata.params.fastPath;
            FastPathStrategy strategy = tableStrategy.kind() != FastPathStrategy.Kind.INHERIT_KEYSPACE
                                        ? tableStrategy : keyspace.params.fastPath;
            Invariants.checkState(strategy.kind() != FastPathStrategy.Kind.INHERIT_KEYSPACE);;
            return strategy;
        }

        Shard createForTable(TableMetadata metadata, Set<Node.Id> unavailable, Map<Node.Id, String> dcMap, ShardLookup lookup)
        {
            TokenRange tokenRange = AccordTopology.range(metadata.id, range);

            Set<Node.Id> fastPath = strategyFor(metadata).calculateFastPath(nodes, unavailable, dcMap);

            return lookup.createOrReuse(tokenRange, nodes, fastPath, pending);
        }

        private static KeyspaceShard forRange(KeyspaceMetadata keyspace, Range<Token> range, Directory directory, VersionedEndpoints.ForRange reads, VersionedEndpoints.ForRange writes)
        {
            // TCM doesn't create wrap around ranges
            Invariants.checkArgument(!range.isWrapAround() || range.right.equals(range.right.minValue()),
                                     "wrap around range %s found", range);

            Set<InetAddressAndPort> readEndpoints = reads.endpoints();
            Set<InetAddressAndPort> writeEndpoints = writes.endpoints();
            Sets.SetView<InetAddressAndPort> readOnly = Sets.difference(readEndpoints, writeEndpoints);
            Invariants.checkState(readOnly.isEmpty(), "Read only replicas detected: %s", readOnly);

            List<Node.Id> nodes = writes.endpoints().stream()
                                        .map(directory::peerId)
                                        .map(AccordTopology::tcmIdToAccord)
                                        .sorted().collect(Collectors.toList());

            Set<Node.Id> pending = readEndpoints.equals(writeEndpoints) ?
                                   Collections.emptySet() :
                                   writeEndpoints.stream()
                                                 .filter(e -> !readEndpoints.contains(e))
                                                 .map(directory::peerId)
                                                 .map(AccordTopology::tcmIdToAccord)
                                                 .collect(Collectors.toSet());

            return new KeyspaceShard(keyspace, range, nodes, pending);
        }

        public static List<KeyspaceShard> forKeyspace(KeyspaceMetadata keyspace, DataPlacements placements, Directory directory, ShardLookup lookup)
        {
            ReplicationParams replication = keyspace.params.replication;
            DataPlacement placement = placements.get(replication);

            List<Range<Token>> ranges = placement.reads.ranges();
            List<KeyspaceShard> shards = new ArrayList<>(ranges.size());
            for (Range<Token> range : ranges)
            {
                VersionedEndpoints.ForRange reads = placement.reads.forRange(range);
                VersionedEndpoints.ForRange writes = placement.writes.forRange(range);
                shards.add(forRange(keyspace, range, directory, reads, writes));
            }
            return shards;
        }
    }

    static TokenRange minRange(TableId table, Token token)
    {
        return new TokenRange(SentinelKey.min(table), new TokenKey(table, token));
    }

    static TokenRange maxRange(TableId table, Token token)
    {
        return new TokenRange(new TokenKey(table, token), SentinelKey.max(table));
    }

    static TokenRange fullRange(TableId table)
    {
        return new TokenRange(SentinelKey.min(table), SentinelKey.max(table));
    }

    static TokenRange range(TableId table, Range<Token> range)
    {
        Token minToken = range.left.minValue();
        return new TokenRange(range.left.equals(minToken) ? SentinelKey.min(table) : new TokenKey(table, range.left),
                              range.right.equals(minToken) ? SentinelKey.max(table) : new TokenKey(table, range.right));
    }

    public static accord.primitives.Ranges toAccordRanges(TableId tableId, Collection<Range<Token>> ranges)
    {
        List<Range<Token>> normalizedRanges = Range.normalize(ranges);
        TokenRange[] tokenRanges = new TokenRange[normalizedRanges.size()];
        for (int i = 0; i < normalizedRanges.size(); i++)
            tokenRanges[i] = range(tableId, normalizedRanges.get(i));
        return Ranges.of(tokenRanges);
    }

    public static accord.primitives.Ranges toAccordRanges(String keyspace, Collection<Range<Token>> ranges)
    {
        Keyspace ks = Keyspace.open(keyspace);
        Ranges accordRanges = Ranges.EMPTY;
        if (ks == null)
            return accordRanges;

        for (TableMetadata tbm : ks.getMetadata().tables)
        {
            accordRanges = accordRanges.with(toAccordRanges(tbm.id, ranges));
        }

        return accordRanges;
    }

    private static Map<Node.Id, String> createDCMap(Directory directory)
    {
        ImmutableMap.Builder<Node.Id, String> builder = ImmutableMap.builder();
        directory.knownDatacenters().forEach(dc -> {
            Set<InetAddressAndPort> dcEndpoints = directory.datacenterEndpoints(dc);
            // nodes aren't added to the endpointsToDCMap until they've joined
            if (dcEndpoints == null)
                return;
            dcEndpoints.forEach(ep -> {
                NodeId tid = directory.peerId(ep);
                Node.Id aid = tcmIdToAccord(tid);
                builder.put(aid, dc);
            });
        });
        return builder.build();
    }

    public static Topology createAccordTopology(Epoch epoch, DistributedSchema schema, DataPlacements placements, Directory directory, AccordFastPath accordFastPath, ShardLookup lookup)
    {
        List<Shard> shards = new ArrayList<>();
        Set<Node.Id> unavailable = accordFastPath.unavailableIds();
        Map<Node.Id, String> dcMap = createDCMap(directory);

        for (KeyspaceMetadata keyspace : schema.getKeyspaces())
        {
            List<TableMetadata> tables = keyspace.tables.stream().filter(TableMetadata::requiresAccordSupport).collect(Collectors.toList());
            if (tables.isEmpty())
                continue;
            List<KeyspaceShard> ksShards = KeyspaceShard.forKeyspace(keyspace, placements, directory, lookup);
            tables.forEach(table -> ksShards.forEach(shard -> shards.add(shard.createForTable(table, unavailable, dcMap, lookup))));
        }

        shards.sort((a, b) -> a.range.compare(b.range));
        return new Topology(epoch.getEpoch(), shards.toArray(new Shard[0]));
    }

    public static Topology createAccordTopology(ClusterMetadata metadata, ShardLookup lookup)
    {
        return createAccordTopology(metadata.epoch, metadata.schema, metadata.placements, metadata.directory, metadata.accordFastPath, lookup);
    }

    public static Topology createAccordTopology(ClusterMetadata metadata, Topology current)
    {
        return createAccordTopology(metadata, createShardLookup(current));
    }

    public static Topology createAccordTopology(ClusterMetadata metadata)
    {
        return createAccordTopology(metadata, (Topology) null);
    }

    public static EndpointMapping directoryToMapping(EndpointMapping mapping, long epoch, Directory directory)
    {
        EndpointMapping.Builder builder = EndpointMapping.builder(epoch);
        for (NodeId id : directory.peerIds())
            builder.add(directory.endpoint(id), tcmIdToAccord(id));

        // There are cases where nodes are removed from the cluster (host replacement, decom, etc.), but inflight events may still be happening;
        // keep the ids around so pending events do not fail with a mapping error
        for (Node.Id id : mapping.differenceIds(builder))
            builder.add(mapping.mappedEndpoint(id), id);
        return builder.build();
    }

    private static ShardLookup createShardLookup(Topology topology)
    {
        ShardLookup map = new ShardLookup();

        if (topology == null)
            return map;

        topology.forEach(shard -> map.put(shard.range, shard));
        return map;
    }
    private static boolean hasAccordSchemaChange(TableMetadata before, TableMetadata after)
    {
        return after.requiresAccordSupport() && (before == null || !before.requiresAccordSupport());
    }

    private static boolean hasAccordSchemaChange(TableMetadata created)
    {
        return hasAccordSchemaChange(null, created);
    }

    private static boolean hasAccordSchemaChange(Diff.Altered<TableMetadata> diff)
    {
        return hasAccordSchemaChange(diff.before, diff.after);
    }

    private static boolean hasAccordSchemaChange(Keyspaces.KeyspacesDiff keyspacesDiff)
    {
        for (KeyspaceMetadata.KeyspaceDiff keyspaceDiff : keyspacesDiff.altered)
        {
            if (Iterables.any(keyspaceDiff.tables.created, AccordTopology::hasAccordSchemaChange))
                return true;

            if (Iterables.any(keyspaceDiff.tables.altered, AccordTopology::hasAccordSchemaChange))
                return true;
        }

        return false;
    }

    /**
     * If an accord related schema change occurs, we need to wait until accord has processed them
     * before unblocking the change
     */
    public static void awaitTopologyReadiness(Keyspaces.KeyspacesDiff keyspacesDiff, Epoch epoch)
    {
        if (!AccordService.isSetup())
            return;

        if (!hasAccordSchemaChange(keyspacesDiff))
            return;

        try
        {
            AccordService.instance().epochReady(epoch).get(DatabaseDescriptor.getTransactionTimeout(MILLISECONDS), MILLISECONDS);
        }
        catch (InterruptedException e)
        {
            throw new UncheckedInterruptedException(e);
        }
        catch (ExecutionException | TimeoutException e)
        {
            throw new RuntimeException(e);
        }
    }

}
