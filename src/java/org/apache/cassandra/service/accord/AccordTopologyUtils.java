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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;

import accord.local.Node;
import accord.topology.Shard;
import accord.topology.Topology;
import accord.utils.Invariants;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.EndpointsForRange;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.DistributedSchema;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.service.accord.api.AccordRoutingKey.SentinelKey;
import org.apache.cassandra.service.accord.api.AccordRoutingKey.TokenKey;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.membership.Directory;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.ownership.DataPlacement;
import org.apache.cassandra.tcm.ownership.DataPlacements;

public class AccordTopologyUtils
{
    static Node.Id tcmIdToAccord(NodeId nodeId)
    {
        return new Node.Id(nodeId.id());
    }

    private static Shard createShard(TokenRange range, Directory directory, EndpointsForRange reads, EndpointsForRange writes)
    {
        Function<InetAddressAndPort, Node.Id> endpointMapper = e -> {
            NodeId tcmId = directory.peerId(e);
            return tcmIdToAccord(tcmId);
        };
        Set<InetAddressAndPort> endpoints = reads.endpoints();
        Set<InetAddressAndPort> writeEndpoints = writes.endpoints();
        List<Node.Id> nodes = endpoints.stream().map(endpointMapper).sorted().collect(Collectors.toList());
        Set<Node.Id> fastPath = new HashSet<>(nodes);  // TODO: support fast path updates
        Set<Node.Id> pending = endpoints.equals(writeEndpoints) ?
                               Collections.emptySet() :
                               writeEndpoints.stream().filter(e -> !endpoints.contains(e)).map(endpointMapper).collect(Collectors.toSet());

        Sets.SetView<InetAddressAndPort> readOnly = Sets.difference(endpoints, writeEndpoints);
        Invariants.checkState(readOnly.isEmpty(), "Read only replicas detected: %s", readOnly);
        return new Shard(range, nodes, fastPath, pending);
    }

    static TokenRange minRange(String keyspace, Token token)
    {
        return new TokenRange(SentinelKey.min(keyspace), new TokenKey(keyspace, token));
    }

    static TokenRange maxRange(String keyspace, Token token)
    {
        return new TokenRange(new TokenKey(keyspace, token), SentinelKey.max(keyspace));
    }

    static TokenRange fullRange(String keyspace)
    {
        return new TokenRange(SentinelKey.min(keyspace), SentinelKey.max(keyspace));
    }

    static TokenRange range(String keyspace, Range<Token> range)
    {
        Token minToken = range.left.minValue();
        return new TokenRange(range.left.equals(minToken) ? SentinelKey.min(keyspace) : new TokenKey(keyspace, range.left),
                              range.right.equals(minToken) ? SentinelKey.max(keyspace) : new TokenKey(keyspace, range.right));
    }

    public static List<Shard> createShards(KeyspaceMetadata keyspace, DataPlacements placements, Directory directory)
    {
        ReplicationParams replication = keyspace.params.replication;
        DataPlacement placement = placements.get(replication);

        List<Range<Token>> ranges = placement.reads.ranges();
        List<Shard> shards = new ArrayList<>(ranges.size());
        for (Range<Token> range : ranges)
        {
            EndpointsForRange reads = placement.reads.forRange(range);
            EndpointsForRange writes = placement.reads.forRange(range);

            // TCM doesn't create wrap around ranges
            Invariants.checkArgument(!range.isWrapAround() || range.right.equals(range.right.minValue()),
                                     "wrap around range %s found", range);
            shards.add(createShard(range(keyspace.name, range), directory, reads, writes));
        }

        return shards;
    }

    public static Topology createAccordTopology(Epoch epoch, DistributedSchema schema, DataPlacements placements, Directory directory, Predicate<String> keyspacePredicate)
    {
        List<Shard> shards = new ArrayList<>();
        for (KeyspaceMetadata keyspace : schema.getKeyspaces())
        {
            if (!keyspacePredicate.test(keyspace.name))
                continue;
            shards.addAll(createShards(keyspace, placements, directory));
        }
        shards.sort((a, b) -> a.range.compare(b.range));
        return new Topology(epoch.getEpoch(), shards.toArray(new Shard[0]));
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

    public static Topology createAccordTopology(ClusterMetadata metadata, Predicate<String> keyspacePredicate)
    {
        return createAccordTopology(metadata.epoch, metadata.schema, metadata.placements, metadata.directory, keyspacePredicate);
    }
}
