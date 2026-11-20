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

package org.apache.cassandra.db.compression;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.db.compression.CompressionDictionaryScope.DatacenterPlace;
import org.apache.cassandra.db.compression.CompressionDictionaryScope.Kind;
import org.apache.cassandra.db.compression.CompressionDictionaryScope.NodePlace;
import org.apache.cassandra.db.compression.CompressionDictionaryScope.Place;
import org.apache.cassandra.db.compression.CompressionDictionaryScope.RackPlace;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.tcm.membership.Directory;
import org.apache.cassandra.tcm.membership.Location;
import org.apache.cassandra.tcm.membership.NodeAddresses;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeState;

import static org.apache.cassandra.db.compression.CompressionDictionaryScope.CLUSTER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class CompressionDictionaryScopeTest
{
    // dc1/r1: nodes 1 and 2, dc1/r2: node 3, dc2/r1: node 4, all joined
    // dc2/r2: node 5, registered only; dc3/r1: node 6, left
    private static Directory directory;
    private static final List<NodeId> NODES = new ArrayList<>();
    private static final NodeId UNKNOWN_NODE = new NodeId(99);

    @BeforeClass
    public static void setup() throws UnknownHostException
    {
        directory = Directory.EMPTY;
        add("dc1", "r1", NodeState.JOINED);
        add("dc1", "r1", NodeState.JOINED);
        add("dc1", "r2", NodeState.JOINED);
        add("dc2", "r1", NodeState.JOINED);
        add("dc2", "r2", NodeState.REGISTERED);
        add("dc3", "r1", NodeState.LEFT);
    }

    private static void add(String datacenter, String rack, NodeState state) throws UnknownHostException
    {
        NodeId id = new NodeId(directory.nextId);
        InetAddressAndPort address = InetAddressAndPort.getByNameOverrideDefaults("127.0.0." + (NODES.size() + 1), 7000);
        directory = directory.with(new NodeAddresses(address), new Location(datacenter, rack)).withNodeState(id, state);
        if (state == NodeState.JOINED)
            directory = directory.withRackAndDC(id);
        NODES.add(id);
    }

    @Test
    public void testCluster()
    {
        assertThat(CLUSTER.isCluster()).isTrue();
        assertThat(CLUSTER.toValue()).isNull();
        assertThat(CLUSTER.toFlattened()).isNull();
        assertThat(CLUSTER.places()).isEmpty();
        assertThat(CLUSTER.contains(directory, node(1))).isTrue();
        assertThat(CLUSTER.contains(null, null)).isTrue();
        assertThat(CLUSTER).hasToString("cluster");
        assertThat(CompressionDictionaryScope.fromValue(null)).isEqualTo(CLUSTER);
        assertThat(CompressionDictionaryScope.fromFlattened(null)).isEqualTo(CLUSTER);
    }

    @Test
    public void testNode()
    {
        CompressionDictionaryScope scope = CompressionDictionaryScope.node(node(1));
        assertThat(scope.isCluster()).isFalse();
        assertThat(scope.toValue()).containsExactly(Map.of("node_id", "1"));
        assertThat(scope.toFlattened()).isDeepEqualTo(new String[][]{ { "node_id", "1" } });
        assertThat(scope.places()).containsExactly(new NodePlace(node(1)));
        assertThat(scope.contains(directory, node(1))).isTrue();
        assertThat(scope.contains(null, node(1))).as("needs no location").isTrue();
        assertThat(scope.contains(directory, node(2))).isFalse();
        assertThat(scope.contains(directory, null)).isFalse();
        assertThat(scope).hasToString("node 1");
        assertThat(CompressionDictionaryScope.fromValue(Set.of(Map.of("node_id", "1")))).isEqualTo(scope);
        assertThat(CompressionDictionaryScope.fromFlattened(new String[][]{ { "node_id", "1" } })).isEqualTo(scope);
    }

    @Test
    public void testRackAndDatacenter()
    {
        CompressionDictionaryScope rack = scope(rack("dc1", "r1"));
        assertThat(rack.toValue()).containsExactly(Map.of("dc", "dc1", "rack", "r1"));
        assertThat(rack.toFlattened()).isDeepEqualTo(new String[][]{ { "dc", "dc1", "rack", "r1" } });
        assertThat(rack).hasToString("rack dc1/r1");
        assertThat(CompressionDictionaryScope.fromValue(rack.toValue())).isEqualTo(rack);
        assertThat(CompressionDictionaryScope.fromFlattened(rack.toFlattened())).isEqualTo(rack);
        assertThat(rack.contains(directory, node(1))).isTrue();
        assertThat(rack.contains(directory, node(2))).isTrue();
        assertThat(rack.contains(directory, node(3))).as("other rack").isFalse();
        assertThat(rack.contains(directory, node(4))).as("rack of the same name in another datacenter").isFalse();
        assertThat(rack.contains(directory, UNKNOWN_NODE)).isFalse();
        assertThat(rack.contains(null, node(1))).as("location unknown without cluster metadata").isFalse();

        CompressionDictionaryScope datacenter = scope(datacenter("dc1"));
        assertThat(datacenter.toValue()).containsExactly(Map.of("dc", "dc1"));
        assertThat(datacenter).hasToString("dc dc1");
        assertThat(CompressionDictionaryScope.fromValue(datacenter.toValue())).isEqualTo(datacenter);
        assertThat(datacenter.contains(directory, node(1))).isTrue();
        assertThat(datacenter.contains(directory, node(3))).isTrue();
        assertThat(datacenter.contains(directory, node(4))).isFalse();
        assertThat(datacenter.contains(null, node(1))).isFalse();

        // nodes first by id, then racks, then datacenters
        CompressionDictionaryScope mixed = scope(datacenter("dc2"), rack("dc1", "r2"), node(4), rack("dc1", "r1"));
        assertThat(mixed).hasToString("node 4, rack dc1/r1, rack dc1/r2, dc dc2");
        assertThat(CompressionDictionaryScope.fromValue(mixed.toValue())).isEqualTo(mixed);
        assertThat(CompressionDictionaryScope.fromFlattened(mixed.toFlattened())).isEqualTo(mixed);
    }

    @Test
    public void testNotAScope()
    {
        // no version writes any of these, reading one fails
        for (Map<String, String> map : List.<Map<String, String>>of(Map.of("region", "eu"),
                                                                    Map.of("rack", "r1"),
                                                                    Map.of("datacenter", "dc1"),
                                                                    Map.of("datacenter", "dc1", "rack", "r1"),
                                                                    Map.of("dc", "dc1", "rack", "r1", "region", "eu"),
                                                                    Map.of("node_id", "1", "rack", "r1"),
                                                                    Map.of("node_id", "not a number"),
                                                                    // only the canonical form of a positive node id is a node
                                                                    Map.of("node_id", "01"),
                                                                    Map.of("node_id", "+1"),
                                                                    Map.of("node_id", "0"),
                                                                    Map.of("node_id", "-1"),
                                                                    Map.of()))
        {
            assertThatThrownBy(() -> CompressionDictionaryScope.fromValue(Set.of(map)))
            .as(map.toString())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining(map + " is not a place of a scope, a place is {node_id=<id>}, {dc=<dc>, rack=<rack>} or {dc=<dc>}");

            // with a place which is one, too
            assertThatThrownBy(() -> CompressionDictionaryScope.fromValue(Set.of(map, Map.of("node_id", "1"))))
            .as(map.toString())
            .isInstanceOf(IllegalArgumentException.class);
        }

        for (Collection<Map<String, String>> empty : List.<Collection<Map<String, String>>>of(Set.of(), List.of()))
        {
            assertThatThrownBy(() -> CompressionDictionaryScope.fromValue(empty))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("A scope which is not the whole cluster has at least one place");
        }
        assertThatThrownBy(() -> CompressionDictionaryScope.of(List.of()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("A scope which is not the whole cluster has at least one place");
    }

    @Test
    public void testCovers()
    {
        assertCovers(CLUSTER, CLUSTER, scope(node(1)), scope(rack("dc1", "r1")), scope(datacenter("dc2")));

        // a datacenter covers its racks regardless of the topology, and nodes located in it
        assertCovers(scope(datacenter("dc1")), scope(datacenter("dc1")), scope(rack("dc1", "r1")), scope(rack("dc1", "r2")),
                     scope(rack("dc1", "no node here")), scope(node(1), node(3)), scope(rack("dc1", "r1"), node(3)));
        assertNotCovers(scope(datacenter("dc1")), CLUSTER, scope(datacenter("dc2")), scope(rack("dc2", "r1")), scope(node(4)),
                        scope(node(1), node(4)), scope(UNKNOWN_NODE));

        // a rack covers nodes located in it
        assertCovers(scope(rack("dc1", "r1")), scope(rack("dc1", "r1")), scope(node(1)), scope(node(1), node(2)));
        assertNotCovers(scope(rack("dc1", "r1")), CLUSTER, scope(datacenter("dc1")), scope(rack("dc1", "r2")), scope(rack("dc2", "r1")),
                        scope(node(3)), scope(node(4)), scope(UNKNOWN_NODE));

        // a node covers only itself
        assertCovers(scope(node(1)), scope(node(1)));
        assertNotCovers(scope(node(1)), CLUSTER, scope(node(2)), scope(node(1), node(2)), scope(rack("dc1", "r1")), scope(datacenter("dc1")));

        // a union covers what any of its places covers
        assertCovers(scope(rack("dc1", "r1"), datacenter("dc2")), scope(node(1), node(4)), scope(rack("dc2", "r2"), node(2)));
        assertNotCovers(scope(rack("dc1", "r1"), datacenter("dc2")), scope(node(3)), scope(node(1), node(3)));
    }

    @Test
    public void testPlus()
    {
        assertThat(CLUSTER.plus(List.of(rack("dc1", "r1")))).isEqualTo(CLUSTER);

        // a node stays even when it is located in an added rack or datacenter, it can be moved elsewhere later
        assertThat(scope(node(1)).plus(List.of(rack("dc1", "r1")))).isEqualTo(scope(node(1), rack("dc1", "r1")));
        assertThat(scope(node(1), node(3)).plus(List.of(rack("dc1", "r1")))).isEqualTo(scope(node(1), node(3), rack("dc1", "r1")));
        assertThat(scope(node(1), node(4)).plus(List.of(datacenter("dc1")))).isEqualTo(scope(node(1), node(4), datacenter("dc1")));

        // racks are added one by one, a datacenter absorbs its racks
        CompressionDictionaryScope r1 = scope(rack("dc1", "r1"));
        CompressionDictionaryScope r1r2 = r1.plus(List.of(rack("dc1", "r2")));
        assertThat(r1r2).isEqualTo(scope(rack("dc1", "r1"), rack("dc1", "r2")));
        assertThat(r1r2.plus(List.of(rack("dc2", "r1")))).isEqualTo(scope(rack("dc1", "r1"), rack("dc1", "r2"), rack("dc2", "r1")));
        assertThat(r1r2.plus(List.of(datacenter("dc1")))).isEqualTo(scope(datacenter("dc1")));
        assertThat(r1r2.plus(List.of(datacenter("dc2")))).isEqualTo(scope(rack("dc1", "r1"), rack("dc1", "r2"), datacenter("dc2")));

        // nothing new, the same scope
        assertThat(r1r2.plus(List.of(rack("dc1", "r1")))).isEqualTo(r1r2);
        assertThat(scope(datacenter("dc1")).plus(List.of(rack("dc1", "r2")))).isEqualTo(scope(datacenter("dc1")));
        assertThat(scope(node(2), rack("dc1", "r1")).plus(List.of(new NodePlace(node(2))))).isEqualTo(scope(node(2), rack("dc1", "r1")));
    }

    @Test
    public void testNodeKeepsScopeWhenMovedToAnotherRack() throws UnknownHostException
    {
        // node 1 in dc1/r1, promoted to its rack, is then moved to dc1/r2
        CompressionDictionaryScope widened = scope(node(1)).plus(List.of(rack("dc1", "r1")));
        assertThat(widened.contains(directory, node(1))).isTrue();
        Directory moved = directory.withUpdatedRackAndDc(node(1), new Location("dc1", "r2"));
        assertThat(moved.location(node(1))).isEqualTo(new Location("dc1", "r2"));
        assertThat(widened.contains(moved, node(1))).as("still in the scope by its own place").isTrue();
        assertThat(scope(rack("dc1", "r1")).contains(moved, node(1))).as("a scope of its old rack only").isFalse();
    }

    @Test
    public void testLegalWidening()
    {
        // step by step along the ladder
        assertWidens(scope(node(1)), scope(rack("dc1", "r1")));
        assertWidens(scope(rack("dc1", "r1")), scope(datacenter("dc1")));
        assertWidens(scope(datacenter("dc1")), CLUSTER);
        // skipping steps
        assertWidens(scope(node(1)), scope(datacenter("dc1")));
        assertWidens(scope(node(1)), CLUSTER);
        assertWidens(scope(rack("dc1", "r1")), CLUSTER);
        // adding places
        assertWidens(scope(node(1)), scope(node(1), node(4)));
        assertWidens(scope(node(1)), scope(node(1), rack("dc2", "r1")));
        assertWidens(scope(rack("dc1", "r1")), scope(rack("dc1", "r1"), rack("dc1", "r2")));
        assertWidens(scope(rack("dc1", "r1"), rack("dc1", "r2")), scope(rack("dc1", "r1"), rack("dc1", "r2"), rack("dc2", "r1")));
        assertWidens(scope(rack("dc1", "r1"), rack("dc1", "r2")), scope(datacenter("dc1")));
        assertWidens(scope(datacenter("dc1")), scope(datacenter("dc1"), datacenter("dc2")));
        assertWidens(scope(rack("dc1", "r1"), node(4)), scope(datacenter("dc1"), datacenter("dc2")));
    }

    @Test
    public void testIllegalWidening()
    {
        // the same scope
        assertNotWidening(scope(node(1)), scope(node(1)), "the scope would stay node 1, nothing to promote");
        assertNotWidening(scope(rack("dc1", "r1"), rack("dc1", "r2")), scope(rack("dc1", "r2"), rack("dc1", "r1")), "the scope would stay rack dc1/r1, rack dc1/r2, nothing to promote");
        assertNotWidening(scope(datacenter("dc1")), scope(datacenter("dc1")), "the scope would stay dc dc1, nothing to promote");
        assertNotWidening(CLUSTER, CLUSTER, "the scope would stay cluster, nothing to promote");

        // shrinking, the message names what would be lost
        assertNotWidening(CLUSTER, scope(datacenter("dc1")), "scope dc dc1 would no longer contain the whole cluster, a scope can only be widened");
        assertNotWidening(scope(datacenter("dc1")), scope(rack("dc1", "r1")), "scope rack dc1/r1 would no longer contain dc dc1, a scope can only be widened");
        assertNotWidening(scope(datacenter("dc1")), scope(rack("dc1", "r1"), rack("dc1", "r2")), "scope rack dc1/r1, rack dc1/r2 would no longer contain dc dc1");
        assertNotWidening(scope(rack("dc1", "r1")), scope(node(1), node(2)), "scope node 1, node 2 would no longer contain rack dc1/r1");
        assertNotWidening(scope(rack("dc1", "r1"), rack("dc1", "r2")), scope(rack("dc1", "r1")), "scope rack dc1/r1 would no longer contain rack dc1/r2");
        assertNotWidening(scope(node(1), node(2)), scope(node(1)), "scope node 1 would no longer contain node 2");

        // sideways, disjoint
        assertNotWidening(scope(node(1)), scope(node(2)), "scope node 2 would no longer contain node 1");
        assertNotWidening(scope(node(1)), scope(rack("dc1", "r2")), "scope rack dc1/r2 would no longer contain node 1");
        assertNotWidening(scope(node(1)), scope(datacenter("dc2")), "scope dc dc2 would no longer contain node 1");
        assertNotWidening(scope(rack("dc1", "r1")), scope(rack("dc1", "r2")), "scope rack dc1/r2 would no longer contain rack dc1/r1");
        assertNotWidening(scope(rack("dc1", "r1")), scope(rack("dc2", "r1")), "scope rack dc2/r1 would no longer contain rack dc1/r1");
        assertNotWidening(scope(rack("dc1", "r1")), scope(datacenter("dc2")), "scope dc dc2 would no longer contain rack dc1/r1");

        // partially overlapping, places are dropped while others are added, only the dropped ones are named
        assertNotWidening(scope(rack("dc1", "r1"), rack("dc1", "r2")), scope(rack("dc1", "r2"), datacenter("dc2")), "scope rack dc1/r2, dc dc2 would no longer contain rack dc1/r1, a scope");
        assertNotWidening(scope(node(1), node(4)), scope(rack("dc1", "r1"), rack("dc1", "r2")), "scope rack dc1/r1, rack dc1/r2 would no longer contain node 4, a scope");

        // a node whose location is not known is not covered by any rack or datacenter
        assertNotWidening(scope(UNKNOWN_NODE), scope(rack("dc1", "r1")), "scope rack dc1/r1 would no longer contain node 99");
        assertNotWidening(scope(UNKNOWN_NODE), scope(datacenter("dc1"), datacenter("dc2")), "scope dc dc1, dc dc2 would no longer contain node 99");
    }

    @Test
    public void testCoveredOf()
    {
        CompressionDictionaryScope dc1 = scope(datacenter("dc1"));
        assertThat(dc1.coveredOf(directory, List.of(rack("dc1", "r1")))).containsExactly(rack("dc1", "r1"));
        assertThat(dc1.coveredOf(directory, List.of(rack("dc2", "r1"), new NodePlace(node(3)), datacenter("dc2")))).containsExactly(new NodePlace(node(3)));
        assertThat(scope(rack("dc1", "r1")).coveredOf(directory, List.of(datacenter("dc1")))).as("a rack does not cover its datacenter").isEmpty();
        assertThat(CLUSTER.coveredOf(directory, List.of(datacenter("dc9"), rack("dc1", "r1")))).hasSize(2);
        assertThat(CompressionDictionaryScope.describe(List.of(datacenter("dc2"), rack("dc1", "r2"), new NodePlace(node(4)), rack("dc1", "r1"))))
        .isEqualTo("node 4, rack dc1/r1, rack dc1/r2, dc dc2");
    }

    @Test
    public void testMembers()
    {
        assertThat(scope(rack("dc1", "r1")).memberEndpoints(directory)).containsExactlyInAnyOrder(endpoint(1), endpoint(2));
        assertThat(scope(datacenter("dc1")).memberEndpoints(directory)).containsExactlyInAnyOrder(endpoint(1), endpoint(2), endpoint(3));
        assertThat(scope(rack("dc1", "r1"), node(4)).memberEndpoints(directory)).containsExactlyInAnyOrder(endpoint(1), endpoint(2), endpoint(4));
        assertThat(scope(rack("dc1", "r1"), node(1)).memberEndpoints(directory)).as("once each").containsExactlyInAnyOrder(endpoint(1), endpoint(2));
        // joined nodes only, for every kind of place
        assertThat(scope(node(1)).memberEndpoints(directory)).containsExactly(endpoint(1));
        assertThat(scope(node(5)).memberEndpoints(directory)).as("registered").isEmpty();
        assertThat(scope(node(6)).memberEndpoints(directory)).as("left").isEmpty();
        assertThat(scope(rack("dc3", "r1")).memberEndpoints(directory)).as("left").isEmpty();
        assertThat(scope(rack("dc9", "r1")).memberEndpoints(directory)).as("no such datacenter").isEmpty();
        assertThat(CLUSTER.memberEndpoints(directory)).containsExactlyInAnyOrder(endpoint(1), endpoint(2), endpoint(3), endpoint(4));

        // a scope with no node in the cluster is orphaned
        assertThat(scope(rack("dc1", "r1")).hasMembers(directory)).isTrue();
        assertThat(scope(rack("dc2", "r2")).hasMembers(directory)).as("registered only").isFalse();
        assertThat(scope(node(5)).hasMembers(directory)).as("registered only").isFalse();
        assertThat(scope(datacenter("dc2")).hasMembers(directory)).isTrue();
        assertThat(scope(rack("dc3", "r1")).hasMembers(directory)).as("left").isFalse();
        assertThat(scope(datacenter("dc3")).hasMembers(directory)).as("left").isFalse();
        assertThat(scope(rack("dc1", "no node here")).hasMembers(directory)).isFalse();
        assertThat(scope(node(6)).hasMembers(directory)).as("left").isFalse();
        assertThat(scope(node(6), rack("dc1", "r1")).hasMembers(directory)).isTrue();
        assertThat(CLUSTER.hasMembers(directory)).isTrue();
    }

    @Test
    public void testKind()
    {
        assertThat(Kind.parse("node")).isEqualTo(Kind.NODE);
        assertThat(Kind.parse(" RACK ")).isEqualTo(Kind.RACK);
        assertThat(Kind.parse("DC")).isEqualTo(Kind.DC);
        assertThat(Kind.parse("cluster")).isEqualTo(Kind.CLUSTER);

        assertThat(Kind.NODE.of(directory, node(3))).isEqualTo(scope(node(3)));
        assertThat(Kind.RACK.of(directory, node(3))).isEqualTo(scope(rack("dc1", "r2")));
        assertThat(Kind.DC.of(directory, node(3))).isEqualTo(scope(datacenter("dc1")));
        assertThat(Kind.CLUSTER.of(directory, node(3))).isEqualTo(CLUSTER);
        assertThat(Kind.CLUSTER.of(null, null)).isEqualTo(CLUSTER);
        assertThat(Kind.NODE.of(null, node(3))).as("needs no location").isEqualTo(scope(node(3)));

        for (Kind kind : List.of(Kind.NODE, Kind.RACK, Kind.DC))
            assertThatThrownBy(() -> kind.of(directory, null)).isInstanceOf(IllegalStateException.class).hasMessageContaining("The id of this node is not known");
        for (Kind kind : List.of(Kind.RACK, Kind.DC))
        {
            assertThatThrownBy(() -> kind.of(directory, UNKNOWN_NODE)).isInstanceOf(IllegalStateException.class).hasMessageContaining("The location of this node is not known");
            assertThatThrownBy(() -> kind.of(null, node(1))).isInstanceOf(IllegalStateException.class).hasMessageContaining("The location of this node is not known");
        }

        for (String invalid : new String[]{ "region", "", null })
        {
            assertThatThrownBy(() -> Kind.parse(invalid))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("valid scopes are [node, rack, dc, cluster]");
        }
    }

    @Test
    public void testFlattenedWithoutValue()
    {
        assertThatThrownBy(() -> CompressionDictionaryScope.fromFlattened(new String[][]{ { "node_id" } }))
        .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> CompressionDictionaryScope.fromFlattened(new String[][]{ {} }))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("{} is not a place of a scope");
        assertThatThrownBy(() -> CompressionDictionaryScope.fromFlattened(new String[0][]))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("has at least one place");
        assertThatThrownBy(() -> CompressionDictionaryScope.fromFlattened(new String[][]{ { "dc", "dc1", "dc", "dc2" } }))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("duplicate key dc");
    }

    private static void assertCovers(CompressionDictionaryScope scope, CompressionDictionaryScope... others)
    {
        for (CompressionDictionaryScope other : others)
            assertThat(scope.covers(directory, other)).as("%s covers %s", scope, other).isTrue();
    }

    private static void assertNotCovers(CompressionDictionaryScope scope, CompressionDictionaryScope... others)
    {
        for (CompressionDictionaryScope other : others)
            assertThat(scope.covers(directory, other)).as("%s does not cover %s", scope, other).isFalse();
    }

    private static void assertWidens(CompressionDictionaryScope from, CompressionDictionaryScope to)
    {
        assertThatCode(() -> from.checkCanBeWidenedTo(directory, to)).as("%s to %s", from, to).doesNotThrowAnyException();
        assertThat(to.covers(directory, from)).as("%s covers %s", to, from).isTrue();
        assertThat(from.covers(directory, to)).as("%s does not cover %s", from, to).isFalse();
    }

    private static void assertNotWidening(CompressionDictionaryScope from, CompressionDictionaryScope to, String message)
    {
        assertThatThrownBy(() -> from.checkCanBeWidenedTo(directory, to))
        .as("%s to %s", from, to)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(message);
    }

    private static NodeId node(int index)
    {
        return NODES.get(index - 1);
    }

    private static InetAddressAndPort endpoint(int index)
    {
        return directory.endpoint(node(index));
    }

    private static CompressionDictionaryScope scope(Object... places)
    {
        List<Place> list = new ArrayList<>();
        for (Object place : places)
            list.add(place instanceof NodeId ? new NodePlace((NodeId) place) : (Place) place);
        return CompressionDictionaryScope.of(list);
    }

    private static RackPlace rack(String datacenter, String rack)
    {
        return new RackPlace(datacenter, rack);
    }

    private static DatacenterPlace datacenter(String datacenter)
    {
        return new DatacenterPlace(datacenter);
    }
}
