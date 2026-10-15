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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Test;

import org.apache.cassandra.db.compression.CompressionDictionaryScope.Kind;
import org.apache.cassandra.db.compression.CompressionDictionaryScope.NodePlace;
import org.apache.cassandra.tcm.membership.NodeId;

import static org.apache.cassandra.db.compression.CompressionDictionaryScope.CLUSTER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class CompressionDictionaryScopeTest
{
    private static final NodeId NODE_1 = new NodeId(1);
    private static final NodeId NODE_2 = new NodeId(2);
    private static final NodeId NODE_3 = new NodeId(3);

    // a kind of place a later version might write
    private static final Map<String, String> RACK = Map.of("datacenter", "dc1", "rack", "r1");

    @Test
    public void testCluster()
    {
        assertThat(CLUSTER.isCluster()).isTrue();
        assertThat(CLUSTER.isKnown()).isTrue();
        assertThat(CLUSTER.toValue()).isNull();
        assertThat(CLUSTER.toFlattened()).isNull();
        assertThat(CLUSTER.places()).isEmpty();
        assertThat(CLUSTER.contains(NODE_1)).isTrue();
        assertThat(CLUSTER.contains(null)).isTrue();
        assertThat(CLUSTER).hasToString("cluster");
        assertThat(CompressionDictionaryScope.fromValue(null)).isEqualTo(CLUSTER);
        assertThat(CompressionDictionaryScope.fromFlattened(null)).isEqualTo(CLUSTER);
    }

    @Test
    public void testNode()
    {
        CompressionDictionaryScope scope = CompressionDictionaryScope.node(NODE_1);
        assertThat(scope.isCluster()).isFalse();
        assertThat(scope.isKnown()).isTrue();
        assertThat(scope.toValue()).containsExactly(Map.of("node_id", "1"));
        assertThat(scope.toFlattened()).isDeepEqualTo(new String[][]{ { "node_id", "1" } });
        assertThat(scope.places()).containsExactly(new NodePlace(NODE_1));
        assertThat(scope.contains(NODE_1)).isTrue();
        assertThat(scope.contains(NODE_2)).isFalse();
        assertThat(scope.contains(null)).isFalse();
        assertThat(scope).hasToString("node 1");
        assertThat(CompressionDictionaryScope.fromValue(Set.of(Map.of("node_id", "1")))).isEqualTo(scope);
        assertThat(CompressionDictionaryScope.fromFlattened(new String[][]{ { "node_id", "1" } })).isEqualTo(scope);
    }

    @Test
    public void testUnionOfPlaces()
    {
        CompressionDictionaryScope scope = nodes(10, 2, 1);
        assertThat(scope.contains(NODE_1)).isTrue();
        assertThat(scope.contains(NODE_2)).isTrue();
        assertThat(scope.contains(NODE_3)).isFalse();
        assertThat(scope).as("ordered by node id").hasToString("node 1, node 2, node 10");
        assertThat(scope.toFlattened()).isDeepEqualTo(new String[][]{ { "node_id", "1" }, { "node_id", "2" }, { "node_id", "10" } });
        assertThat(CompressionDictionaryScope.fromValue(scope.toValue())).isEqualTo(scope);
        assertThat(CompressionDictionaryScope.fromFlattened(scope.toFlattened())).isEqualTo(scope);
        assertThat(nodes(1, 2, 2)).as("duplicates").isEqualTo(nodes(2, 1));
    }

    @Test
    public void testUnknownPlacesContainNoNode()
    {
        // e.g. kinds of places a later version writes
        for (Map<String, String> map : List.<Map<String, String>>of(RACK,
                                               Map.of("datacenter", "dc1"),
                                               Map.of("node_id", "1", "rack", "r1"),
                                               Map.of("node_id", "not a number"),
                                               Map.of()))
        {
            CompressionDictionaryScope scope = CompressionDictionaryScope.fromValue(Set.of(map));
            assertThat(scope.isCluster()).as(map.toString()).isFalse();
            assertThat(scope.isKnown()).as(map.toString()).isFalse();
            assertThat(scope.contains(NODE_1)).as(map.toString()).isFalse();
            assertThat(scope.toValue()).as(map.toString()).containsExactly(map);
            assertThat(CompressionDictionaryScope.fromFlattened(scope.toFlattened())).as(map.toString()).isEqualTo(scope);
        }

        // the known places of a scope still apply
        CompressionDictionaryScope mixed = CompressionDictionaryScope.fromValue(Set.of(RACK, Map.of("node_id", "1")));
        assertThat(mixed.contains(NODE_1)).isTrue();
        assertThat(mixed.contains(NODE_2)).isFalse();
        assertThat(mixed.isKnown()).isFalse();
        assertThat(mixed).as("known places first").hasToString("node 1, {datacenter=dc1, rack=r1}");

        // an empty set is never written
        CompressionDictionaryScope empty = CompressionDictionaryScope.fromValue(Set.of());
        assertThat(empty.isCluster()).isFalse();
        assertThat(empty.isKnown()).isFalse();
        assertThat(empty.contains(NODE_1)).isFalse();
    }

    @Test
    public void testCovers()
    {
        assertThat(CLUSTER.covers(CLUSTER)).isTrue();
        assertThat(CLUSTER.covers(nodes(1))).isTrue();
        assertThat(CLUSTER.covers(unknown())).isTrue();

        assertThat(nodes(1).covers(CLUSTER)).isFalse();
        assertThat(nodes(1).covers(nodes(1))).isTrue();
        assertThat(nodes(1, 2).covers(nodes(1))).isTrue();
        assertThat(nodes(1).covers(nodes(1, 2))).isFalse();
        assertThat(nodes(1).covers(nodes(2))).isFalse();
        assertThat(nodes(1, 2).covers(nodes(2, 3))).isFalse();

        // a place this version does not know is covered only by the cluster or by itself
        assertThat(nodes(1).covers(unknown())).isFalse();
        assertThat(unknown().covers(nodes(1))).isFalse();
        assertThat(unknown().covers(unknown())).isTrue();
    }

    @Test
    public void testLegalWidening()
    {
        assertWidens(nodes(1), CLUSTER);
        assertWidens(nodes(1), nodes(1, 2));
        assertWidens(nodes(1), nodes(1, 2, 3));
        assertWidens(nodes(1, 2), nodes(1, 2, 3));
        assertWidens(nodes(1, 2), CLUSTER);
        // a scope written by a later version can still be promoted to the whole cluster
        assertWidens(unknown(), CLUSTER);
        assertWidens(CompressionDictionaryScope.fromValue(Set.of(RACK, Map.of("node_id", "1"))), CLUSTER);
    }

    @Test
    public void testIllegalWidening()
    {
        // the same scope
        assertNotWidening(nodes(1), nodes(1), "is already scoped to node 1");
        assertNotWidening(nodes(1, 2), nodes(2, 1), "is already scoped to node 1, node 2");
        assertNotWidening(CLUSTER, CLUSTER, "is already scoped to cluster");

        // shrinking
        assertNotWidening(CLUSTER, nodes(1), "does not contain all of scope cluster");
        assertNotWidening(nodes(1, 2), nodes(1), "Scope node 1 does not contain all of scope node 1, node 2");
        assertNotWidening(nodes(1, 2, 3), nodes(2, 3), "does not contain all of scope");

        // disjoint or partially overlapping, neither contains the other
        assertNotWidening(nodes(1), nodes(2), "Scope node 2 does not contain all of scope node 1");
        assertNotWidening(nodes(1, 2), nodes(2, 3), "Scope node 2, node 3 does not contain all of scope node 1, node 2");

        // to places this version does not know, they contain no node from its point of view
        assertNotWidening(nodes(1), unknown(), "contains places this version does not know");
        assertNotWidening(nodes(1), CompressionDictionaryScope.fromValue(Set.of(RACK, Map.of("node_id", "1"))), "contains places this version does not know");
        assertNotWidening(nodes(1), CompressionDictionaryScope.fromValue(Set.of()), "contains places this version does not know");

        // from places this version does not know to anything but the cluster
        assertNotWidening(unknown(), nodes(1), "does not contain all of scope");
    }

    @Test
    public void testKind()
    {
        assertThat(Kind.parse("node")).isEqualTo(Kind.NODE);
        assertThat(Kind.parse(" NODE ")).isEqualTo(Kind.NODE);
        assertThat(Kind.parse("Cluster")).isEqualTo(Kind.CLUSTER);
        assertThat(Kind.NODE.of(NODE_1)).isEqualTo(CompressionDictionaryScope.node(NODE_1));
        assertThat(Kind.CLUSTER.of(NODE_1)).isEqualTo(CLUSTER);
        assertThat(Kind.CLUSTER.of(null)).isEqualTo(CLUSTER);

        assertThatThrownBy(() -> Kind.NODE.of(null)).isInstanceOf(IllegalStateException.class);
        for (String invalid : new String[]{ "rack", "datacenter", "", null })
        {
            assertThatThrownBy(() -> Kind.parse(invalid))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("valid scopes are [node, cluster]");
        }
    }

    @Test
    public void testFlattenedWithoutValue()
    {
        assertThatThrownBy(() -> CompressionDictionaryScope.fromFlattened(new String[][]{ { "node_id" } }))
        .isInstanceOf(IllegalArgumentException.class);
    }

    private static void assertWidens(CompressionDictionaryScope from, CompressionDictionaryScope to)
    {
        assertThatCode(() -> from.checkCanBeWidenedTo(to)).as("%s to %s", from, to).doesNotThrowAnyException();
        assertThat(to.covers(from)).as("%s covers %s", to, from).isTrue();
        assertThat(from.covers(to)).as("%s does not cover %s", from, to).isFalse();
    }

    private static void assertNotWidening(CompressionDictionaryScope from, CompressionDictionaryScope to, String message)
    {
        assertThatThrownBy(() -> from.checkCanBeWidenedTo(to))
        .as("%s to %s", from, to)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(message);
    }

    private static CompressionDictionaryScope nodes(int... ids)
    {
        return CompressionDictionaryScope.of(Arrays.stream(ids).mapToObj(id -> new NodePlace(new NodeId(id))).collect(Collectors.toList()));
    }

    private static CompressionDictionaryScope unknown()
    {
        return CompressionDictionaryScope.fromValue(Set.of(RACK));
    }
}
