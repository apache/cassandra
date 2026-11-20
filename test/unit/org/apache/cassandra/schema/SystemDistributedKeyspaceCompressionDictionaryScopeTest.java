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

package org.apache.cassandra.schema;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.compression.CompressionDictionary;
import org.apache.cassandra.db.compression.CompressionDictionary.DictId;
import org.apache.cassandra.db.compression.CompressionDictionary.Kind;
import org.apache.cassandra.db.compression.CompressionDictionary.LightweightCompressionDictionary;
import org.apache.cassandra.db.compression.CompressionDictionaryScope;
import org.apache.cassandra.db.compression.CompressionDictionaryScope.DatacenterPlace;
import org.apache.cassandra.db.compression.CompressionDictionaryScope.NodePlace;
import org.apache.cassandra.db.compression.CompressionDictionaryScope.RackPlace;
import org.apache.cassandra.db.compression.ZstdCompressionDictionary;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.Location;
import org.apache.cassandra.tcm.membership.NodeId;

import static java.lang.String.format;
import static org.apache.cassandra.db.compression.CompressionDictionaryScope.CLUSTER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Storing, promoting and discarding dictionaries by their scope, stored in the scope column as a frozen map.
 */
public class SystemDistributedKeyspaceCompressionDictionaryScopeTest extends CQLTester
{
    private static final String KS = "scope_keyspace";
    private static final String TABLE = "scope_table";

    private String tableId;
    private CompressionDictionaryScope thisNode;

    @BeforeClass
    public static void setup()
    {
        // lightweight transactions
        requireNetwork();
    }

    @Before
    public void setUp()
    {
        tableId = UUID.randomUUID().toString();
        thisNode = CompressionDictionaryScope.node(ClusterMetadata.current().myNodeId());
    }

    @Test
    public void testStoreAndReadScope()
    {
        store(100, CLUSTER);
        store(200, thisNode);

        assertThat(scopeColumn(100)).isNull();
        assertThat(scopeColumn(200)).isEqualTo(Set.of(Map.of("node_id", String.valueOf(ClusterMetadata.current().myNodeId().id()))));

        // several places
        CompressionDictionaryScope twoNodes = CompressionDictionaryScope.of(List.of(new NodePlace(new NodeId(42)), new NodePlace(ClusterMetadata.current().myNodeId())));
        store(300, twoNodes);
        assertThat(scopeColumn(300)).isEqualTo(twoNodes.toValue());
        assertThat(SystemDistributedKeyspace.readLightweightLatestCompressionDictionary(KS, TABLE, tableId).scope).isEqualTo(twoNodes);

        LightweightCompressionDictionary latest = SystemDistributedKeyspace.retrieveLightweightCompressionDictionaries(KS, TABLE, tableId, 300L, 1).get(0);
        assertThat(latest.dictId.id).isEqualTo(200);
        assertThat(latest.scope).isEqualTo(thisNode);
        assertThat(SystemDistributedKeyspace.retrieveCompressionDictionary(KS, TABLE, tableId, 200).scope()).isEqualTo(thisNode);
        assertThat(SystemDistributedKeyspace.retrieveCompressionDictionary(KS, TABLE, tableId, 100).scope()).isEqualTo(CLUSTER);
    }

    @Test
    public void testPromote()
    {
        assertThatThrownBy(() -> SystemDistributedKeyspace.promoteCompressionDictionary(KS, TABLE, tableId, CLUSTER))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Could not find a dictionary to promote");

        store(100, thisNode);
        assertThatThrownBy(() -> SystemDistributedKeyspace.promoteCompressionDictionary(KS, TABLE, tableId, CompressionDictionaryScope.node(new NodeId(42))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("The latest dictionary 100 of table " + KS + '.' + TABLE + ", scoped to " + thisNode + ", can not be promoted: " +
                              "scope node 42 would no longer contain " + thisNode + ", a scope can only be widened");

        LightweightCompressionDictionary promoted = SystemDistributedKeyspace.promoteCompressionDictionary(KS, TABLE, tableId, CLUSTER);
        assertThat(promoted.dictId.id).isEqualTo(100);
        assertThat(promoted.scope).as("the scope it was promoted to").isEqualTo(CLUSTER);
        assertThat(scopeColumn(100)).isNull();

        assertThatThrownBy(() -> SystemDistributedKeyspace.promoteCompressionDictionary(KS, TABLE, tableId, CLUSTER))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("The latest dictionary 100 of table " + KS + '.' + TABLE + " is already used by all nodes");
    }

    @Test
    public void testWidening()
    {
        NodeId self = ClusterMetadata.current().myNodeId();
        CompressionDictionaryScope selfAnd42 = nodes(self, new NodeId(42));
        CompressionDictionaryScope selfAnd42And43 = nodes(self, new NodeId(42), new NodeId(43));
        store(100, thisNode);

        // narrower to wider, step by step
        assertThat(SystemDistributedKeyspace.promoteCompressionDictionary(KS, TABLE, tableId, selfAnd42).scope).isEqualTo(selfAnd42);
        assertThat(scopeColumn(100)).isEqualTo(selfAnd42.toValue());
        assertThat(SystemDistributedKeyspace.promoteCompressionDictionary(KS, TABLE, tableId, selfAnd42And43).scope).isEqualTo(selfAnd42And43);
        assertThat(scopeColumn(100)).isEqualTo(selfAnd42And43.toValue());

        // never shrinks, never moves sideways, never to the same scope, never to places this version does not know
        assertNotPromoted(selfAnd42And43, selfAnd42, "would no longer contain node 43");
        assertNotPromoted(selfAnd42And43, thisNode, "would no longer contain node 42, node 43");
        assertNotPromoted(selfAnd42And43, nodes(new NodeId(42), new NodeId(43), new NodeId(44)), "would no longer contain " + thisNode);
        assertNotPromoted(selfAnd42And43, nodes(new NodeId(43), new NodeId(42), self), "the scope would stay " + selfAnd42And43 + ", nothing to promote");
        assertNotPromoted(selfAnd42And43, CompressionDictionaryScope.fromValue(Set.of(Map.of("region", "eu"))), "contains places this version does not know");
        assertThat(scopeColumn(100)).as("rejected promotions do not change the scope").isEqualTo(selfAnd42And43.toValue());

        // to the whole cluster, from where it can not go anywhere
        assertThat(SystemDistributedKeyspace.promoteCompressionDictionary(KS, TABLE, tableId, CLUSTER).scope).isEqualTo(CLUSTER);
        assertThat(scopeColumn(100)).isNull();
        for (CompressionDictionaryScope target : List.of(CLUSTER, thisNode, selfAnd42And43))
        {
            assertThatThrownBy(() -> SystemDistributedKeyspace.promoteCompressionDictionary(KS, TABLE, tableId, target))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("is already used by all nodes");
        }
    }

    @Test
    public void testWideningAlongRacksAndDatacenters()
    {
        Location location = ClusterMetadata.current().directory.location(ClusterMetadata.current().myNodeId());
        CompressionDictionaryScope rack = CompressionDictionaryScope.of(List.of(new RackPlace(location.datacenter, location.rack)));
        CompressionDictionaryScope otherRack = CompressionDictionaryScope.of(List.of(new RackPlace(location.datacenter, location.rack + "_other")));
        CompressionDictionaryScope datacenter = CompressionDictionaryScope.of(List.of(new DatacenterPlace(location.datacenter)));
        store(100, thisNode);

        // a rack this node is not in does not contain the node scope
        assertNotPromoted(thisNode, otherRack, "would no longer contain " + thisNode);

        // node, its rack, adding a rack, its datacenter, the cluster
        assertThat(SystemDistributedKeyspace.promoteCompressionDictionary(KS, TABLE, tableId, rack).scope).isEqualTo(rack);
        assertThat(scopeColumn(100)).isEqualTo(Set.of(Map.of("dc", location.datacenter, "rack", location.rack)));
        CompressionDictionaryScope twoRacks = SystemDistributedKeyspace.promoteCompressionDictionary(KS, TABLE, tableId,
                                                                                                    current -> current.plus(otherRack.places())).scope;
        assertThat(twoRacks).isEqualTo(CompressionDictionaryScope.of(List.of(new RackPlace(location.datacenter, location.rack),
                                                                             new RackPlace(location.datacenter, location.rack + "_other"))));

        // adding a rack which is in the scope already, or a node located in it, is not a promotion
        assertNotPromoted(twoRacks, twoRacks.plus(rack.places()), "the scope would stay");
        // a node is never dropped by normalization, so adding this node makes the scope list it explicitly
        assertThat(twoRacks.plus(thisNode.places()).places()).containsAll(twoRacks.places()).containsAll(thisNode.places());
        // back to the node or to one of the racks
        assertNotPromoted(twoRacks, thisNode, "would no longer contain");
        assertNotPromoted(twoRacks, rack, "would no longer contain " + otherRack);

        // the datacenter absorbs its racks
        assertThat(SystemDistributedKeyspace.promoteCompressionDictionary(KS, TABLE, tableId,
                                                                          current -> current.plus(datacenter.places())).scope)
        .isEqualTo(datacenter);
        assertThat(scopeColumn(100)).isEqualTo(Set.of(Map.of("dc", location.datacenter)));
        assertNotPromoted(datacenter, twoRacks, "would no longer contain " + datacenter);
        assertNotPromoted(datacenter, rack, "would no longer contain " + datacenter);
        assertNotPromoted(datacenter, thisNode, "would no longer contain " + datacenter);

        assertThat(SystemDistributedKeyspace.promoteCompressionDictionary(KS, TABLE, tableId, CLUSTER).scope).isEqualTo(CLUSTER);
        assertThat(scopeColumn(100)).isNull();
    }

    @Test
    public void testPromoteScopeOfLaterVersion()
    {
        // a scope this version does not understand can still be promoted to the whole cluster, but not to a node
        CompressionDictionaryScope region = CompressionDictionaryScope.fromValue(Set.of(Map.of("region", "eu")));
        store(100, region);
        assertNotPromoted(region, thisNode, "would no longer contain {region=eu}");
        assertThat(SystemDistributedKeyspace.promoteCompressionDictionary(KS, TABLE, tableId, CLUSTER).scope).isEqualTo(CLUSTER);
        assertThat(scopeColumn(100)).isNull();
    }

    @Test
    public void testDiscard()
    {
        assertThatThrownBy(() -> SystemDistributedKeyspace.discardCompressionDictionary(KS, TABLE, tableId))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Could not find a dictionary to discard");

        store(100, CLUSTER);
        assertThatThrownBy(() -> SystemDistributedKeyspace.discardCompressionDictionary(KS, TABLE, tableId))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("is used by all nodes, only a dictionary scoped narrower than the cluster can be discarded");

        store(200, thisNode);
        LightweightCompressionDictionary discarded = SystemDistributedKeyspace.discardCompressionDictionary(KS, TABLE, tableId);
        assertThat(discarded.dictId.id).isEqualTo(200);
        assertThat(discarded.scope).isEqualTo(thisNode);
        assertThat(SystemDistributedKeyspace.retrieveCompressionDictionary(KS, TABLE, tableId, 200)).isNull();
        assertThat(SystemDistributedKeyspace.readLightweightLatestCompressionDictionary(KS, TABLE, tableId).dictId.id).isEqualTo(100);
    }

    @Test
    public void testStoredScopeNotInCanonicalForm()
    {
        // e.g. written by hand or by a later version: the node id '01' is not a node of this version, two maps print
        // the same; the stored value is the condition of the transactions, not the value of the parsed scope
        storeRaw(100, Set.of(Map.of("node_id", "01")));
        assertThat(SystemDistributedKeyspace.readLightweightLatestCompressionDictionary(KS, TABLE, tableId).scope.isKnown()).isFalse();
        assertThat(SystemDistributedKeyspace.promoteCompressionDictionary(KS, TABLE, tableId, CLUSTER).scope).isEqualTo(CLUSTER);
        assertThat(scopeColumn(100)).isNull();

        Set<Map<String, String>> sameString = Set.of(Map.of("a", "b, c=d"), Map.of("a", "b", "c", "d"));
        storeRaw(200, sameString);
        assertThat(SystemDistributedKeyspace.discardCompressionDictionary(KS, TABLE, tableId).dictId.id).isEqualTo(200);
        assertThat(SystemDistributedKeyspace.retrieveCompressionDictionary(KS, TABLE, tableId, 200)).isNull();
    }

    @Test
    public void testStoreOnce()
    {
        store(100, thisNode);
        assertThatThrownBy(() -> store(100, CLUSTER))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Dictionary 100 of table " + KS + '.' + TABLE + " is stored already");
        assertThat(scopeColumn(100)).as("not overwritten").isEqualTo(thisNode.toValue());
    }

    @Test
    public void testTransactionNotApplied()
    {
        store(100, thisNode);
        LightweightCompressionDictionary read = SystemDistributedKeyspace.readLightweightLatestCompressionDictionary(KS, TABLE, tableId);

        // promoted by someone else after it was read
        assertThat(SystemDistributedKeyspace.promoteCompressionDictionary(KS, TABLE, tableId, CLUSTER).dictId.id).isEqualTo(100);
        assertThatThrownBy(() -> SystemDistributedKeyspace.checkApplied(promote(100, read.scope), read, "promoted"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("was not promoted, its scope changed to cluster meanwhile");

        // scoped to another node than expected
        store(200, CompressionDictionaryScope.node(new NodeId(42)));
        LightweightCompressionDictionary other = SystemDistributedKeyspace.readLightweightLatestCompressionDictionary(KS, TABLE, tableId);
        assertThatThrownBy(() -> SystemDistributedKeyspace.checkApplied(promote(200, thisNode), other, "promoted"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("was not promoted, its scope changed to node 42 meanwhile");

        // deleted after it was read, e.g. discarded on another node
        SystemDistributedKeyspace.deleteCompressionDictionary(KS, TABLE, tableId, 200);
        assertThatThrownBy(() -> SystemDistributedKeyspace.checkApplied(promote(200, other.scope), other, "discarded"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("was not discarded, it does not exist anymore");

        // applied
        store(300, thisNode);
        LightweightCompressionDictionary applied = SystemDistributedKeyspace.readLightweightLatestCompressionDictionary(KS, TABLE, tableId);
        SystemDistributedKeyspace.checkApplied(promote(300, thisNode), applied, "promoted");
        assertThat(scopeColumn(300)).isNull();
    }

    private void assertNotPromoted(CompressionDictionaryScope from, CompressionDictionaryScope to, String message)
    {
        assertThatThrownBy(() -> SystemDistributedKeyspace.promoteCompressionDictionary(KS, TABLE, tableId, to))
        .as("%s to %s", from, to)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("scoped to " + from + ", can not be promoted: ")
        .hasMessageContaining(message);
    }

    private static CompressionDictionaryScope nodes(NodeId... ids)
    {
        List<NodePlace> places = new ArrayList<>();
        for (NodeId id : ids)
            places.add(new NodePlace(id));
        return CompressionDictionaryScope.of(places);
    }

    private void store(long dictId, CompressionDictionaryScope scope)
    {
        byte[] dict = ("dictionary " + dictId).getBytes();
        ZstdCompressionDictionary dictionary = new ZstdCompressionDictionary(new DictId(Kind.ZSTD, dictId),
                                                                             scope,
                                                                             dict,
                                                                             CompressionDictionary.calculateChecksum((byte) Kind.ZSTD.ordinal(), dictId, dict),
                                                                             Instant.now());
        SystemDistributedKeyspace.storeCompressionDictionary(KS, TABLE, tableId, dictionary);
    }

    /** stores a dictionary with the scope column set to the given value, as a later version or a hand might */
    private void storeRaw(long dictId, Set<Map<String, String>> scope)
    {
        byte[] dict = ("dictionary " + dictId).getBytes();
        QueryProcessor.execute(format("INSERT INTO %s.%s (keyspace_name, table_name, table_id, kind, dict_id, dict, dict_length, dict_checksum, created_at, scope) " +
                                      "VALUES (?, ?, ?, 'ZSTD', ?, ?, ?, ?, toTimestamp(now()), ?)",
                                      SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, SystemDistributedKeyspace.COMPRESSION_DICTIONARIES),
                               ConsistencyLevel.QUORUM,
                               KS, TABLE, tableId, dictId, java.nio.ByteBuffer.wrap(dict), dict.length,
                               CompressionDictionary.calculateChecksum((byte) Kind.ZSTD.ordinal(), dictId, dict), scope);
    }

    /** promotes the dictionary to the whole cluster when its scope is the expected one, the way promotion does */
    private UntypedResultSet promote(long dictId, CompressionDictionaryScope expected)
    {
        return QueryProcessor.execute(format("UPDATE %s.%s SET scope = null " +
                                             "WHERE keyspace_name = ? AND table_name = ? AND table_id = ? AND dict_id = ? IF scope = ?",
                                             SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, SystemDistributedKeyspace.COMPRESSION_DICTIONARIES),
                                      ConsistencyLevel.QUORUM,
                                      KS, TABLE, tableId, dictId, expected.toValue());
    }

    private Set<Map<String, String>> scopeColumn(long dictId)
    {
        UntypedResultSet result = QueryProcessor.execute(format("SELECT scope FROM %s.%s WHERE keyspace_name = ? AND table_name = ? AND table_id = ? AND dict_id = ?",
                                                                SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, SystemDistributedKeyspace.COMPRESSION_DICTIONARIES),
                                                         ConsistencyLevel.QUORUM,
                                                         KS, TABLE, tableId, dictId);
        assertThat(result).hasSize(1);
        UntypedResultSet.Row row = result.one();
        return row.has("scope") ? row.getFrozenSet("scope", CompressionDictionaryScope.TYPE.getElementsType()) : null;
    }
}
