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

package org.apache.cassandra.distributed.test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.membership.Location;
import org.apache.cassandra.tcm.membership.NodeAddresses;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeVersion;
import org.apache.cassandra.tcm.ownership.DataPlacement;
import org.apache.cassandra.tcm.ownership.VersionedEndpoints;
import org.apache.cassandra.tcm.transformations.Register;
import org.apache.cassandra.tcm.transformations.UnsafeJoin;
import org.apache.cassandra.utils.Shared;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class DataPlacementsTableTest extends TestBaseImpl
{
    private static final int NUM_FAKE_NODES = 60;
    private static final String DC1 = "datacenter0";
    private static final String DC2 = "datacenter1";
    private static final String SIMPLE_KS = "test_ks_simple";
    private static final String NTS_KS = "test_ks_nts";

    private static final String SIMPLE_TABLE1 = "tbl1";
    private static final String SIMPLE_TABLE2 = "tbl2";
    private static final String NTS_TABLE1 = "nts_tbl1";
    private static final String NTS_TABLE2 = "nts_tbl2";

    private static final long REAL_NODE_TOKEN = Long.MAX_VALUE - 1;

    private static Cluster CLUSTER;
    private static Map<Range, Set<Endpoint>> SIMPLE_EXPECTED;
    private static Map<Range, Set<Endpoint>> NTS_EXPECTED;

    @BeforeClass
    public static void setup() throws IOException
    {
        CLUSTER = Cluster.build(1)
                         .withConfig(c -> c.set("num_tokens", 1)
                                           .set("initial_token", Long.toString(REAL_NODE_TOKEN)))
                         .start();

        // Register 60 fake nodes split evenly across the two DCs, evenly spaced around the ring.
        CLUSTER.get(1).runOnInstance(() -> {
            try
            {
                for (int i = 0; i < NUM_FAKE_NODES; i++)
                {
                    String dc = (i % 2 == 0) ? DC1 : DC2;
                    String address = "127.0.1." + (i + 2);

                    InetAddressAndPort addr = InetAddressAndPort.getByName(address);
                    NodeAddresses nodeAddresses = new NodeAddresses(addr);
                    Location location = new Location(dc, "rack1");

                    ClusterMetadata metadata = ClusterMetadataService.instance().commit(
                            new Register(nodeAddresses, location, NodeVersion.CURRENT));

                    NodeId nodeId = metadata.directory.peerId(addr);

                    long token = Long.MIN_VALUE + (long) i * (Long.MAX_VALUE / (NUM_FAKE_NODES / 2));
                    Set<Token> tokens = new HashSet<>();
                    tokens.add(new Murmur3Partitioner.LongToken(token));

                    UnsafeJoin.unsafeJoin(nodeId, tokens);
                }
            }
            catch (Exception e)
            {
                throw new RuntimeException("Failed to register fake nodes", e);
            }
        });

        CLUSTER.schemaChange(String.format("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}", SIMPLE_KS));
        CLUSTER.schemaChange(String.format("CREATE KEYSPACE %s WITH replication = {'class': 'NetworkTopologyStrategy', '%s': 2, '%s': 2}", NTS_KS, DC1, DC2));

        CLUSTER.schemaChange(String.format("CREATE TABLE %s.%s (id int PRIMARY KEY, val text)", SIMPLE_KS, SIMPLE_TABLE1));
        CLUSTER.schemaChange(String.format("CREATE TABLE %s.%s (id int PRIMARY KEY, val text)", SIMPLE_KS, SIMPLE_TABLE2));
        CLUSTER.schemaChange(String.format("CREATE TABLE %s.%s (id int PRIMARY KEY, val text)", NTS_KS, NTS_TABLE1));
        CLUSTER.schemaChange(String.format("CREATE TABLE %s.%s (id int PRIMARY KEY, val text)", NTS_KS, NTS_TABLE2));

        SIMPLE_EXPECTED = fetchExpectedPlacements(SIMPLE_KS);
        NTS_EXPECTED    = fetchExpectedPlacements(NTS_KS);
    }


    @AfterClass
    public static void cleanup()
    {
        if (CLUSTER != null)
            CLUSTER.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testPlacements()
    {
        Map<String, Map<Range, Set<Endpoint>>> allExpected = new HashMap<>();
        allExpected.put(SIMPLE_KS, SIMPLE_EXPECTED);
        allExpected.put(NTS_KS,    NTS_EXPECTED);

        Object[][] result = CLUSTER.coordinator(1)
                                   .execute("SELECT keyspace_name, table_name, range_start, range_end, token_type, " +
                                            "range_start_bytes, range_end_bytes, read_endpoints, write_endpoints, " +
                                            "read_replicas, write_replicas " +
                                            "FROM system_views.data_placements",
                                            ConsistencyLevel.ONE);

        assertThat(result).isNotEmpty();

        Map<String, Set<String>> tablesFound = new HashMap<>();
        Map<String, Set<Range>>  rangesFound = new HashMap<>();
        for (String ks : allExpected.keySet())
        {
            tablesFound.put(ks, new HashSet<>());
            rangesFound.put(ks, new HashSet<>());
        }

        for (Object[] row : result)
        {
            String keyspace = (String) row[0];
            Map<Range, Set<Endpoint>> expected = allExpected.get(keyspace);
            if (expected == null)
                continue; // skip system keyspaces

            String tableName           = (String)     row[1];
            String rangeStart          = (String)     row[2];
            String rangeEnd            = (String)     row[3];
            String tokenType           = (String)     row[4];
            ByteBuffer rangeStartBytes = (ByteBuffer) row[5];
            ByteBuffer rangeEndBytes   = (ByteBuffer) row[6];
            Set<String>  readEndpoints  = (Set<String>)  row[7];
            Set<String>  writeEndpoints = (Set<String>)  row[8];
            Set<Integer> readReplicas   = (Set<Integer>) row[9];
            Set<Integer> writeReplicas  = (Set<Integer>) row[10];

            assertThat(tokenType).as("token_type for %s.%s", keyspace, tableName)
                                 .contains("Murmur3Partitioner");
            assertThat(rangeStartBytes).as("range_start_bytes for %s.%s", keyspace, tableName).isNotNull();
            assertThat(rangeEndBytes).as("range_end_bytes for %s.%s", keyspace, tableName).isNotNull();

            Range range = new Range(Long.parseLong(rangeStart), Long.parseLong(rangeEnd));
            Set<Endpoint> exp = expected.get(range);
            assertThat(exp).as("range (%s, %s] not in expected for %s", rangeStart, rangeEnd, keyspace)
                           .isNotNull();

            Set<String>  expectedIps     = exp.stream().map(e -> e.ip).collect(Collectors.toSet());
            Set<Integer> expectedNodeIds = exp.stream().map(e -> e.nodeId).collect(Collectors.toSet());

            assertThat(readEndpoints).as("read endpoints for %s.%s (%s,%s]", keyspace, tableName, rangeStart, rangeEnd)
                                     .isEqualTo(expectedIps);
            assertThat(writeEndpoints).as("write endpoints for %s.%s (%s,%s]", keyspace, tableName, rangeStart, rangeEnd)
                                      .isEqualTo(expectedIps);
            assertThat(readReplicas).as("read replica node-ids for %s.%s (%s,%s]", keyspace, tableName, rangeStart, rangeEnd)
                                    .isEqualTo(expectedNodeIds);
            assertThat(writeReplicas).as("write replica node-ids for %s.%s (%s,%s]", keyspace, tableName, rangeStart, rangeEnd)
                                     .isEqualTo(expectedNodeIds);

            tablesFound.get(keyspace).add(tableName);
            rangesFound.get(keyspace).add(range);
        }

        assertThat(tablesFound.get(SIMPLE_KS)).as("tables in " + SIMPLE_KS).contains(SIMPLE_TABLE1, SIMPLE_TABLE2);
        assertThat(rangesFound.get(SIMPLE_KS)).as("unique ranges in " + SIMPLE_KS).hasSize(SIMPLE_EXPECTED.size());
        assertThat(tablesFound.get(NTS_KS)).as("tables in " + NTS_KS).contains(NTS_TABLE1, NTS_TABLE2);
        assertThat(rangesFound.get(NTS_KS)).as("unique ranges in " + NTS_KS).hasSize(NTS_EXPECTED.size());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testQueryByKeyspaceAndTable()
    {
        Object[][] result = CLUSTER.coordinator(1)
                                   .execute("SELECT keyspace_name, table_name, range_start, range_end, " +
                                            "read_endpoints, write_endpoints " +
                                            "FROM system_views.data_placements WHERE keyspace_name = ? AND table_name = ?",
                                            ConsistencyLevel.ONE, NTS_KS, NTS_TABLE1);

        assertThat(result).isNotEmpty();

        Set<Range> rangesFound = new HashSet<>();
        for (Object[] row : result)
        {
            assertThat(row[0]).isEqualTo(NTS_KS);
            assertThat(row[1]).isEqualTo(NTS_TABLE1);

            String rangeStart = (String) row[2];
            String rangeEnd   = (String) row[3];
            Set<String> readEndpoints  = (Set<String>) row[4];
            Set<String> writeEndpoints = (Set<String>) row[5];

            Range range = new Range(Long.parseLong(rangeStart), Long.parseLong(rangeEnd));
            Set<Endpoint> expected = NTS_EXPECTED.get(range);
            assertThat(expected).as("range (%s, %s] not found in simulation", rangeStart, rangeEnd)
                                .isNotNull();

            Set<String> expectedIps = expected.stream().map(e -> e.ip).collect(Collectors.toSet());
            assertThat(readEndpoints).as("read endpoints for (%s,%s]", rangeStart, rangeEnd)
                                     .isEqualTo(expectedIps);
            assertThat(writeEndpoints).as("write endpoints for (%s,%s]", rangeStart, rangeEnd)
                                      .isEqualTo(expectedIps);

            rangesFound.add(range);
        }

        // The 2-component query must return ALL ranges for the table, not a subset.
        assertThat(rangesFound).as("ranges for " + NTS_KS + "." + NTS_TABLE1)
                               .hasSize(NTS_EXPECTED.size());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testQueryByFullPartitionKey()
    {
        String rangeStart, rangeEnd;
        {
            Range range = NTS_EXPECTED.keySet().iterator().next();
            rangeStart = String.valueOf(range.start);
            rangeEnd = String.valueOf(range.end);
        }

        Object[][] result = CLUSTER.coordinator(1)
                                   .execute("SELECT keyspace_name, table_name, range_start, range_end, " +
                                            "read_endpoints, write_endpoints, read_replicas, write_replicas " +
                                            "FROM system_views.data_placements " +
                                            "WHERE keyspace_name = ? AND table_name = ? AND range_start = ? AND range_end = ?",
                                            ConsistencyLevel.ONE, NTS_KS, NTS_TABLE1, rangeStart, rangeEnd);

        assertThat(result).as("full-PK query must return exactly one row").hasNumberOfRows(1);

        Object[] row = result[0];
        assertThat(row[0]).as("keyspace_name").isEqualTo(NTS_KS);
        assertThat(row[1]).as("table_name").isEqualTo(NTS_TABLE1);
        assertThat(row[2]).as("range_start").isEqualTo(rangeStart);
        assertThat(row[3]).as("range_end").isEqualTo(rangeEnd);

        Set<String>  readEndpoints  = (Set<String>)  row[4];
        Set<String>  writeEndpoints = (Set<String>)  row[5];
        Set<Integer> readReplicas   = (Set<Integer>) row[6];
        Set<Integer> writeReplicas  = (Set<Integer>) row[7];

        Range range = new Range(Long.parseLong(rangeStart), Long.parseLong(rangeEnd));
        Set<Endpoint> expected = NTS_EXPECTED.get(range);
        assertThat(expected).as("range (%s, %s] not found in simulation", rangeStart, rangeEnd)
                            .isNotNull();

        Set<String>  expectedIps     = expected.stream().map(e -> e.ip).collect(Collectors.toSet());
        Set<Integer> expectedNodeIds = expected.stream().map(e -> e.nodeId).collect(Collectors.toSet());

        assertThat(readEndpoints).as("read endpoints").isEqualTo(expectedIps);
        assertThat(writeEndpoints).as("write endpoints").isEqualTo(expectedIps);
        assertThat(readReplicas).as("read replica node-ids").isEqualTo(expectedNodeIds);
        assertThat(writeReplicas).as("write replica node-ids").isEqualTo(expectedNodeIds);
    }

    @Shared
    public static final class Range
    {
        public final long start;
        public final long end;

        public Range(long start, long end)
        {
            this.start = start;
            this.end = end;
        }

        @Override
        public boolean equals(Object o)
        {
            if (!(o instanceof Range)) return false;
            Range r = (Range) o;
            return r.start == start && r.end == end;
        }

        @Override
        public int hashCode()
        {
            return Long.hashCode(start) * 31 + Long.hashCode(end);
        }

        @Override
        public String toString()
        {
            return "(" + start + ", " + end + "]";
        }
    }

    @Shared
    public static final class Endpoint
    {
        public final String ip;
        public final int nodeId;

        public Endpoint(String ip, int nodeId)
        {
            this.ip = ip;
            this.nodeId = nodeId;
        }

        @Override
        public boolean equals(Object o)
        {
            if (!(o instanceof Endpoint)) return false;
            Endpoint e = (Endpoint) o;
            return e.nodeId == nodeId && e.ip.equals(ip);
        }

        @Override
        public int hashCode()
        {
            return ip.hashCode() * 31 + nodeId;
        }
    }

    private static Map<Range, Set<Endpoint>> fetchExpectedPlacements(String keyspaceName)
    {
        return CLUSTER.get(1).callOnInstance(() -> {
            Map<Range, Set<Endpoint>> result = new HashMap<>();
            ClusterMetadata metadata = ClusterMetadata.current();
            KeyspaceMetadata ksm = metadata.schema.getKeyspaceMetadata(keyspaceName);
            DataPlacement placement = metadata.placements.get(ksm.params.replication);
            for (org.apache.cassandra.dht.Range<Token> range : placement.reads.ranges)
            {
                VersionedEndpoints.ForRange re = placement.reads.forRange(range);
                Set<Endpoint> endpoints = new HashSet<>();
                for (Replica r : re.get())
                {
                    NodeId nid = metadata.directory.peerId(r.endpoint());
                    endpoints.add(new Endpoint(r.endpoint().toString(), nid.id()));
                }
                result.put(new Range(Long.parseLong(range.left.toString()),
                                     Long.parseLong(range.right.toString())),
                           endpoints);
            }
            return result;
        });
    }
}
