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
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ownership.DataPlacement;
import org.apache.cassandra.tcm.ownership.VersionedEndpoints;
import org.apache.cassandra.utils.Shared;

import static org.assertj.core.api.Assertions.assertThat;

public class PartitionLocationTableTest extends TestBaseImpl
{
    private static final int NUM_NODES = 3;
    private static final String KEYSPACE = "test_ks";

    private static Cluster CLUSTER;

    @BeforeClass
    public static void setup() throws IOException
    {
        CLUSTER = Cluster.build(NUM_NODES).start();
    }

    @AfterClass
    public static void cleanup()
    {
        if (CLUSTER != null)
            CLUSTER.close();
    }


    @Test
    @SuppressWarnings("unchecked")
    public void testSimplePartitionKey()
    {
        CLUSTER.schemaChange("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}");
        CLUSTER.schemaChange("CREATE TABLE IF NOT EXISTS " + KEYSPACE + ".tbl1 (col1 int PRIMARY KEY, col2 text)");

        ExpectedLocation expected = computeExpectedLocation(KEYSPACE, "tbl1", "123");

        Object[][] result = CLUSTER.coordinator(1).execute(
                "SELECT tkn, range_start, range_end, range_start_bytes, range_end_bytes, " +
                "read_endpoints, write_endpoints, read_replicas, write_replicas " +
                "FROM system_views.partition_location " +
                "WHERE keyspace_name = ? AND table_name = ? AND key = ?",
                ConsistencyLevel.ONE,
                KEYSPACE, "tbl1", "123");

        Assert.assertEquals(1, result.length);
        Object[] row = result[0];
        String       token          = (String)      row[0];
        String       rangeStart     = (String)      row[1];
        String       rangeEnd       = (String)      row[2];
        ByteBuffer   rangeStartBytes = (ByteBuffer)  row[3];
        ByteBuffer   rangeEndBytes   = (ByteBuffer)  row[4];
        Set<String>  readEndpoints  = (Set<String>)  row[5];
        Set<String>  writeEndpoints = (Set<String>)  row[6];
        Set<Integer> readReplicas   = (Set<Integer>) row[7];
        Set<Integer> writeReplicas  = (Set<Integer>) row[8];

        assertThat(token).as("token").isEqualTo(expected.token);
        assertThat(rangeStart).as("range_start").isEqualTo(expected.rangeStart);
        assertThat(rangeEnd).as("range_end").isEqualTo(expected.rangeEnd);
        assertThat(rangeStartBytes).as("range_start_bytes").isNotNull();
        assertThat(rangeEndBytes).as("range_end_bytes").isNotNull();

        assertThat(readEndpoints).as("read endpoints").isEqualTo(expected.readEndpoints);
        assertThat(writeEndpoints).as("write endpoints").isEqualTo(expected.writeEndpoints);
        assertThat(readReplicas).as("read replica node-ids").isEqualTo(expected.readReplicas);
        assertThat(writeReplicas).as("write replica node-ids").isEqualTo(expected.writeReplicas);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCompositePartitionKey()
    {
        CLUSTER.schemaChange("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}");
        CLUSTER.schemaChange("CREATE TABLE IF NOT EXISTS " + KEYSPACE + ".tbl2 (col1 int, col2 text, col3 int, PRIMARY KEY ((col1, col2), col3))");

        ExpectedLocation expected = computeExpectedLocation(KEYSPACE, "tbl2", "123:value1");

        Object[][] result = CLUSTER.coordinator(1).execute(
                "SELECT tkn, range_start, range_end, range_start_bytes, range_end_bytes, " +
                "read_endpoints, write_endpoints, read_replicas, write_replicas " +
                "FROM system_views.partition_location " +
                "WHERE keyspace_name = ? AND table_name = ? AND key = ?",
                ConsistencyLevel.ONE,
                KEYSPACE, "tbl2", "123:value1");

        Assert.assertEquals(1, result.length);
        Object[] row = result[0];
        String       token          = (String)      row[0];
        String       rangeStart     = (String)      row[1];
        String       rangeEnd       = (String)      row[2];
        ByteBuffer   rangeStartBytes = (ByteBuffer)  row[3];
        ByteBuffer   rangeEndBytes   = (ByteBuffer)  row[4];
        Set<String>  readEndpoints  = (Set<String>)  row[5];
        Set<String>  writeEndpoints = (Set<String>)  row[6];
        Set<Integer> readReplicas   = (Set<Integer>) row[7];
        Set<Integer> writeReplicas  = (Set<Integer>) row[8];

        assertThat(token).as("token").isEqualTo(expected.token);
        assertThat(rangeStart).as("range_start").isEqualTo(expected.rangeStart);
        assertThat(rangeEnd).as("range_end").isEqualTo(expected.rangeEnd);
        assertThat(rangeStartBytes).as("range_start_bytes").isNotNull();
        assertThat(rangeEndBytes).as("range_end_bytes").isNotNull();

        assertThat(readEndpoints).as("read endpoints").isEqualTo(expected.readEndpoints);
        assertThat(writeEndpoints).as("write endpoints").isEqualTo(expected.writeEndpoints);
        assertThat(readReplicas).as("read replica node-ids").isEqualTo(expected.readReplicas);
        assertThat(writeReplicas).as("write replica node-ids").isEqualTo(expected.writeReplicas);
    }

    @Shared
    public static final class ExpectedLocation // class needs to be public since it's shared
    {
        public final String token;
        public final String rangeStart;
        public final String rangeEnd;
        public final Set<String>  readEndpoints;
        public final Set<String>  writeEndpoints;
        public final Set<Integer> readReplicas;
        public final Set<Integer> writeReplicas;

        public ExpectedLocation(String token, String rangeStart, String rangeEnd,
                                Set<String> readEndpoints, Set<String> writeEndpoints,
                                Set<Integer> readReplicas, Set<Integer> writeReplicas)
        {
            this.token = token;
            this.rangeStart = rangeStart;
            this.rangeEnd = rangeEnd;
            this.readEndpoints = readEndpoints;
            this.writeEndpoints = writeEndpoints;
            this.readReplicas = readReplicas;
            this.writeReplicas = writeReplicas;
        }
    }

    private static ExpectedLocation computeExpectedLocation(String ks, String tbl, String key)
    {
        return CLUSTER.get(1).callOnInstance(() -> {
            ClusterMetadata metadata = ClusterMetadata.current();
            KeyspaceMetadata ksm = metadata.schema.getKeyspaceMetadata(ks);
            TableMetadata table = ksm.getTableOrViewNullable(tbl);

            ByteBuffer partitionKeyBytes = table.partitionKeyType.fromString(key);
            DecoratedKey dk = table.partitioner.decorateKey(partitionKeyBytes);
            Token token = dk.getToken();

            DataPlacement placement = metadata.placements.get(ksm.params.replication);
            VersionedEndpoints.ForRange readEps  = placement.reads.forRange(token);
            VersionedEndpoints.ForRange writeEps = placement.writes.forRange(token);
            Range<Token> range = readEps.get().range();

            Set<String> readEndpoints = readEps.get().stream()
                                               .map(r -> r.endpoint().toString())
                                               .collect(Collectors.toSet());
            Set<String> writeEndpoints = writeEps.get().stream()
                                                 .map(r -> r.endpoint().toString())
                                                 .collect(Collectors.toSet());
            Set<Integer> readReplicas = readEps.get().stream()
                                               .map(r -> metadata.directory.peerId(r.endpoint()).id())
                                               .collect(Collectors.toSet());
            Set<Integer> writeReplicas = writeEps.get().stream()
                                                 .map(r -> metadata.directory.peerId(r.endpoint()).id())
                                                 .collect(Collectors.toSet());

            return new ExpectedLocation(token.toString(),
                                        range.left.toString(),
                                        range.right.toString(),
                                        readEndpoints,
                                        writeEndpoints,
                                        readReplicas,
                                        writeReplicas);
        });
    }
}
