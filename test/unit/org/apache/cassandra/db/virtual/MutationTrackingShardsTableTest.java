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

package org.apache.cassandra.db.virtual;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;

import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.replication.MutationJournal;
import org.apache.cassandra.replication.MutationTrackingService;
import org.apache.cassandra.tcm.ClusterMetadata;

import static org.assertj.core.api.Assertions.assertThat;

public class MutationTrackingShardsTableTest extends CQLTester
{
    private static final String KS_NAME = "vts";

    @BeforeClass
    public static void setUpClass()
    {
        CQLTester.setUpClass();
        MutationTrackingTables.MutationTrackingShardsTable table = new MutationTrackingTables.MutationTrackingShardsTable(KS_NAME);
        VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace(KS_NAME, ImmutableList.of(table)));
    }

    @Before
    public void setUp()
    {
        // Start required services for mutation tracking
        MutationJournal.start();
        MutationTrackingService.start(ClusterMetadata.current());

        // Create a tracked keyspace
        schemaChange("CREATE KEYSPACE IF NOT EXISTS tracked_ks WITH replication = " +
                     "{'class': 'SimpleStrategy', 'replication_factor': 1} AND replication_type='tracked'");

        // Create a table in the tracked keyspace
        schemaChange("CREATE TABLE tracked_ks.tbl(" +
                     "pk int PRIMARY KEY, " +
                     "v int)");
    }

    @After
    public void tearDown() throws InterruptedException
    {
        // Shutdown the service to prevent test hanging
        MutationTrackingService.shutdown();
    }

    @Test
    public void testSelectAll()
    {
        // Write data to trigger shard creation
        for (int i = 0; i < 100; i++)
        {
            execute("INSERT INTO tracked_ks.tbl(pk, v) VALUES (?, ?)", i, i);
        }

        // Query the virtual table
        ResultSet result = executeNet("SELECT * FROM vts.mutation_tracking_shards");

        // Verify the existence of all columns
        assertThat(result.getColumnDefinitions().asList()
                         .stream()
                         .map(ColumnDefinitions.Definition::getName)
                         .collect(Collectors.toSet()))
            .containsAll(Arrays.asList(
                "keyspace",
                "range_start",
                "range_end",
                "log_id",
                "local_node_id",
                "participants",
                "witnessed_offsets",
                "reconciled_offsets",
                "persisted_offsets"
            ));

        boolean foundShards = false;

        for (Row r : result)
        {
            foundShards = true;

            // Extract all columns
            String keyspace = r.getString("keyspace");
            String rangeStart = r.getString("range_start");
            String rangeEnd = r.getString("range_end");
            String logId = r.getString("log_id");
            int localNodeId = r.getInt("local_node_id");
            String participants = r.getString("participants");
            String witnessedOffsets = r.getString("witnessed_offsets");
            String reconciledOffsets = r.getString("reconciled_offsets");
            String persistedOffsets = r.getString("persisted_offsets");

            assertThat(keyspace).isNotNull();

            assertThat(rangeStart).isNotNull();
            assertThat(rangeEnd).isNotNull();

            assertThat(logId).isNotNull();

            assertThat(localNodeId).isGreaterThanOrEqualTo(0);

            assertThat(participants).isNotNull(); // should show replica node IDs

            assertThat(witnessedOffsets).isNotNull();
            assertThat(reconciledOffsets).isNotNull();
            assertThat(persistedOffsets).isNotNull();
        }

        assertThat(foundShards).isTrue();
    }

    @Test
    public void testSelectKeyspace()
    {
        // Write data to trigger shard creation
        for (int i = 0; i < 100; i++)
        {
            execute("INSERT INTO tracked_ks.tbl(pk, v) VALUES (?, ?)", i, i);
        }

        ResultSet empty = executeNet("SELECT * FROM vts.mutation_tracking_shards WHERE \"keyspace\" = 'doesnotexist'");
        Assertions.assertThat(empty.all()).isEmpty();

        ResultSet result = executeNet("SELECT * FROM vts.mutation_tracking_shards WHERE \"keyspace\" = 'tracked_ks'");
        List<Row> rows = result.all();
        Assertions.assertThat(rows).isNotEmpty();
        rows.forEach(row -> Assertions.assertThat(row.getString("keyspace")).isEqualTo("tracked_ks"));
    }
}
