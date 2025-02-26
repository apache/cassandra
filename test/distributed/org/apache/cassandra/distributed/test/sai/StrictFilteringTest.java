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

package org.apache.cassandra.distributed.test.sai;

import java.io.IOException;
import java.util.Iterator;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.test.TestBaseImpl;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;

/**
 * This class contains a small set of hand-crafted tests that document corner cases around how SAI handles
 * resolving partial updates, unrepaired data, and post-index filtering.
 * 
 * @see <a href="https://issues.apache.org/jira/browse/CASSANDRA-19018">CASSANDRA-19018</a>
 */
public class StrictFilteringTest extends TestBaseImpl
{
    private static Cluster CLUSTER;

    @BeforeClass
    public static void setUpCluster() throws IOException
    {
        CLUSTER = init(Cluster.build(2).withConfig(config -> config.set("hinted_handoff_enabled", false).with(GOSSIP).with(NETWORK)).start());
    }

    @Test
    public void shouldDegradeToUnionOnSingleStatic()
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.single_static (pk0 int, ck0 int, ck1 int, s0 int static, v0 int, PRIMARY KEY (pk0, ck0, ck1)) " +
                                          "WITH read_repair = 'NONE' AND CLUSTERING ORDER BY (ck0 ASC, ck1 DESC)"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.single_static(ck0) USING 'sai'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.single_static(s0) USING 'sai'"));
        SAIUtil.waitForIndexQueryable(CLUSTER, KEYSPACE);

        // To present the coordinator with enough data to find a row match, both replicas must degrade to OR at query
        // time. The static column match from node 2 and the clustering key match from node 1 must be merged.
        CLUSTER.get(2).executeInternal(withKeyspace("INSERT INTO %s.single_static (pk0, ck0, ck1, s0, v0) VALUES (0, 1, 2, 3, 4)"));
        CLUSTER.get(1).executeInternal(withKeyspace("UPDATE %s.single_static SET v0 = 5 WHERE pk0 = 0 AND ck0 = 6 AND ck1 = 7"));

        // A static column predicate and ANY other predicate makes strict filtering impossible, as the static match
        // applies to the entire partition.
        String select = withKeyspace("SELECT * FROM %s.single_static WHERE s0 = 3 AND ck0 = 6");
        assertRows(CLUSTER.coordinator(1).execute(select, ConsistencyLevel.ALL), row(0, 6, 7, 3, 5));
    }

    @Test
    public void shouldPostFilterNonStrictIN()
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.reject_in (k int PRIMARY KEY, a int, b int) WITH read_repair = 'NONE'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.reject_in(a) USING 'sai'"));
        SAIUtil.waitForIndexQueryable(CLUSTER, KEYSPACE);

        // insert an unrepaired row
        CLUSTER.get(1).executeInternal(withKeyspace("INSERT INTO %s.reject_in(k, a) VALUES (0, 1)"));
        CLUSTER.get(2).executeInternal(withKeyspace("INSERT INTO %s.reject_in(k, b) VALUES (0, 2)"));

        String select = withKeyspace("SELECT * FROM %s.reject_in WHERE a = 1 AND b IN (2, 3) ALLOW FILTERING");

        assertRows(CLUSTER.coordinator(1).execute(select, ConsistencyLevel.ALL), row(0,1,2));

        // Repair fixes the split row, although we still only allow the query when reconciliation is not required:
        CLUSTER.get(1).nodetoolResult("repair", KEYSPACE).asserts().success();
        assertRows(CLUSTER.coordinator(1).execute(select, ConsistencyLevel.ONE), row(0, 1, 2));
    }

    @Test
    public void testPartialUpdates()
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.partial_updates (k int PRIMARY KEY, a int, b int) WITH read_repair = 'NONE'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.partial_updates(a) USING 'sai'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.partial_updates(b) USING 'sai'"));
        SAIUtil.waitForIndexQueryable(CLUSTER, KEYSPACE);

        // insert a split row
        CLUSTER.get(1).executeInternal(withKeyspace("INSERT INTO %s.partial_updates(k, a) VALUES (0, 1) USING TIMESTAMP 1"));
        CLUSTER.get(2).executeInternal(withKeyspace("INSERT INTO %s.partial_updates(k, b) VALUES (0, 2) USING TIMESTAMP 2"));

        String select = withKeyspace("SELECT * FROM %s.partial_updates WHERE a = 1 AND b = 2");
        Object[][] initialRows = CLUSTER.coordinator(1).execute(select, ConsistencyLevel.ALL);
        assertRows(initialRows, row(0, 1, 2));
    }

    @Test
    public void testPartialUpdatesOnNonIndexedColumnsAfterRepair()
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.partial_updates_non_indexed_columns (k int PRIMARY KEY, a int, b int, c int) WITH read_repair = 'NONE'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.partial_updates_non_indexed_columns(a) USING 'sai'"));
        SAIUtil.waitForIndexQueryable(CLUSTER, KEYSPACE);

        CLUSTER.coordinator(1).execute(withKeyspace("INSERT INTO %s.partial_updates_non_indexed_columns(k, a) VALUES (0, 1) USING TIMESTAMP 1"), ConsistencyLevel.ALL);
        CLUSTER.get(1).nodetoolResult("repair", KEYSPACE).asserts().success();
        
        // insert a split row
        CLUSTER.get(1).executeInternal(withKeyspace("INSERT INTO %s.partial_updates_non_indexed_columns(k, b) VALUES (0, 2) USING TIMESTAMP 2"));
        CLUSTER.get(2).executeInternal(withKeyspace("INSERT INTO %s.partial_updates_non_indexed_columns(k, c) VALUES (0, 3) USING TIMESTAMP 3"));

        String select = withKeyspace("SELECT * FROM %s.partial_updates_non_indexed_columns WHERE a = 1 AND b = 2 AND c = 3 ALLOW FILTERING");
        Object[][] initialRows = CLUSTER.coordinator(1).execute(select, ConsistencyLevel.ALL);
        assertRows(initialRows, row(0, 1, 2, 3));
    }

    @Test
    public void testPartialUpdateOnNonIndexedColumnAfterRepair()
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.partial_updates_non_indexed_column (k int PRIMARY KEY, a int, b int) WITH read_repair = 'NONE'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.partial_updates_non_indexed_column(a) USING 'sai'"));
        SAIUtil.waitForIndexQueryable(CLUSTER, KEYSPACE);

        CLUSTER.coordinator(1).execute(withKeyspace("INSERT INTO %s.partial_updates_non_indexed_column(k, a) VALUES (0, 1) USING TIMESTAMP 1"), ConsistencyLevel.ALL);
        CLUSTER.get(1).nodetoolResult("repair", KEYSPACE).asserts().success();

        // insert a split row
        CLUSTER.get(1).executeInternal(withKeyspace("INSERT INTO %s.partial_updates_non_indexed_column(k, b) VALUES (0, 2) USING TIMESTAMP 2"));

        String select = withKeyspace("SELECT * FROM %s.partial_updates_non_indexed_column WHERE a = 1 AND b = 2 ALLOW FILTERING");
        Object[][] initialRows = CLUSTER.coordinator(1).execute(select, ConsistencyLevel.ALL);
        assertRows(initialRows, row(0, 1, 2));
    }

    @Test
    public void testPartialUpdatesWithDeleteBetween()
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.partial_updates_delete_between (k int, c int, a int, b int, x int, y int, PRIMARY KEY (k, c)) WITH read_repair = 'NONE'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.partial_updates_delete_between(a) USING 'sai'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.partial_updates_delete_between(b) USING 'sai'"));
        SAIUtil.waitForIndexQueryable(CLUSTER, KEYSPACE);

        // insert a split row w/ a range tombstone sandwiched in the middle 
        CLUSTER.get(1).executeInternal(withKeyspace("INSERT INTO %s.partial_updates_delete_between(k, c, a, x) VALUES (0, 1, 1, 100) USING TIMESTAMP 1"));
        CLUSTER.get(2).executeInternal(withKeyspace("DELETE FROM %s.partial_updates_delete_between USING TIMESTAMP 2 WHERE k = 0 AND c > 0"));
        CLUSTER.get(2).executeInternal(withKeyspace("INSERT INTO %s.partial_updates_delete_between(k, c, b, y) VALUES (0, 1, 2, 200) USING TIMESTAMP 3"));

        String select = withKeyspace("SELECT * FROM %s.partial_updates_delete_between WHERE a = 1 AND b = 2");
        Object[][] initialRows = CLUSTER.coordinator(1).execute(select, ConsistencyLevel.ALL);
        assertRows(initialRows);
    }

    @Test
    public void testDanglingUnfilteredColumnWithDeleteBetween()
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.dangling_unfiltered_delete_between (k int, c int, a int, b int, x int, PRIMARY KEY (k, c)) WITH read_repair = 'NONE'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.dangling_unfiltered_delete_between(a) USING 'sai'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.dangling_unfiltered_delete_between(b) USING 'sai'"));
        SAIUtil.waitForIndexQueryable(CLUSTER, KEYSPACE);

        // insert a split row w/ a range tombstone sandwiched in the middle 
        CLUSTER.get(1).executeInternal(withKeyspace("INSERT INTO %s.dangling_unfiltered_delete_between(k, c, a, b, x) VALUES (0, 1, 1, 2, 100) USING TIMESTAMP 1"));
        CLUSTER.get(2).executeInternal(withKeyspace("DELETE FROM %s.dangling_unfiltered_delete_between USING TIMESTAMP 2 WHERE k = 0 AND c > 0"));
        CLUSTER.get(2).executeInternal(withKeyspace("INSERT INTO %s.dangling_unfiltered_delete_between(k, c, a, b) VALUES (0, 1, 1, 2) USING TIMESTAMP 3"));

        String select = withKeyspace("SELECT * FROM %s.dangling_unfiltered_delete_between WHERE a = 1 AND b = 2");
        Object[][] initialRows = CLUSTER.coordinator(1).execute(select, ConsistencyLevel.ALL);
        assertRows(initialRows, row(0, 1, 1, 2, null));
    }

    @Test
    public void testPartialUpdatesStaticOnly()
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.partial_updates_statics (k int, c int, s int static, b int, PRIMARY KEY (k, c)) WITH read_repair = 'NONE'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.partial_updates_statics(s) USING 'sai'"));
        SAIUtil.waitForIndexQueryable(CLUSTER, KEYSPACE);

        // insert a split row
        CLUSTER.get(1).executeInternal(withKeyspace("INSERT INTO %s.partial_updates_statics(k, s) VALUES (0, 2) USING TIMESTAMP 100"));
        CLUSTER.get(2).executeInternal(withKeyspace("INSERT INTO %s.partial_updates_statics(k, c, s, b) VALUES (0, 0, 1, 2) USING TIMESTAMP 10"));

        String select = withKeyspace("SELECT * FROM %s.partial_updates_statics WHERE s = 2");
        Object[][] initialRows = CLUSTER.coordinator(1).execute(select, ConsistencyLevel.ALL);
        assertRows(initialRows, row(0, 0, 2, 2));
    }

    @Test
    public void testShortReadWithRegularColumns()
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.partial_updates_short_read (k int PRIMARY KEY, a int, b int) WITH read_repair = 'NONE'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.partial_updates_short_read(a) USING 'sai'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.partial_updates_short_read(b) USING 'sai'"));
        SAIUtil.waitForIndexQueryable(CLUSTER, KEYSPACE);

        // insert a split row
        CLUSTER.get(1).executeInternal(withKeyspace("INSERT INTO %s.partial_updates_short_read(k, a) VALUES (0, 1) USING TIMESTAMP 1"));
        CLUSTER.get(2).executeInternal(withKeyspace("INSERT INTO %s.partial_updates_short_read(k, b) VALUES (0, 2) USING TIMESTAMP 2"));

        CLUSTER.get(1).executeInternal(withKeyspace("INSERT INTO %s.partial_updates_short_read(k, a, b) VALUES (1, 4, 2) USING TIMESTAMP 3"));
        CLUSTER.get(2).executeInternal(withKeyspace("INSERT INTO %s.partial_updates_short_read(k, a, b) VALUES (1, 1, 4) USING TIMESTAMP 4"));

        String select = withKeyspace("SELECT * FROM %s.partial_updates_short_read WHERE a = 1 AND b = 2 LIMIT 1");
        Iterator<Object[]> initialRows = CLUSTER.coordinator(1).executeWithPaging(select, ConsistencyLevel.ALL, 2);
        assertRows(initialRows, row(0, 1, 2));
    }

    @Test
    public void testShortReadWithStaticColumn()
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.partial_updates_short_read_static (k int, c int, a int, b int static, PRIMARY KEY(k, c)) WITH read_repair = 'NONE'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.partial_updates_short_read_static(a) USING 'sai'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.partial_updates_short_read_static(b) USING 'sai'"));
        SAIUtil.waitForIndexQueryable(CLUSTER, KEYSPACE);

        // insert a split row
        CLUSTER.get(1).executeInternal(withKeyspace("INSERT INTO %s.partial_updates_short_read_static(k, c, a) VALUES (0, 0, 1) USING TIMESTAMP 1"));
        CLUSTER.get(2).executeInternal(withKeyspace("INSERT INTO %s.partial_updates_short_read_static(k, c, b) VALUES (0, 0, 2) USING TIMESTAMP 2"));

        CLUSTER.get(1).executeInternal(withKeyspace("INSERT INTO %s.partial_updates_short_read_static(k, c, a, b) VALUES (1, 1, 4, 2) USING TIMESTAMP 3"));
        CLUSTER.get(2).executeInternal(withKeyspace("INSERT INTO %s.partial_updates_short_read_static(k, c, a, b) VALUES (1, 1, 1, 4) USING TIMESTAMP 4"));

        String select = withKeyspace("SELECT k, a, b FROM %s.partial_updates_short_read_static WHERE a = 1 AND b = 2 LIMIT 1");
        Object[][] rows = CLUSTER.coordinator(1).execute(select, ConsistencyLevel.ALL);
        assertRows(rows, row(0, 1, 2));
    }

    @Test
    public void testTimestampCollision()
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.timestamp_collision (k int PRIMARY KEY, a int, b int) WITH read_repair = 'NONE'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.timestamp_collision(a) USING 'sai'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.timestamp_collision(b) USING 'sai'"));
        SAIUtil.waitForIndexQueryable(CLUSTER, KEYSPACE);

        // insert a split row
        CLUSTER.get(1).executeInternal(withKeyspace("INSERT INTO %s.timestamp_collision(k, a, b) VALUES (0, 1, 2) USING TIMESTAMP 1"));
        CLUSTER.get(2).executeInternal(withKeyspace("INSERT INTO %s.timestamp_collision(k, a, b) VALUES (0, 2, 1) USING TIMESTAMP 1"));
        
        String select = withKeyspace("SELECT * FROM %s.timestamp_collision WHERE a = 2 AND b = 2");
        Object[][] initialRows = CLUSTER.coordinator(1).execute(select, ConsistencyLevel.ALL);
        assertRows(initialRows, row(0, 2, 2));
    }

    @Test
    public void testPartialUpdateOnOneColumn()
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.one_column (k int PRIMARY KEY, a int) WITH read_repair = 'NONE'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s.one_column(a) USING 'sai'"));
        SAIUtil.waitForIndexQueryable(CLUSTER, KEYSPACE);

        CLUSTER.get(1).executeInternal(withKeyspace("INSERT INTO %s.one_column(k, a) VALUES (0, 1) USING TIMESTAMP 1"));
        CLUSTER.get(2).executeInternal(withKeyspace("INSERT INTO %s.one_column(k, a) VALUES (0, 100) USING TIMESTAMP 1"));
        
        // resolved via replica filtering protection
        Object[][] initialRows = CLUSTER.coordinator(1).execute(withKeyspace("SELECT * FROM %s.one_column WHERE a = 1"), ConsistencyLevel.ALL);
        assertRows(initialRows);
    }

    @AfterClass
    public static void shutDownCluster()
    {
        if (CLUSTER != null)
            CLUSTER.close();
    }
}
