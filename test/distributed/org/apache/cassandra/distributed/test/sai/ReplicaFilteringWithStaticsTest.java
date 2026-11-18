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
import java.util.Set;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.test.TestBaseImpl;

import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;

public class ReplicaFilteringWithStaticsTest extends TestBaseImpl
{
    private static Cluster CLUSTER;

    @BeforeClass
    public static void setUpCluster() throws IOException
    {
        CLUSTER = init(Cluster.build(3).withConfig(config -> config.set("hinted_handoff_enabled", false).with(GOSSIP).with(NETWORK)).start());
    }

    @Test
    public void testRowFilterDeletePurging()
    {
        testRowFilterDeletePurging(false);
    }

    @Test
    public void testRowFilterDeletePurgingSAI()
    {
        testRowFilterDeletePurging(true);
    }

    public void testRowFilterDeletePurging(boolean sai)
    {
        String table = "row_filtering_delete_purging" + (sai ? "_sai" : "");

        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s." + table + " (pk0 double, ck0 boolean, s0 ascii static, v0 ascii, " +
                                          "PRIMARY KEY (pk0, ck0)) WITH CLUSTERING ORDER BY (ck0 DESC) AND read_repair = 'NONE'"));
        disableCompaction(CLUSTER, KEYSPACE, table);

        if (sai)
        {
            CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s." + table + "(s0) USING 'sai'"));
            SAIUtil.waitForIndexQueryable(CLUSTER, KEYSPACE);
        }

        CLUSTER.get(3).executeInternal(withKeyspace("UPDATE %s." + table + " USING TIMESTAMP 1 SET s0='foo', v0='c' WHERE  pk0 = 2.9 AND  ck0 IN (false, true)"));
        
        // This delete must be resolved by RFP to eliminate the row with ck0 = true from node 3:
        CLUSTER.get(1).executeInternal(withKeyspace("DELETE FROM %s." + table + " USING TIMESTAMP 2 WHERE  pk0 = 2.9 AND  ck0 = true"));
        CLUSTER.get(1).executeInternal(withKeyspace("INSERT INTO %s." + table + " (pk0, ck0, s0, v0) VALUES (2.9, false, 'bar', 'xyz') USING TIMESTAMP 3"));

        String select = withKeyspace("SELECT ck0 FROM %s." + table + " WHERE s0 = 'bar' ALLOW FILTERING");
        assertRows(CLUSTER.coordinator(1).executeWithPaging(select, ALL, 100), row(false));
    }

    @Test
    public void testStaticMatchWithPartitionDelete()
    {
        testStaticMatchWithPartitionDelete(false);
    }

    @Test
    public void testStaticMatchWithPartitionDeleteSAI()
    {
        testStaticMatchWithPartitionDelete(true);
    }

    public void testStaticMatchWithPartitionDelete(boolean sai)
    {
        String table = "static_and_delete" + (sai ? "_sai" : "");
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s." + table + " (pk0 boolean, ck0 ascii, s1 tinyint static, v0 boolean, PRIMARY KEY (pk0, ck0)) " +
                                          "WITH CLUSTERING ORDER BY (ck0 ASC) AND read_repair = 'NONE'"));

        if (sai)
        {
            CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s." + table + "(s1) USING 'sai'"));
            SAIUtil.waitForIndexQueryable(CLUSTER, KEYSPACE);
        }

        CLUSTER.get(3).executeInternal(withKeyspace("UPDATE %s." + table + " USING TIMESTAMP 1 SET v0 = false WHERE pk0 = true AND ck0 = 'D'"));
        CLUSTER.get(1).executeInternal(withKeyspace("DELETE FROM %s." + table + " USING TIMESTAMP 2 WHERE pk0 = true"));

        CLUSTER.get(1).executeInternal(withKeyspace("INSERT INTO %s." + table + " (pk0, ck0, s1, v0) VALUES (true, 'G', -114, true) USING TIMESTAMP 3"));
        CLUSTER.get(3).executeInternal(withKeyspace("INSERT INTO %s." + table + " (pk0, ck0) VALUES (true, 'F') USING TIMESTAMP 4"));
        CLUSTER.get(1).executeInternal(withKeyspace("INSERT INTO %s." + table + " (pk0, ck0, s1, v0) VALUES (true, 'C', 17, true) USING TIMESTAMP 5"));

        // This update to the static column creates matches across all previously written live rows in the partition.
        // When RFP sees the unresolved static row, it must read enough data from the silent replicas at nodes 1 and 3
        // to find all potential matches. With a page size of 1, reading only 1 row from node 3 will return the row at
        // ck = 'D', as the partition delete never made it to node 3. This means we'll ignore the live result node 3 has
        // at ck = 'F', because node 1 will produce a result at ck = 'G', and that determines the next paging cursor.
        CLUSTER.get(2).executeInternal(withKeyspace("UPDATE %s." + table + " USING TIMESTAMP 6 SET s1 = 1, v0 = false WHERE pk0 = true AND ck0 = 'A'"));

        String select = withKeyspace("SELECT ck0 FROM %s." + table + " WHERE s1 = 1" + (sai ? "" : " ALLOW FILTERING" ));
        assertRows(CLUSTER.coordinator(1).executeWithPaging(select, ALL, 1), row("A"), row("C"), row("F"), row("G"));
    }

    @Test
    public void testMissingStaticRowWithNonStaticExpression()
    {
        testMissingStaticRowWithNonStaticExpression(false);
    }

    @Test
    public void testMissingStaticRowWithNonStaticExpressionSAI()
    {
        testMissingStaticRowWithNonStaticExpression(true);
    }
    
    public void testMissingStaticRowWithNonStaticExpression(boolean sai)
    {
        String table = "single_predicate" + (sai ? "_sai" : "");
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s." + table + " (pk0 int, ck0 int, ck1 int, s0 int static, s1 int static, v0 int, PRIMARY KEY (pk0, ck0, ck1)) " +
                                          "WITH CLUSTERING ORDER BY (ck0 ASC, ck1 DESC) AND read_repair = 'NONE'"));
        
        if (sai)
        {
            CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s." + table + "(ck1) USING 'sai'"));
            SAIUtil.waitForIndexQueryable(CLUSTER, KEYSPACE);
        }

        CLUSTER.get(1).executeInternal(withKeyspace("INSERT INTO %s." + table + " (pk0, ck0, ck1, s0, s1, v0) " +
                                                    "VALUES (0, 1, 2, 3, 4, 5) USING TIMESTAMP 1"));
        CLUSTER.get(2).executeInternal(withKeyspace("UPDATE %s." + table + "  USING TIMESTAMP 2 SET s0 = 6, s1 = 7, v0 = 8 " +
                                                    "WHERE  pk0 = 0 AND ck0 = 9 AND ck1 = 10"));

        // Node 2 will not produce a match for the static row. Make sure that replica filtering protection does not
        // fetch the entire partition, which could let non-matching rows slip through combined with the fact that we 
        // don't post-filter at the coordinator with no regular column predicates in the query.
        String select = withKeyspace("SELECT pk0, ck0, ck1, s0, s1 FROM %s." + table + " WHERE ck1 = 2" + (sai ? "" : " ALLOW FILTERING"));
        assertRows(CLUSTER.coordinator(1).execute(select, ConsistencyLevel.ALL), row(0, 1, 2, 6, 7));
    }

    @Test
    public void testRangeTombstoneWithStaticSAI()
    {
        testRangeTombstoneWithStatic(true);
    }

    @Test
    public void testRangeTombstoneWithStatic()
    {
        testRangeTombstoneWithStatic(false);
    }

    private void testRangeTombstoneWithStatic(boolean sai)
    {
        String table = "range_tombstone_with_static" + (sai ? "_sai" : "");
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s." + table + " (pk0 int, ck0 boolean, ck1 double, s1 int static, v0 boolean," + " PRIMARY KEY (pk0, ck0, ck1)) WITH read_repair = 'NONE'"));
        disableCompaction(CLUSTER, KEYSPACE, table);

        if (sai)
        {
            CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s." + table + "(s1) USING 'sai'"));
            SAIUtil.waitForIndexQueryable(CLUSTER, KEYSPACE);
        }

        // Node 3 gets a row at ck0=false with an old s1 value. This will be locally live but globally dead once the range tombstone on node 2 is considered.
        CLUSTER.get(3).executeInternal(withKeyspace("INSERT INTO %s." + table + " (pk0, ck0, ck1, s1, v0) VALUES (1, false, 1.0, 99, false) USING TIMESTAMP 1"));

        // Node 2 gets a range tombstone covering all rows (ck0 is boolean, <= true covers everything). Nodes 1 and 3 never receive this.
        CLUSTER.get(2).executeInternal(withKeyspace("DELETE FROM %s." + table + " USING TIMESTAMP 2 WHERE pk0 = 1 AND ck0 <= true"));

        // Node 2 also gets a new surviving row at ck0=true with a new s1 value. This is the only row that should be visible after reconciliation.
        CLUSTER.get(2).executeInternal(withKeyspace("INSERT INTO %s." + table + " (pk0, ck0, ck1, s1, v0) VALUES (1, true, 5.0, 42, true) USING TIMESTAMP 3"));

        // The query on s1=42 should return only the single surviving row, and the first pass input to RFP get exactly that.
        // However, since there is an unresolved static, RFP completion reads for nodes 1 and 3 must read the entire partition.
        // This returns the rows written before the deletion, which never arrived there. If the range tombstone from
        // node 2 is not included in the first pass query, it will not be available to shadow the logically deleted rows.
        String select = withKeyspace("SELECT ck0, ck1 FROM %s." + table + " WHERE s1 = 42" + (sai ? "" : " ALLOW FILTERING"));
        assertRows(CLUSTER.coordinator(1).executeWithPaging(select, ALL, 1), row(true, 5.0));
    }

    @Test
    public void testRangeTombstoneFromOtherReplica()
    {
        String table = "index_query_range_tombstone";
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s." + table + " (pk0 int, pk1 int, ck0 boolean, v1 set<int>, PRIMARY KEY ((pk0, pk1), ck0)) WITH read_repair = 'NONE'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s." + table + "(pk1) USING 'sai'"));
        SAIUtil.waitForIndexQueryable(CLUSTER, KEYSPACE);
        disableCompaction(CLUSTER, KEYSPACE, table);

        // node 2 has the stale element, node 1 has the range tombstone and the new row
        CLUSTER.get(2).executeInternal(withKeyspace("INSERT INTO %s." + table + " (pk0, pk1, ck0, v1) VALUES (1, 2, false, {1}) USING TIMESTAMP 1"));
        CLUSTER.get(1).executeInternal(withKeyspace("DELETE FROM %s." + table + " USING TIMESTAMP 2 WHERE pk0 = 1 AND pk1 = 2 AND ck0 >= false AND ck0 <= false"));
        CLUSTER.get(1).executeInternal(withKeyspace("UPDATE %s." + table + " USING TIMESTAMP 3 SET v1 = v1 + {2, 3, 4} WHERE pk0 = 1 AND pk1 = 2 AND ck0 = false"));

        CLUSTER.get(1).flush(KEYSPACE);
        CLUSTER.get(2).flush(KEYSPACE);

        String select = withKeyspace("SELECT ck0, v1 FROM %s." + table + " WHERE pk1 = 2 ALLOW FILTERING");
        assertRows(CLUSTER.coordinator(1).execute(select, ALL), row(false, Set.of(2, 3, 4)));
    }

    @Test
    public void testRangeTombstoneCoveringInsertInSeparateSSTables()
    {
        testRangeTombstoneCoveringInsert("index_query_rt_insert_sstables", true);
    }

    @Test
    public void testRangeTombstoneCoveringInsertInMemtable()
    {
        testRangeTombstoneCoveringInsert("index_query_rt_insert_memtable", false);
    }

    private void testRangeTombstoneCoveringInsert(String table, boolean flushInsert)
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s." + table + " (pk0 int, pk1 int, ck0 boolean, v1 set<int>, PRIMARY KEY ((pk0, pk1), ck0)) WITH read_repair = 'NONE'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s." + table + "(pk1) USING 'sai'"));
        SAIUtil.waitForIndexQueryable(CLUSTER, KEYSPACE);
        disableCompaction(CLUSTER, KEYSPACE, table);

        CLUSTER.get(2).executeInternal(withKeyspace("INSERT INTO %s." + table + " (pk0, pk1, ck0, v1) VALUES (1, 2, false, {1}) USING TIMESTAMP 1"));
        CLUSTER.get(2).flush(KEYSPACE);
        CLUSTER.get(1).executeInternal(withKeyspace("DELETE FROM %s." + table + " USING TIMESTAMP 2 WHERE pk0 = 1 AND pk1 = 2 AND ck0 >= false AND ck0 <= false"));
        CLUSTER.get(1).flush(KEYSPACE);
        CLUSTER.get(1).executeInternal(withKeyspace("INSERT INTO %s." + table + " (pk0, pk1, ck0, v1) VALUES (1, 2, false, {2, 3, 4}) USING TIMESTAMP 3"));
        if (flushInsert)
            CLUSTER.get(1).flush(KEYSPACE);

        String control = withKeyspace("SELECT ck0, v1 FROM %s." + table + " WHERE pk0 = 1 AND pk1 = 2");
        assertRows(CLUSTER.coordinator(1).execute(control, ALL), row(false, Set.of(2, 3, 4)));

        String select = withKeyspace("SELECT ck0, v1 FROM %s." + table + " WHERE pk1 = 2 ALLOW FILTERING");
        assertRows(CLUSTER.coordinator(1).execute(select, ALL), row(false, Set.of(2, 3, 4)));
    }

    @Test
    public void testRangeTombstoneWithinSinglePartition()
    {
        String table = "index_query_single_partition_range_tombstone";
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s." + table + " (pk0 int, pk1 int, ck0 boolean, v1 set<int>, x int, PRIMARY KEY ((pk0, pk1), ck0)) WITH read_repair = 'NONE'"));
        // x, not pk1, is indexed here, so a fully specified partition key can still use the index for
        // row filtering within that one partition, without turning the read into a multi-partition scan.
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s." + table + "(x) USING 'sai'"));
        SAIUtil.waitForIndexQueryable(CLUSTER, KEYSPACE);
        disableCompaction(CLUSTER, KEYSPACE, table);

        // node 2 has the stale element, node 1 has the range tombstone and the new row
        CLUSTER.get(2).executeInternal(withKeyspace("INSERT INTO %s." + table + " (pk0, pk1, ck0, v1, x) VALUES (1, 2, false, {1}, 5) USING TIMESTAMP 1"));
        CLUSTER.get(1).executeInternal(withKeyspace("DELETE FROM %s." + table + " USING TIMESTAMP 2 WHERE pk0 = 1 AND pk1 = 2 AND ck0 >= false AND ck0 <= false"));
        CLUSTER.get(1).executeInternal(withKeyspace("UPDATE %s." + table + " USING TIMESTAMP 3 SET v1 = v1 + {2, 3, 4}, x = 5 WHERE pk0 = 1 AND pk1 = 2 AND ck0 = false"));

        CLUSTER.get(1).flush(KEYSPACE);
        CLUSTER.get(2).flush(KEYSPACE);

        // Full partition key, explicit LIMIT, no static expression, so RFP's completion fetch is a targeted
        // re-fetch rather than a partition dump. But RFP only reconciles filtered columns, and v1 is only
        // selected here, so the ordinary cross-replica cell merge must still see node 1's range tombstone.
        String select = withKeyspace("SELECT ck0, v1 FROM %s." + table + " WHERE pk0 = 1 AND pk1 = 2 AND x = 5 LIMIT 10");
        assertRows(CLUSTER.coordinator(1).execute(select, ALL), row(false, Set.of(2, 3, 4)));
    }

    @Test
    public void testRangeTombstoneWithClusteringRangeAndNonKeyColumn()
    {
        String table = "index_query_clustering_range_non_key";
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s." + table + " (pk0 int, pk1 int, ck0 int, v1 set<int>, x int, PRIMARY KEY ((pk0, pk1), ck0)) WITH read_repair = 'NONE'"));
        // x, not pk1, is indexed here, so the fully specified partition key plus clustering range can
        // still use the index for row filtering within that one partition and clustering slice.
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s." + table + "(x) USING 'sai'"));
        SAIUtil.waitForIndexQueryable(CLUSTER, KEYSPACE);
        disableCompaction(CLUSTER, KEYSPACE, table);

        // node 2 has the stale element, node 1 has the range tombstone and the new row
        CLUSTER.get(2).executeInternal(withKeyspace("INSERT INTO %s." + table + " (pk0, pk1, ck0, v1, x) VALUES (1, 2, 3, {1}, 5) USING TIMESTAMP 1"));
        CLUSTER.get(1).executeInternal(withKeyspace("DELETE FROM %s." + table + " USING TIMESTAMP 2 WHERE pk0 = 1 AND pk1 = 2 AND ck0 >= 3 AND ck0 <= 3"));
        CLUSTER.get(1).executeInternal(withKeyspace("UPDATE %s." + table + " USING TIMESTAMP 3 SET v1 = v1 + {2, 3, 4}, x = 5 WHERE pk0 = 1 AND pk1 = 2 AND ck0 = 3"));

        CLUSTER.get(1).flush(KEYSPACE);
        CLUSTER.get(2).flush(KEYSPACE);

        // Same as above but with a clustering range instead of a point, plus a non-key column restriction.
        // Still needs node 1's range tombstone for the ordinary cross-replica cell merge.
        String select = withKeyspace("SELECT ck0, v1 FROM %s." + table + " WHERE pk0 = 1 AND pk1 = 2 AND ck0 >= 0 AND ck0 <= 10 AND x = 5 LIMIT 10");
        assertRows(CLUSTER.coordinator(1).execute(select, ALL), row(3, Set.of(2, 3, 4)));
    }

    @Test
    public void testAdjoiningRangeTombstonesAcrossABoundary()
    {
        String table = "index_query_boundary_markers";
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s." + table + " (pk0 int, pk1 int, ck0 int, v1 set<int>, x int, PRIMARY KEY ((pk0, pk1), ck0)) WITH read_repair = 'NONE'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s." + table + "(x) USING 'sai'"));
        SAIUtil.waitForIndexQueryable(CLUSTER, KEYSPACE);
        disableCompaction(CLUSTER, KEYSPACE, table);

        // node 2 has a stale element in each of two adjoining ranges deleted on node 1
        CLUSTER.get(2).executeInternal(withKeyspace("INSERT INTO %s." + table + " (pk0, pk1, ck0, v1, x) VALUES (1, 2, 2, {1}, 5) USING TIMESTAMP 1"));
        CLUSTER.get(2).executeInternal(withKeyspace("INSERT INTO %s." + table + " (pk0, pk1, ck0, v1, x) VALUES (1, 2, 4, {9}, 5) USING TIMESTAMP 1"));

        // These ranges touch exactly at ck0 = 3, so node 1 merges them into one boundary marker
        // instead of two independent open/close pairs.
        CLUSTER.get(1).executeInternal(withKeyspace("DELETE FROM %s." + table + " USING TIMESTAMP 2 WHERE pk0 = 1 AND pk1 = 2 AND ck0 >= 1 AND ck0 < 3"));
        CLUSTER.get(1).executeInternal(withKeyspace("DELETE FROM %s." + table + " USING TIMESTAMP 3 WHERE pk0 = 1 AND pk1 = 2 AND ck0 >= 3 AND ck0 < 6"));

        CLUSTER.get(1).executeInternal(withKeyspace("UPDATE %s." + table + " USING TIMESTAMP 4 SET v1 = v1 + {2, 3, 4}, x = 5 WHERE pk0 = 1 AND pk1 = 2 AND ck0 = 2"));
        CLUSTER.get(1).executeInternal(withKeyspace("UPDATE %s." + table + " USING TIMESTAMP 5 SET v1 = v1 + {5, 6, 7}, x = 5 WHERE pk0 = 1 AND pk1 = 2 AND ck0 = 4"));

        CLUSTER.get(1).flush(KEYSPACE);
        CLUSTER.get(2).flush(KEYSPACE);

        // control: no index involved
        String control = withKeyspace("SELECT ck0, v1 FROM %s." + table + " WHERE pk0 = 1 AND pk1 = 2");
        assertRows(CLUSTER.coordinator(1).execute(control, ALL), row(2, Set.of(2, 3, 4)), row(4, Set.of(5, 6, 7)));

        // If the boundary's reopened deletion were dropped, node 2's stale {9} element would survive reconciliation.
        String select = withKeyspace("SELECT ck0, v1 FROM %s." + table + " WHERE pk0 = 1 AND pk1 = 2 AND x = 5 LIMIT 10");
        assertRows(CLUSTER.coordinator(1).execute(select, ALL), row(2, Set.of(2, 3, 4)), row(4, Set.of(5, 6, 7)));
    }

    @Test
    public void testOverlappingRangeTombstonesAcrossABoundary()
    {
        String table = "index_query_overlapping_boundary_markers";
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s." + table + " (pk0 int, pk1 int, ck0 int, v1 set<int>, x int, PRIMARY KEY ((pk0, pk1), ck0)) WITH read_repair = 'NONE'"));
        CLUSTER.schemaChange(withKeyspace("CREATE INDEX ON %s." + table + "(x) USING 'sai'"));
        SAIUtil.waitForIndexQueryable(CLUSTER, KEYSPACE);
        disableCompaction(CLUSTER, KEYSPACE, table);

        // node 2 has a stale element inside the overlap of two ranges deleted on node 1
        CLUSTER.get(2).executeInternal(withKeyspace("INSERT INTO %s." + table + " (pk0, pk1, ck0, v1, x) VALUES (1, 2, 4, {1}, 5) USING TIMESTAMP 2"));

        // These ranges overlap at ck0 in [3, 5), so node 1 merges them into a boundary where the higher
        // timestamp wins across the overlap, not just whichever range happens to open first.
        CLUSTER.get(1).executeInternal(withKeyspace("DELETE FROM %s." + table + " USING TIMESTAMP 1 WHERE pk0 = 1 AND pk1 = 2 AND ck0 >= 1 AND ck0 < 5"));
        CLUSTER.get(1).executeInternal(withKeyspace("DELETE FROM %s." + table + " USING TIMESTAMP 3 WHERE pk0 = 1 AND pk1 = 2 AND ck0 >= 3 AND ck0 < 8"));

        CLUSTER.get(1).executeInternal(withKeyspace("UPDATE %s." + table + " USING TIMESTAMP 4 SET v1 = v1 + {2, 3, 4}, x = 5 WHERE pk0 = 1 AND pk1 = 2 AND ck0 = 4"));

        CLUSTER.get(1).flush(KEYSPACE);
        CLUSTER.get(2).flush(KEYSPACE);

        // Node 2's stale element is timestamped 2: older than the overlap's winning deletion (3), but newer
        // than the first range's own deletion (1). If the overlap incorrectly resolved to timestamp 1, {1}
        // would survive reconciliation instead of being correctly shadowed by the winning timestamp 3.
        String select = withKeyspace("SELECT ck0, v1 FROM %s." + table + " WHERE pk0 = 1 AND pk1 = 2 AND x = 5 LIMIT 10");
        assertRows(CLUSTER.coordinator(1).execute(select, ALL), row(4, Set.of(2, 3, 4)));
    }

    @AfterClass
    public static void shutDownCluster()
    {
        if (CLUSTER != null)
            CLUSTER.close();
    }
}
