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

package org.apache.cassandra.distributed.test.tracking;

import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.distributed.api.ConsistencyLevel;

import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;

/**
 * Tracked range reads, each case scored against the untracked oracle harness in {@link TrackedRangeReadTestBase}.
 */
public class TrackedRangeReadTest extends TrackedRangeReadTestBase
{
    @Test
    public void testTokenRangeOnFullPartitionKeysWithPerPartitionLimitEmpty()
    {
        String keyspace = "token_range_per_partition_limit_empty";
        createTrackedKeyspace(keyspace);
        cluster.schemaChange(withKeyspace("CREATE TYPE IF NOT EXISTS %s.\"6iiPTW_Oe1eyqpNyLtoSbn\" (f0 smallint, f1 uuid)", keyspace));
        cluster.schemaChange(withKeyspace("CREATE TYPE IF NOT EXISTS %s.\"tjQi_gfccLmvemLRbkg\" (f0 uuid)", keyspace));

        cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk0 smallint, pk1 double, ck0 int, s0 text static, s1 map<frozen<map<time, double>>, bigint> static, " +
                                          "v0 frozen<map<timestamp, timeuuid>>, v1 frozen<set<uuid>>, v2 uuid, v3 frozen<tuple<vector<date, 1>, frozen<\"6iiPTW_Oe1eyqpNyLtoSbn\">, " +
                                          "frozen<\"tjQi_gfccLmvemLRbkg\">>>, v4 smallint, PRIMARY KEY ((pk0, pk1), ck0)) WITH CLUSTERING ORDER BY (ck0 ASC) AND read_repair = 'NONE'", keyspace));
        cluster.forEach(i -> i.nodetoolResult("disableautocompaction", keyspace, "tbl").asserts().success());

        cluster.get(2).executeInternal(withKeyspace("DELETE s1 FROM %s.tbl USING TIMESTAMP 1 WHERE pk0 = 4217 AND  pk1 = -2.2644046491088394E265", keyspace));
        cluster.get(2).executeInternal(withKeyspace("INSERT INTO %s.tbl (pk0, pk1, s1) VALUES (-16150, 1.0086497658456055E-263, {{'07:58:45.097000261': -2.1560404491129945E225}: 588520316827010420}) USING TIMESTAMP 2", keyspace));
        cluster.get(3).executeInternal(withKeyspace("INSERT INTO %s.tbl (pk0, pk1, ck0, s0, s1, v0) " +
                                                    "VALUES (4217, -2.2644046491088394E265, -2077196678, '᱔惔겎꣘', null, {'1972-11-15T21:50:31.510Z': 00000000-0000-1100-aa00-000000000000, '1973-10-01T03:02:11.345Z': 00000000-0000-1900-b500-000000000000, '2053-09-18T06:21:05.430Z': 00000000-0000-1900-a100-000000000000}) USING TIMESTAMP 3", keyspace));

        String select = withKeyspace("SELECT * FROM %s.tbl WHERE token(pk0, pk1) >= -9223372036854775808 AND token(pk0, pk1) < -3253266623840194343 PER PARTITION LIMIT 995 LIMIT 950", keyspace);
        cluster.coordinator(1).executeWithPaging(select, ConsistencyLevel.ALL, 5000);

        // both bounds are exclusive and the two partitions written above are exactly the two endpoints, so
        // a per partition limit is applied to a range with nothing in it
        select = withKeyspace("SELECT * FROM %s.tbl WHERE token(pk0, pk1) > token(4217, -2.2644046491088394E265) AND token(pk0, pk1) < token(-16150, 1.0086497658456055E-263) PER PARTITION LIMIT 89 LIMIT 832", keyspace);
        Iterator<Object[]> pagingResult = cluster.coordinator(3).executeWithPaging(select, ConsistencyLevel.ALL, 10);
        assertRows(pagingResult);
    }

    /**
     * A per partition limit can leave the data replica's own result and a reconciliation follow up both carrying rows
     * for one partition, which is the overlap the merge has to reconcile. Whether this case produces that overlap
     * depends on when reconciliation delivers, so TrackedDataResponseTest is what pins the merge and this is an end to
     * end case over it.
     */
    @Test
    public void testTokenRangeOnFullPartitionKeysWithPerPartitionLimitNonEmpty()
    {
        String keyspace = "token_range_per_partition_limit_non_empty";
        createTrackedKeyspace(keyspace);
        cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk0 smallint, pk1 uuid, ck0 'org.apache.cassandra.db.marshal.LexicalUUIDType', ck1 timeuuid, v0 int, PRIMARY KEY ((pk0, pk1), ck0, ck1)) WITH CLUSTERING ORDER BY (ck0 DESC, ck1 DESC) AND read_repair = 'NONE'", keyspace));
        cluster.forEach(i -> i.nodetoolResult("disableautocompaction", keyspace, "tbl").asserts().success());

        cluster.coordinator(1).execute(withKeyspace("SELECT * FROM %s.tbl", keyspace), ConsistencyLevel.ALL);
        cluster.get(2).executeInternal(withKeyspace("INSERT INTO %s.tbl (pk0, pk1, ck0, ck1, v0) VALUES (24199, 00000000-0000-4900-9c00-000000000000, 0x0000000000001800b700000000000000, 00000000-0000-1000-8f00-000000000000, 1) USING TIMESTAMP 1", keyspace));

        cluster.get(3).executeInternal(withKeyspace("DELETE FROM %s.tbl USING TIMESTAMP 2 WHERE pk0 = -16322 AND pk1 = 00000000-0000-4400-ba00-000000000000", keyspace));
        cluster.get(3).executeInternal(withKeyspace("UPDATE %s.tbl USING TIMESTAMP 3 SET v0=2 WHERE  pk0 = 24199 AND pk1 = 00000000-0000-4900-9c00-000000000000 AND  ck0 IN (0x00000000000015008100000000000000) AND ck1 = 00000000-0000-1b00-bd00-000000000000", keyspace));

        cluster.get(1).executeInternal(withKeyspace("INSERT INTO %s.tbl (pk0, pk1, ck0, ck1, v0) VALUES (21485, 00000000-0000-4100-ba00-000000000000, 0x0000000000004c00a900000000000000, 00000000-0000-1200-b700-000000000000, 3) USING TIMESTAMP 4", keyspace));

        String select = withKeyspace("SELECT pk0 FROM %s.tbl WHERE token(pk0, pk1) >= token(24199, 00000000-0000-4900-9c00-000000000000) AND token(pk0, pk1) <= token(21485, 00000000-0000-4100-ba00-000000000000) PER PARTITION LIMIT 139 LIMIT 587", keyspace);
        Iterator<Object[]> pagingResult = cluster.coordinator(2).executeWithPaging(select, ConsistencyLevel.ALL, 100);
        assertRows(pagingResult, row((short) 24199), row((short) 24199), row((short) 21485));
    }

    /** Enough partitions that each of the three primary ranges holds a few dozen of them. */
    private static final int PARTITIONS = 100;

    /**
     * A full table scan from every coordinator, over a hundred partitions written at ALL. Every node is a full
     * replica of the whole ring and so holds every partition, and the answer is the whole table whichever node
     * coordinates it, which makes a short answer rows lost on the read path. It asserts the identity of every
     * partition returned rather than a count, and that no partition comes back twice.
     */
    @Test
    public void testFullTableScanFromEveryCoordinator()
    {
        String keyspace = "full_table_scan_every_coordinator";
        createTrackedKeyspace(keyspace);
        cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int PRIMARY KEY, v int) WITH read_repair = 'NONE'", keyspace));
        cluster.forEach(i -> i.nodetoolResult("disableautocompaction", keyspace, "tbl").asserts().success());

        Map<Integer, Integer> expected = new TreeMap<>();
        for (int pk = 0; pk < PARTITIONS; pk++)
        {
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.tbl (pk, v) VALUES (?, ?)", keyspace), ConsistencyLevel.ALL, pk, pk);
            expected.put(pk, pk);
        }

        assertEveryNodeHoldsEveryPartition(keyspace);

        for (int node = 1; node <= REPLICAS; node++)
        {
            Object[][] rows = cluster.coordinator(node).execute(withKeyspace("SELECT pk, v FROM %s.tbl", keyspace), ConsistencyLevel.ALL);
            Map<Integer, Integer> actual = new TreeMap<>();
            for (Object[] row : rows)
                Assert.assertNull("partition " + row[0] + " returned twice", actual.put((Integer) row[0], (Integer) row[1]));
            Assert.assertEquals("full table scan coordinated on node " + node, expected, actual);
        }
    }

    /**
     * The unstressed case check for {@link #testFullTableScanFromEveryCoordinator}, made with
     * {@code executeInternal} so that it cannot reconcile away the state it is measuring. Every node is a full
     * replica of the whole ring, so every node has to hold every partition: nothing is missing anywhere, and a
     * short coordinated answer is the read path losing rows rather than data that was never written.
     */
    private static void assertEveryNodeHoldsEveryPartition(String keyspace)
    {
        for (int node = 1; node <= REPLICAS; node++)
        {
            int local = nodeLocal(keyspace, node, "SELECT pk FROM %s.tbl").length;
            Assert.assertEquals("node " + node + " does not hold all " + PARTITIONS + " partitions", PARTITIONS, local);
        }
    }
}
