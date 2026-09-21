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

import java.util.Arrays;
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
    public void testPartialPartitionFilterWithPerPartitionLimit()
    {
        String keyspace = "partial_partition_filter_per_partition_limit";
        createTrackedKeyspace(keyspace);

        cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk0 bigint, pk1 text, ck0 bigint, s0 frozen<list<frozen<list<time>>>> static, " +
                                          "v0 'org.apache.cassandra.db.marshal.LexicalUUIDType', PRIMARY KEY ((pk0, pk1), ck0)) WITH CLUSTERING ORDER BY (ck0 DESC) AND read_repair = 'NONE'", keyspace));
        cluster.forEach(i -> i.nodetoolResult("disableautocompaction", keyspace, "tbl").asserts().success());

        cluster.get(1).executeInternal(withKeyspace("UPDATE %s.tbl USING TIMESTAMP 2 SET s0=[['03:28:16.047802044']] WHERE  pk0 = 7137864754153440313 AND  pk1 = '뢸镝蔥'", keyspace));
        cluster.get(2).executeInternal(withKeyspace("INSERT INTO %s.tbl (pk0, pk1, ck0, v0) VALUES (7137864754153440313, '뢸镝蔥', 7732824726196172505, 0x0000000000004d00af00000000000000) USING TIMESTAMP 3", keyspace));

        cluster.get(2).executeInternal(withKeyspace("UPDATE %s.tbl USING TIMESTAMP 5 " +
                                                    "SET s0=[['01:28:35.208066780', '05:25:43.184564123'], ['16:14:58.464860367', '13:59:53.463983006', '10:32:10.674489767']] " +
                                                    "WHERE  pk0 = 1699976006349660742 AND  pk1 = 'ጬ葲'", keyspace));

        cluster.get(3).executeInternal(withKeyspace("INSERT INTO %s.tbl (pk0, pk1, s0) VALUES (7137864754153440313, '뢸镝蔥', [['11:13:31.615781929', '02:03:35.298191424', '21:32:35.861361643']]) USING TIMESTAMP 6", keyspace));

        String select = withKeyspace("SELECT * FROM %s.tbl WHERE token(pk0, pk1) BETWEEN token(1699976006349660742, 'ጬ葲') AND token(7137864754153440313, '뢸镝蔥') PER PARTITION LIMIT 297 LIMIT 954", keyspace);
        cluster.coordinator(1).execute(select, ConsistencyLevel.ALL);

        select = withKeyspace("SELECT pk0, pk1, ck0 FROM %s.tbl WHERE pk0 = 7137864754153440313 PER PARTITION LIMIT 21 LIMIT 914 ALLOW FILTERING", keyspace);
        Iterator<Object[]> pagingResult = cluster.coordinator(3).executeWithPaging(select, ConsistencyLevel.ALL, 1);

        assertRows(pagingResult, row(7137864754153440313L, "뢸镝蔥", 7732824726196172505L));
    }

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

    @Test
    public void testTextRangeFilterWithHighLimit()
    {
        String keyspace = "text_range_filter_with_high_limit";
        createTrackedKeyspace(keyspace);
        cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk0 bigint, pk1 smallint, ck0 inet, ck1 double, v3 text, PRIMARY KEY ((pk0, pk1), ck0, ck1)) WITH CLUSTERING ORDER BY (ck0 DESC, ck1 ASC) AND read_repair = 'NONE'", keyspace));
        cluster.forEach(i -> i.nodetoolResult("disableautocompaction", keyspace, "tbl").asserts().success());

        cluster.get(2).executeInternal(withKeyspace("DELETE FROM %s.tbl USING TIMESTAMP 1 WHERE pk0 = -3279716623783136579 AND  pk1 = -25927", keyspace));
        cluster.get(1).executeInternal(withKeyspace("INSERT INTO %s.tbl (pk0, pk1, ck0, ck1, v3) VALUES (3754566280912306098, -28139, '9c05:10e3:8a10:dd12:b357:6f0b:736b:c3d', 6.248336852153311E-201 * -1.711074442164963E-123, '⩭爭ᣪ흟赃') USING TIMESTAMP 3", keyspace));

        cluster.get(2).executeInternal(withKeyspace("INSERT INTO %s.tbl (pk0, pk1, ck0, ck1, v3) " +
                                                    "VALUES (-3279716623783136579, -25927, '9191:f315:92eb:f9b8:ebbe:6456:10f4:ca6c', -1.8918823041672677E168 - -3.900839250480109E-214, '吮植' + '䛆') USING TIMESTAMP 4", keyspace));

        cluster.get(2).executeInternal(withKeyspace("INSERT INTO %s.tbl (pk0, pk1, ck0, ck1, v3) VALUES (5882007412747503201, 3756, '4a4b:7deb:98f4:a0ab:f5d0:43f:ab2b:2628', 6.334562923798137E276 * -4.6068109424772055E-29, '㺍ັୁ' + '䝱\u000E݂ụ') USING TIMESTAMP 6", keyspace));

        String select = withKeyspace("SELECT * FROM %s.tbl WHERE pk0 > 5882007412747503201 LIMIT 764 ALLOW FILTERING", keyspace);
        cluster.coordinator(1).executeWithPaging(select, ConsistencyLevel.ALL, 1);

        select = withKeyspace("SELECT pk0, pk1 FROM %s.tbl WHERE v3 > '브ﭶ熒讘ꯄ謏??䎸锭商Ử豫羀펛葕䝆㛔' LIMIT 785 ALLOW FILTERING", keyspace);
        Iterator<Object[]> pagingResult = cluster.coordinator(2).executeWithPaging(select, ConsistencyLevel.ALL, 1);
        assertRows(pagingResult, row(3754566280912306098L, (short) -28139));
    }

    /**
     * The read still has range left when it chases a flagged key, so FilteredFollowupRead starts a range read of its
     * own and its callback asks nextBounds where that read stopped - which needs the partial read startLocal delivers
     * to its consumer.
     */
    @Test
    public void testRangeFilterOnFrozenSetNoLimit()
    {
        String keyspace = "range_filter_on_frozen_set_no_limit";
        createTrackedKeyspace(keyspace);

        cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk0 int, pk1 boolean, ck0 inet, v1 int, v4 frozen<set<bigint>>, PRIMARY KEY ((pk0, pk1), ck0)) WITH CLUSTERING ORDER BY (ck0 DESC) AND read_repair = 'NONE'", keyspace));
        cluster.forEach(i -> i.nodetoolResult("disableautocompaction", keyspace, "tbl").asserts().success());

        cluster.get(1).executeInternal(withKeyspace("UPDATE %s.tbl USING TIMESTAMP 3 SET v4={-4237118076428244729, -1815831816430314156} " +
                                                    "WHERE pk0 = -1256431887 AND pk1 = true AND ck0 IN ('c50:5c4d:35cb:1739:f958:8f83:5d95:963d', '7bf6:c19e:d3f2:8679:b3b3:377f:1ac8:1416', 'd035:5ffc:960c:1b8c:f4ed:a2cf:73f6:af9c')", keyspace));
        cluster.get(1).executeInternal(withKeyspace("INSERT INTO %s.tbl (pk0, pk1, ck0, v4) VALUES (-639885536, false, '238.234.202.249', {8383242616920701144}) USING TIMESTAMP 4", keyspace));

        String select = withKeyspace("SELECT * FROM %s.tbl WHERE v1 = 3 ALLOW FILTERING", keyspace);
        cluster.coordinator(3).executeWithPaging(select, ConsistencyLevel.ALL, 100);

        select = withKeyspace("SELECT * FROM %s.tbl WHERE v1 <= 3 LIMIT 175 ALLOW FILTERING", keyspace);
        cluster.coordinator(3).executeWithPaging(select, ConsistencyLevel.ALL, 1);

        cluster.get(3).executeInternal(withKeyspace("UPDATE %s.tbl USING TIMESTAMP 7 SET v4={7721973864222015806} WHERE  pk0 = -1256431887 AND  pk1 = true AND  ck0 = 'b318:85d4:d6a0:907:ff1e:9262:9635:ccfa'", keyspace));
        cluster.get(2).executeInternal(withKeyspace("DELETE FROM %s.tbl USING TIMESTAMP 8 WHERE  pk0 = -639885536 AND  pk1 = false", keyspace));

        select = withKeyspace("SELECT pk0, pk1 FROM %s.tbl WHERE v4 > {-4237118076428244729, -1815831816430314156} ALLOW FILTERING", keyspace);
        Iterator<Object[]> pagingResult = cluster.coordinator(2).executeWithPaging(select, ConsistencyLevel.ALL, 5000);
        assertRows(pagingResult, row(-1256431887, true));
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

    private static final String TABLE =
        "CREATE TABLE %s.tbl (pk0 int, pk1 text, ck int, v int, PRIMARY KEY ((pk0, pk1), ck)) WITH read_repair = 'NONE'";

    private static final String TABLE_WITH_STATIC =
        "CREATE TABLE %s.tbl (pk0 int, pk1 text, ck int, s int static, v int, PRIMARY KEY ((pk0, pk1), ck)) WITH read_repair = 'NONE'";

    private static final String TABLE_WITH_FROZEN_SET =
        "CREATE TABLE %s.tbl (pk0 int, pk1 text, ck int, fs frozen<set<int>>, PRIMARY KEY ((pk0, pk1), ck)) WITH read_repair = 'NONE'";

    private static final String FILTER = "SELECT pk0, pk1, ck, v FROM %s.tbl WHERE v > 100 ALLOW FILTERING";

    /**
     * Node 1 holds (1,'a') with the value the filter rejects and node 2 the newer value it accepts; node 3 holds
     * neither. Node 1 coordinates, so it is the data replica for its own stale view, and reconciliation has to
     * deliver a mutation for a key that replica has already materialized and thrown away.
     */
    private static final String[] SOLE_PARTITION_STALE_ON_NODE_1 =
    {
        "1:INSERT INTO %s.tbl (pk0, pk1, ck, v) VALUES (1, 'a', 1, 1) USING TIMESTAMP 10",
        "2:UPDATE %s.tbl USING TIMESTAMP 20 SET v = 500 WHERE pk0 = 1 AND pk1 = 'a' AND ck = 1"
    };

    /**
     * As {@link #SOLE_PARTITION_STALE_ON_NODE_1}, plus a partition (2,'b') that node 1 already holds a matching
     * row for, so the data replica's materialized data is not empty and the reconciled result set is two rows.
     */
    private static final String[] TWO_PARTITIONS_ONE_STALE_ON_NODE_1 =
    {
        "1:INSERT INTO %s.tbl (pk0, pk1, ck, v) VALUES (1, 'a', 1, 1) USING TIMESTAMP 10",
        "1:INSERT INTO %s.tbl (pk0, pk1, ck, v) VALUES (2, 'b', 1, 900) USING TIMESTAMP 11",
        "2:UPDATE %s.tbl USING TIMESTAMP 20 SET v = 500 WHERE pk0 = 1 AND pk1 = 'a' AND ck = 1"
    };

    /**
     * Three partitions written through the coordinator at ALL, so every replica holds identical data and
     * reconciliation has nothing to do, plus a partition level tombstone on (2,'b') older than the row that
     * partition contains. With the default Murmur3 partitioner a range scan visits (3,'c'), then (2,'b'), then
     * (1,'a'), so the tombstoned partition is reached before the only one satisfying {@code v > 100}.
     */
    private static final String[] TOMBSTONED_PARTITION_BEFORE_THE_MATCH =
    {
        "*:DELETE FROM %s.tbl USING TIMESTAMP 5 WHERE pk0 = 2 AND pk1 = 'b'",
        "*:INSERT INTO %s.tbl (pk0, pk1, ck, v) VALUES (1, 'a', 1, 500) USING TIMESTAMP 10",
        "*:INSERT INTO %s.tbl (pk0, pk1, ck, v) VALUES (2, 'b', 1, 1) USING TIMESTAMP 11",
        "*:INSERT INTO %s.tbl (pk0, pk1, ck, v) VALUES (3, 'c', 1, 2) USING TIMESTAMP 12"
    };

    /** {@link #TOMBSTONED_PARTITION_BEFORE_THE_MATCH} with the tombstone left out and nothing else changed. */
    private static final String[] SAME_PARTITIONS_WITHOUT_THE_TOMBSTONE =
        Arrays.copyOfRange(TOMBSTONED_PARTITION_BEFORE_THE_MATCH, 1, TOMBSTONED_PARTITION_BEFORE_THE_MATCH.length);

    /**
     * A row filtered range read where the data replica filters out every partition it can see locally, and
     * reconciliation then hands it a mutation for one of those filtered partitions. The mutation does not satisfy
     * the row filter either, so no follow up read is needed, but the read was still augmented and therefore still
     * takes the extending path.
     * <p>
     * The oracle's answer is empty, which the data replica also answers on its own, so the unstressed case check
     * cannot tell this case apart from a vacuous one and the probe asserts the divergence directly instead. That
     * makes it the one probe that names the rows a node holds, so it is also the one that has to ask where a row
     * is allowed to be: node 2 is a witness of (1,'a') under {@link Mode#WITNESSES}, and journals the newer value
     * without applying it, so its copy of the row is present in one mode and absent in the other while the
     * divergence the case needs - node 1 stale, the newer value only reachable by reconciling - is identical.
     */
    @Test
    public void testFilteredRangeReadWhereEveryLocalPartitionIsFilteredOut()
    {
        String everything = "SELECT pk0, pk1, ck, v FROM %s.tbl";
        String[] writes =
        {
            "1:INSERT INTO %s.tbl (pk0, pk1, ck, v) VALUES (1, 'a', 1, 1) USING TIMESTAMP 10",
            // reconciliation delivers this to node 1, which augments the read even though the update is filtered out too
            "2:UPDATE %s.tbl USING TIMESTAMP 20 SET v = 2 WHERE pk0 = 1 AND pk1 = 'a' AND ck = 1"
        };

        String tracked = assertTrackedMatchesOracle("b_all_locals_filtered", TABLE, writes, FILTER, UNPAGED, (keyspace, oracle) -> {
            // the defect is in the branch taken when the filter empties a non empty local map, so the data replica
            // has to have materialized the partition in the first place
            Assert.assertTrue("(1,'a') is only witnessed by the data replica, so its local map is empty for another reason",
                              fullReplicasFor(keyspace, 1, "a").contains(1));
            assertRows(nodeLocal(keyspace, 1, everything), row(1, "a", 1, 1));
            assertRows(nodeLocal(keyspace, 2, everything), materializedOn(keyspace, 2, new Object[][]{ row(1, "a", 1, 2) }));
            assertRows(nodeLocal(keyspace, 3, everything));
        });

        // and the read did reconcile: node 1 now holds the value node 2 had, so the empty answer is not vacuous
        assertRows(nodeLocal(tracked, 1, everything), row(1, "a", 1, 2));
    }

    /**
     * {@link #testFilteredRangeReadWhereReconciliationRestoresTheOnlyMatch} on a table with a static column and with no
     * LIMIT. The filter is on {@code v}, a regular column, so the static value does not keep the partition in the
     * materialized map: {@code RowFilter.applyToPartition} discards a partition whose every row a row level expression
     * rejects, static row or not. What the static column adds is that the static row has to come back alongside the
     * value reconciliation restored.
     */
    @Test
    public void testFilteredRangeReadWithAStaticColumn()
    {
        String[] writes =
        {
            "1:INSERT INTO %s.tbl (pk0, pk1, ck, s, v) VALUES (1, 'a', 1, 7, 1) USING TIMESTAMP 10",
            "2:UPDATE %s.tbl USING TIMESTAMP 20 SET v = 500 WHERE pk0 = 1 AND pk1 = 'a' AND ck = 1"
        };
        String select = "SELECT pk0, pk1, ck, s, v FROM %s.tbl WHERE v > 100 ALLOW FILTERING";
        assertTrackedMatchesOracle("b_static_column", TABLE_WITH_STATIC, writes, select, UNPAGED,
                                   (keyspace, oracle) -> assertDataReplicaCannotAnswerAlone(keyspace, select, oracle));
    }

    /**
     * The direction of a frozen collection predicate decides whether the same shape is broken. Here node 1's local
     * row satisfies {@code fs > {1, 2}} and the row reconciliation delivers does not, so the data replica
     * materializes a matching partition and reconciliation takes it away again - the opposite of the case above. A
     * fix for one direction must not break the other.
     */
    @Test
    public void testFilteredRangeReadOnAFrozenSetInTheDirectionThatWorks()
    {
        String[] writes =
        {
            "1:INSERT INTO %s.tbl (pk0, pk1, ck, fs) VALUES (1, 'a', 1, {3}) USING TIMESTAMP 10",
            "2:UPDATE %s.tbl USING TIMESTAMP 20 SET fs = {1} WHERE pk0 = 1 AND pk1 = 'a' AND ck = 1"
        };
        String select = "SELECT pk0, pk1, ck, fs FROM %s.tbl WHERE fs > {1, 2} ALLOW FILTERING";
        assertTrackedMatchesOracle("b_frozen_set_other_direction", TABLE_WITH_FROZEN_SET, writes, select, UNPAGED,
                                   (keyspace, oracle) -> assertDataReplicaCannotAnswerAlone(keyspace, select, oracle));
    }

    /**
     * The same shape as {@link #testFilteredRangeReadWhereEveryLocalPartitionIsFilteredOut}, except that the
     * mutation reconciliation hands the data replica does satisfy the row filter. That records a follow up key, so
     * the read takes the FilteredFollowupRead path instead of finishing where it stands.
     * <p>
     * The LIMIT is not what opens the follow up path: {@code FilteredCompletedRead.followUpRequired} chases a flagged
     * key whenever one interleaves with the partitions the read kept or the merged result is not yet full, and neither
     * needs a limit - {@link #testFilteredRangeReadWithAStaticColumn} is this shape without one.
     */
    @Test
    public void testFilteredRangeReadWhereReconciliationRestoresTheOnlyMatch()
    {
        String select = "SELECT pk0, pk1, ck, v FROM %s.tbl WHERE v > 100 LIMIT 10 ALLOW FILTERING";
        assertTrackedMatchesOracle("c_reconciled_only_match", TABLE, SOLE_PARTITION_STALE_ON_NODE_1, select, UNPAGED,
                                   (keyspace, oracle) -> assertDataReplicaCannotAnswerAlone(keyspace, select, oracle));
    }

    /**
     * The same follow up path reached with a limit that merely exceeds the reconciled result set rather than
     * dwarfing it: two rows come back and the limit is three. Reaching the path at all also needs the fix
     * {@link #testUnlimitedFilteredRangeReadWhereAFlaggedKeySortsLast} covers, because the key reconciliation
     * flagged here sorts after a partition the read kept.
     */
    @Test
    public void testFilteredRangeReadWithALimitLargerThanTheReconciledResult()
    {
        String select = "SELECT pk0, pk1, ck, v FROM %s.tbl WHERE v > 100 LIMIT 3 ALLOW FILTERING";
        assertTrackedMatchesOracle("c_limit_exceeds_result", TABLE, TWO_PARTITIONS_ONE_STALE_ON_NODE_1, select, UNPAGED,
                                   (keyspace, oracle) -> assertDataReplicaCannotAnswerAlone(keyspace, select, oracle));
    }

    /**
     * The control for the two methods above, and the axis that separates them from it: reconciliation contributes to
     * a partition the data replica kept rather than to one it discarded, so nothing is recorded as a follow up key
     * and the limit is filled by the merge itself. No follow up read is requested and FilteredFollowupRead is never
     * constructed.
     * <p>
     * Node 2's row is the lowest clustering in the partition, so the two rows the limit admits are not the two the
     * data replica holds, which is what the probe checks.
     */
    @Test
    public void testFilteredRangeReadWithALimitTheReconciledResultSatisfies()
    {
        String[] writes =
        {
            "1:INSERT INTO %s.tbl (pk0, pk1, ck, v) VALUES (1, 'a', 2, 500) USING TIMESTAMP 10",
            "1:INSERT INTO %s.tbl (pk0, pk1, ck, v) VALUES (1, 'a', 3, 600) USING TIMESTAMP 11",
            "2:INSERT INTO %s.tbl (pk0, pk1, ck, v) VALUES (1, 'a', 1, 700) USING TIMESTAMP 12"
        };
        String select = "SELECT pk0, pk1, ck, v FROM %s.tbl WHERE v > 100 LIMIT 2 ALLOW FILTERING";
        assertTrackedMatchesOracle("c_limit_satisfied", TABLE, writes, select, UNPAGED,
                                   (keyspace, oracle) -> assertDataReplicaCannotAnswerAlone(keyspace, select, oracle));
    }

    /**
     * A filtered range read with no limit at all over two partitions, one of which the data replica discards and
     * reconciliation then shows a match in, so the answer has to carry both.
     * <p>
     * A key the row filter dropped is inside the range the read already scanned, so a follow up read that resumes
     * the scan past the last key it saw can never revisit it: PartialTrackedRangeRead.Filtered.FilteredCompletedRead
     * is the only thing that will ever ask for it. It asks when the key interleaves with the partitions the read kept,
     * and otherwise only while the merged result is not full - and short read protection cannot stand in for either,
     * because this read did not stop early, it threw a partition away.
     * <p>
     * With the default Murmur3 partitioner (2,'b') sorts before (1,'a'), so the kept partition is (2,'b') and the
     * discarded one is (1,'a'): not interleaved, and the read reached the end of its range.
     * {@link #testFilteredRangeReadWithALimitLargerThanTheReconciledResult} is the same defect reached with a LIMIT;
     * this one shows it does not need one, which is what separates it from the follow up read defect that one also
     * covers.
     */
    @Test
    public void testUnlimitedFilteredRangeReadWhereAFlaggedKeySortsLast()
    {
        assertTrackedMatchesOracle("f_flagged_key_sorts_last", TABLE, TWO_PARTITIONS_ONE_STALE_ON_NODE_1, FILTER, UNPAGED,
                                   (keyspace, oracle) -> assertDataReplicaCannotAnswerAlone(keyspace, FILTER, oracle));
    }

    /**
     * The control for reading a discarded partition back: what a follow up read fetches still has to survive the row
     * filter. Reconciliation decides which discarded keys to chase by asking the row filter how many matches an update
     * could contain, and that count is deliberately optimistic - it stops at the first expression the update satisfies,
     * so with two partition level expressions any update satisfying either one is chased. Here (1,'b') satisfies
     * {@code pk0 = 1} and not {@code s = 7} and is fetched in full, and only the filter standing between the follow up
     * read and the answer keeps it out of the result.
     */
    @Test
    public void testFilteredRangeReadWhereAFollowUpKeyDoesNotMatchTheFilter()
    {
        String[] writes =
        {
            // (1,'a') matches only once reconciliation has delivered node 2's static value
            "1:INSERT INTO %s.tbl (pk0, pk1, ck, s, v) VALUES (1, 'a', 1, 1, 10) USING TIMESTAMP 10",
            "2:UPDATE %s.tbl USING TIMESTAMP 20 SET s = 7 WHERE pk0 = 1 AND pk1 = 'a'",
            // (1,'b') matches neither before nor after, but its update does satisfy the partition key expression
            "1:INSERT INTO %s.tbl (pk0, pk1, ck, s, v) VALUES (1, 'b', 1, 2, 20) USING TIMESTAMP 11",
            "2:UPDATE %s.tbl USING TIMESTAMP 21 SET s = 3 WHERE pk0 = 1 AND pk1 = 'b'"
        };
        String select = "SELECT pk0, pk1, ck, s, v FROM %s.tbl WHERE pk0 = 1 AND s = 7 LIMIT 10 ALLOW FILTERING";
        assertTrackedMatchesOracle("f_followup_key_not_matching", TABLE_WITH_STATIC, writes, select, UNPAGED,
                                   (keyspace, oracle) -> assertDataReplicaCannotAnswerAlone(keyspace, select, oracle));
    }

    /**
     * A tombstoned partition (2,'b') that the range scan reaches before the matching partition (1,'a'), at a page
     * size of one. There is no divergence, no reconciliation and no exception here, so the only way this read can go
     * wrong is silently, by answering a page short.
     * <p>
     * Nothing rescues a short page. The read is not augmented, so it completes as {@code CompletedRead.simple}, which
     * issues no short read follow up, and the coordinator reads an empty page as the end of the result set - so a
     * partition the filter rejects must not reach the limit counter, however little of it the filter rejects.
     * <p>
     * The three methods after this one are this one with a single axis changed, and each axis is necessary.
     */
    @Test
    public void testPagedFilteredRangeReadOverATombstonedPartition()
    {
        assertTrackedMatchesOracle("d_paged_tombstone", TABLE, TOMBSTONED_PARTITION_BEFORE_THE_MATCH, FILTER, 1,
                                   (keyspace, oracle) -> assertEveryReplicaCanAnswerAlone(keyspace, FILTER, oracle));
    }

    /**
     * {@link #testPagedFilteredRangeReadOverATombstonedPartition} unpaged. The whole range is read in one go, so the
     * materialization never stops short and nothing has to survive a page boundary.
     */
    @Test
    public void testUnpagedFilteredRangeReadOverATombstonedPartition()
    {
        assertTrackedMatchesOracle("d_unpaged_tombstone", TABLE, TOMBSTONED_PARTITION_BEFORE_THE_MATCH, FILTER, UNPAGED,
                                   (keyspace, oracle) -> assertEveryReplicaCanAnswerAlone(keyspace, FILTER, oracle));
    }

    /**
     * {@link #testPagedFilteredRangeReadOverATombstonedPartition} without the partition level tombstone. The same
     * three partitions in the same order at the same page size, so neither paging nor the filter nor the presence of
     * non matching partitions is enough on its own. The tombstone is what keeps a partition the filter rejects in the
     * replica's materialized data, where it spends the page's row budget.
     */
    @Test
    public void testPagedFilteredRangeReadWithoutTheTombstone()
    {
        assertTrackedMatchesOracle("d_paged_no_tombstone", TABLE, SAME_PARTITIONS_WITHOUT_THE_TOMBSTONE, FILTER, 1,
                                   (keyspace, oracle) -> assertEveryReplicaCanAnswerAlone(keyspace, FILTER, oracle));
    }

    /**
     * {@link #testPagedFilteredRangeReadOverATombstonedPartition} without the row filter. The same data, tombstone and
     * page size return every row, so the page only comes back empty once a filter can reject everything on it.
     */
    @Test
    public void testPagedUnfilteredRangeReadOverATombstonedPartition()
    {
        String select = "SELECT pk0, pk1, ck, v FROM %s.tbl";
        assertTrackedMatchesOracle("d_paged_unfiltered_tombstone", TABLE, TOMBSTONED_PARTITION_BEFORE_THE_MATCH, select, 1,
                                   (keyspace, oracle) -> assertEveryReplicaCanAnswerAlone(keyspace, select, oracle));
    }

    /**
     * The partner of {@link #testPagedFilteredRangeReadOverATombstonedPartition}, and the reason the two cannot be
     * separated. Filtering on part of the partition key produces a filter made up entirely of partition level
     * expressions, and a row filter only evaluates those when it is applied to a partition rather than to a row
     * iterator, so applied to a row iterator it matches everything and (3,'c') is carried rather than discarded -
     * spending the page's whole row budget on a partition the coordinator discards.
     */
    @Test
    public void testPagedRangeReadFilteredOnAPartitionKeyColumn()
    {
        // (3,'c') sorts ahead of (1,'a'), and pk0 = 1 is the only thing that rules it out
        String[] writes =
        {
            "*:INSERT INTO %s.tbl (pk0, pk1, ck, v) VALUES (3, 'c', 1, 9) USING TIMESTAMP 10",
            "*:INSERT INTO %s.tbl (pk0, pk1, ck, v) VALUES (1, 'a', 1, 500) USING TIMESTAMP 11"
        };
        String select = "SELECT pk0, pk1, ck, v FROM %s.tbl WHERE pk0 = 1 ALLOW FILTERING";
        assertTrackedMatchesOracle("d_paged_partition_key_filter", TABLE, writes, select, 1,
                                   (keyspace, oracle) -> assertEveryReplicaCanAnswerAlone(keyspace, select, oracle));
    }

    /**
     * A page's worth of rows is counted on the replica before the row filter has had a say, so rows that cannot be
     * returned still spend the page. (3,'c') holds one row the filter keeps and one it rejects, which is a full page
     * of two by the replica's count and one row by the coordinator's, and a page shorter than the page size is how
     * AbstractQueryPager recognizes the end of a result set, so (1,'a') is never read.
     */
    @Test
    public void testPagedFilteredRangeReadWhereARejectedRowSpendsThePage()
    {
        // (3,'c') sorts ahead of (1,'a'), and only one of its two rows can satisfy the filter
        String[] writes =
        {
            "*:INSERT INTO %s.tbl (pk0, pk1, ck, v) VALUES (3, 'c', 1, 500) USING TIMESTAMP 10",
            "*:INSERT INTO %s.tbl (pk0, pk1, ck, v) VALUES (3, 'c', 2, 1) USING TIMESTAMP 11",
            "*:INSERT INTO %s.tbl (pk0, pk1, ck, v) VALUES (1, 'a', 1, 500) USING TIMESTAMP 12"
        };
        assertTrackedMatchesOracle("e_rejected_row_spends_page", TABLE, writes, FILTER, 2,
                                   (keyspace, oracle) -> assertEveryReplicaCanAnswerAlone(keyspace, FILTER, oracle));
    }

    /**
     * {@link #testPagedFilteredRangeReadWhereARejectedRowSpendsThePage} with a partition level deletion in front of
     * the rows. The deletion must not change the answer: {@code FilteredMaterializer.matchesFilter} checks the
     * partition's surviving content directly rather than asking {@code UnfilteredRowIterator.isEmpty()}, which short
     * circuits on a partition level deletion and would report the partition as non empty however little of it can
     * satisfy the filter.
     */
    @Test
    public void testPagedFilteredRangeReadWhereARejectedRowSpendsThePageBehindATombstone()
    {
        // the deletion predates every row it covers, so it removes nothing
        String[] writes =
        {
            "*:DELETE FROM %s.tbl USING TIMESTAMP 5 WHERE pk0 = 3 AND pk1 = 'c'",
            "*:INSERT INTO %s.tbl (pk0, pk1, ck, v) VALUES (3, 'c', 1, 500) USING TIMESTAMP 10",
            "*:INSERT INTO %s.tbl (pk0, pk1, ck, v) VALUES (3, 'c', 2, 1) USING TIMESTAMP 11",
            "*:INSERT INTO %s.tbl (pk0, pk1, ck, v) VALUES (1, 'a', 1, 500) USING TIMESTAMP 12"
        };
        assertTrackedMatchesOracle("e_rejected_row_behind_a_tombstone", TABLE, writes, FILTER, 2,
                                   (keyspace, oracle) -> assertEveryReplicaCanAnswerAlone(keyspace, FILTER, oracle));
    }
}
