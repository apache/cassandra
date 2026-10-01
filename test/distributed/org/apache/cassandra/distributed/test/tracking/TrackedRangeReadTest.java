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
import java.util.function.BiConsumer;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.distributed.api.ConsistencyLevel;

import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;

@RunWith(Parameterized.class)
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

        select = withKeyspace("SELECT * FROM %s.tbl WHERE token(pk0, pk1) > token(4217, -2.2644046491088394E265) AND token(pk0, pk1) < token(-16150, 1.0086497658456055E-263) PER PARTITION LIMIT 89 LIMIT 832", keyspace);
        Iterator<Object[]> pagingResult = cluster.coordinator(3).executeWithPaging(select, ConsistencyLevel.ALL, 10);
        assertRows(pagingResult);
    }

    /**
     * Whether this test makes the data replica's response and a follow up read's response both hold rows of one
     * partition depends on when reconciliation delivers; the {@code overlap_} cases of
     * {@link TrackedFilteredRangeReadCarryOverTest} do it deterministically.
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
     * Reconciliation makes a partition the row filter rejected match while the page has room, so FilteredFollowupRead
     * also starts a range read and gets its PartialTrackedRead from the consumer passed to TrackedRead.startLocal.
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

    private static final int PARTITIONS = 100;

    /**
     * Under {@code '3/1'} each node witnesses one of the three token ranges, so no node is a full replica of every
     * range.
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

        assertEveryNodeHoldsWhatTheModeSays(keyspace);

        for (int node = 1; node <= REPLICAS; node++)
        {
            Object[][] rows = cluster.coordinator(node).execute(withKeyspace("SELECT pk, v FROM %s.tbl", keyspace), ConsistencyLevel.ALL);
            Map<Integer, Integer> actual = new TreeMap<>();
            for (Object[] row : rows)
                Assert.assertNull("partition " + row[0] + " returned twice", actual.put((Integer) row[0], (Integer) row[1]));
            Assert.assertEquals("full table scan coordinated on node " + node, expected, actual);
        }
    }

    private static void assertEveryNodeHoldsWhatTheModeSays(String keyspace)
    {
        for (int node = 1; node <= REPLICAS; node++)
        {
            int local = nodeLocal(keyspace, node, "SELECT pk FROM %s.tbl").length;
            if (mode == Mode.FULL)
            {
                Assert.assertEquals("node " + node + " does not hold all " + PARTITIONS + " partitions", PARTITIONS, local);
            }
            else
            {
                Assert.assertTrue("Not stressed: node " + node + " holds all " + PARTITIONS + " partitions, so it witnesses none of them",
                                  local < PARTITIONS);
                Assert.assertTrue("node " + node + " holds no data at all, so it is not a full replica of anything",
                                  local > 0);
            }
        }
    }

    private static final String TABLE =
        "CREATE TABLE %s.tbl (pk0 int, pk1 text, ck int, v int, PRIMARY KEY ((pk0, pk1), ck)) WITH read_repair = 'NONE'";

    private static final String TABLE_WITH_STATIC =
        "CREATE TABLE %s.tbl (pk0 int, pk1 text, ck int, s int static, v int, PRIMARY KEY ((pk0, pk1), ck)) WITH read_repair = 'NONE'";

    private static final String TABLE_WITH_FROZEN_SET =
        "CREATE TABLE %s.tbl (pk0 int, pk1 text, ck int, fs frozen<set<int>>, PRIMARY KEY ((pk0, pk1), ck)) WITH read_repair = 'NONE'";

    private static final String TABLE_WITH_INDEXED_VALUE =
        "CREATE TABLE %s.tbl (pk0 int, pk1 text, ck int, v int, w int, PRIMARY KEY ((pk0, pk1), ck)) WITH read_repair = 'NONE';" +
        "CREATE INDEX tbl_v ON %s.tbl(v) USING 'SAI'";

    /** Legacy 2i: SAI's {@code IndexTermType.valueOf} returns null for a clustering column of a static row. */
    private static final String TABLE_WITH_INDEXED_CLUSTERING_AND_STATIC =
        "CREATE TABLE %s.tbl (pk0 int, pk1 text, ck int, s int static, v int, PRIMARY KEY ((pk0, pk1), ck)) WITH read_repair = 'NONE';" +
        "CREATE INDEX tbl_ck ON %s.tbl(ck) USING 'legacy_local_table'";

    /**
     * No query projects {@code s2}, so a write can set it to show which replicas received the write without changing
     * any query's answer.
     */
    private static final String TABLE_WITH_LEGACY_INDEXED_STATIC =
        "CREATE TABLE %s.tbl (pk0 int, pk1 text, ck int, s int static, s2 int static, v int, PRIMARY KEY ((pk0, pk1), ck)) WITH read_repair = 'NONE';" +
        "CREATE INDEX tbl_s ON %s.tbl(s) USING 'legacy_local_table'";

    private static final String TABLE_WITH_INDEXED_PARTITION_KEY_AND_STATIC =
        "CREATE TABLE %s.tbl (pk0 int, pk1 text, ck int, s int static, v int, PRIMARY KEY ((pk0, pk1), ck)) WITH read_repair = 'NONE';" +
        "CREATE INDEX tbl_pk0 ON %s.tbl(pk0) USING 'legacy_local_table'";

    private static final String FILTER = "SELECT pk0, pk1, ck, v FROM %s.tbl WHERE v > 100 ALLOW FILTERING";

    private static final String[] SOLE_PARTITION_STALE_ON_NODE_1 =
    {
        "1:INSERT INTO %s.tbl (pk0, pk1, ck, v) VALUES (1, 'a', 1, 1) USING TIMESTAMP 10",
        "2:UPDATE %s.tbl USING TIMESTAMP 20 SET v = 500 WHERE pk0 = 1 AND pk1 = 'a' AND ck = 1"
    };

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
     * With the default Murmur3 partitioner (1,'z'), which node 1's row filter rejects and reconciliation makes match,
     * sorts before (1,'a').
     */
    private static final String[] INTERLEAVING_STALE_PARTITION_ON_NODE_1 =
    {
        "*:INSERT INTO %s.tbl (pk0, pk1, ck, v) VALUES (1, 'a', 1, 900) USING TIMESTAMP 10",
        "*:INSERT INTO %s.tbl (pk0, pk1, ck, v) VALUES (1, 'z', 1, 1) USING TIMESTAMP 11",
        "!1:UPDATE %s.tbl USING TIMESTAMP 20 SET v = 500 WHERE pk0 = 1 AND pk1 = 'z' AND ck = 1"
    };

    private static BiConsumer<String, Object[][]> interleavingProbe(String select)
    {
        return (keyspace, oracle) -> {
            assertReadTogetherFromNode1(keyspace, row(1, "z"), row(1, "a"));
            assertDataReplicaCannotAnswerAlone(keyspace, select, oracle);
        };
    }

    /**
     * The answer is empty, which node 1 also returns on its own, so the lambda checks each node's rows instead of
     * calling {@link #assertDataReplicaCannotAnswerAlone}.
     */
    @Test
    public void testFilteredRangeReadWhereEveryLocalPartitionIsFilteredOut()
    {
        String everything = "SELECT pk0, pk1, ck, v FROM %s.tbl";
        String[] writes =
        {
            "1:INSERT INTO %s.tbl (pk0, pk1, ck, v) VALUES (1, 'a', 1, 1) USING TIMESTAMP 10",
            "2:UPDATE %s.tbl USING TIMESTAMP 20 SET v = 2 WHERE pk0 = 1 AND pk1 = 'a' AND ck = 1"
        };

        String tracked = assertTrackedMatchesOracle("b_all_locals_filtered", TABLE, writes, FILTER, UNPAGED, (keyspace, oracle) -> {
            Assert.assertTrue("(1,'a') is only witnessed by the data replica, so its local map is empty for another reason",
                              fullReplicasFor(keyspace, 1, "a").contains(1));
            assertRows(nodeLocal(keyspace, 1, everything), row(1, "a", 1, 1));
            assertRows(nodeLocal(keyspace, 2, everything), materializedOn(keyspace, 2, new Object[][]{ row(1, "a", 1, 2) }));
            assertRows(nodeLocal(keyspace, 3, everything));
        });

        assertRows(nodeLocal(tracked, 1, everything), row(1, "a", 1, 2));
    }

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

    @Test
    public void testFilteredRangeReadWhereReconciliationRestoresTheOnlyMatch()
    {
        String select = "SELECT pk0, pk1, ck, v FROM %s.tbl WHERE v > 100 LIMIT 10 ALLOW FILTERING";
        assertTrackedMatchesOracle("c_reconciled_only_match", TABLE, SOLE_PARTITION_STALE_ON_NODE_1, select, UNPAGED,
                                   (keyspace, oracle) -> assertDataReplicaCannotAnswerAlone(keyspace, select, oracle));
    }

    @Test
    public void testFilteredRangeReadWithALimitLargerThanTheReconciledResult()
    {
        String select = "SELECT pk0, pk1, ck, v FROM %s.tbl WHERE v > 100 LIMIT 3 ALLOW FILTERING";
        assertTrackedMatchesOracle("c_limit_exceeds_result", TABLE, TWO_PARTITIONS_ONE_STALE_ON_NODE_1, select, UNPAGED,
                                   (keyspace, oracle) -> assertDataReplicaCannotAnswerAlone(keyspace, select, oracle));
    }

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
     * With the default Murmur3 partitioner (1,'a'), which node 1's row filter rejects and reconciliation makes match,
     * sorts after (2,'b'), so only {@code FilteredCompletedRead.hasRoomForFollowUpKeys} requests its read.
     */
    @Test
    public void testUnlimitedFilteredRangeReadWhereAFlaggedKeySortsLast()
    {
        assertTrackedMatchesOracle("f_flagged_key_sorts_last", TABLE, TWO_PARTITIONS_ONE_STALE_ON_NODE_1, FILTER, UNPAGED,
                                   (keyspace, oracle) -> assertDataReplicaCannotAnswerAlone(keyspace, FILTER, oracle));
    }

    /**
     * {@code RowFilter.RowFilterTransformation.potentialMatches} counts a match when an update satisfies any partition
     * key or static column expression, so (1,'b'), whose update satisfies {@code pk0 = 1} but not {@code s = 7}, is
     * read and only the row filter keeps it out of the result.
     */
    @Test
    public void testFilteredRangeReadWhereAFollowUpKeyDoesNotMatchTheFilter()
    {
        String[] writes =
        {
            "1:INSERT INTO %s.tbl (pk0, pk1, ck, s, v) VALUES (1, 'a', 1, 1, 10) USING TIMESTAMP 10",
            "2:UPDATE %s.tbl USING TIMESTAMP 20 SET s = 7 WHERE pk0 = 1 AND pk1 = 'a'",
            "1:INSERT INTO %s.tbl (pk0, pk1, ck, s, v) VALUES (1, 'b', 1, 2, 20) USING TIMESTAMP 11",
            "2:UPDATE %s.tbl USING TIMESTAMP 21 SET s = 3 WHERE pk0 = 1 AND pk1 = 'b'"
        };
        String select = "SELECT pk0, pk1, ck, s, v FROM %s.tbl WHERE pk0 = 1 AND s = 7 LIMIT 10 ALLOW FILTERING";
        assertTrackedMatchesOracle("f_followup_key_not_matching", TABLE_WITH_STATIC, writes, select, UNPAGED,
                                   (keyspace, oracle) -> assertDataReplicaCannotAnswerAlone(keyspace, select, oracle));
    }

    /**
     * Identical to {@link #testFilteredRangeReadWhereAnInterleavingKeyDisplacesTheRowTheLimitAdmits},
     * except that the stale interleaving partition (1,'z') also carries a static column.
     * <p>
     * Under commit 1949ec15e237, {@code matchesFilter} checks {@code !partition.staticRow().isEmpty()},
     * which incorrectly returns {@code true} for (1,'z') on node 1 even though its only clustering row
     * does not satisfy {@code v > 100}. Because it returns {@code true}, (1,'z') is never added to
     * {@code filteredKeys}. When reconciliation delivers the update from node 2 that makes (1,'z')
     * match, {@code canAcceptUpdate} does not see (1,'z') in {@code filteredKeys}, so it fails to
     * record it in {@code followUpReadInfo} and does not trigger {@link FilteredFollowupRead}.
     * The row in (1,'z') that should displace (1,'a') is lost.
     */
    @Test
    public void testFilteredRangeReadWhereInterleavingKeyWithStaticRowDisplacesTheRowTheLimitAdmits()
    {
        String[] writes =
        {
            "*:INSERT INTO %s.tbl (pk0, pk1, ck, s, v) VALUES (1, 'a', 1, 7, 900) USING TIMESTAMP 10",
            "*:INSERT INTO %s.tbl (pk0, pk1, ck, s, v) VALUES (1, 'z', 1, 7, 1) USING TIMESTAMP 11",
            "!1:UPDATE %s.tbl USING TIMESTAMP 20 SET v = 500 WHERE pk0 = 1 AND pk1 = 'z' AND ck = 1"
        };
        String select = "SELECT pk0, pk1, ck, s, v FROM %s.tbl WHERE v > 100 LIMIT 1 ALLOW FILTERING";
        assertTrackedMatchesOracle("l_interleaving_key_with_static_unpaged", TABLE_WITH_STATIC, writes, select, UNPAGED,
                                   interleavingProbe(select));
    }

    /**
     * The row of (1,'z') replaces the row of (1,'a') that fills {@code LIMIT 1}, so {@code FilteredFollowupRead} reads
     * (1,'z') under the command's limit, not under the zero rows left of it.
     */
    @Test
    public void testFilteredRangeReadWhereAnInterleavingKeyDisplacesTheRowTheLimitAdmits()
    {
        String select = "SELECT pk0, pk1, ck, v FROM %s.tbl WHERE v > 100 LIMIT 1 ALLOW FILTERING";
        assertTrackedMatchesOracle("l_interleaving_key_unpaged", TABLE, INTERLEAVING_STALE_PARTITION_ON_NODE_1, select, UNPAGED,
                                   interleavingProbe(select));
    }

    /**
     * At a page size of one, reading (1,'z') with the zero rows left of the page would lose its row, since the next
     * page starts after (1,'a'), which sorts after (1,'z').
     */
    @Test
    public void testPagedFilteredRangeReadWhereAnInterleavingKeyDisplacesTheRowOnThePage()
    {
        assertTrackedMatchesOracle("l_interleaving_key_paged", TABLE, INTERLEAVING_STALE_PARTITION_ON_NODE_1, FILTER, 1,
                                   interleavingProbe(FILTER));
    }

    @Test
    public void testPagedGroupByRangeReadWhereAKeyIsFlagged()
    {
        String select = "SELECT pk0, pk1, count(*) FROM %s.tbl WHERE v > 100 GROUP BY pk0, pk1 ALLOW FILTERING";
        assertTrackedMatchesOracle("m_group_by_flagged_key", TABLE, INTERLEAVING_STALE_PARTITION_ON_NODE_1, select, 1,
                                   interleavingProbe(select));
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
     * the rows. The deletion must not change the answer: {@code FilteredMaterializer} decides whether to flag a key by
     * counting the rows the filter keeps, and a partition level deletion is not one of them. Nothing in that path asks
     * {@code UnfilteredRowIterator.isEmpty()}, which reports a partition as non empty whenever it carries a deletion,
     * however little of it can satisfy the filter.
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

    /**
     * An index on a clustering column must skip the static row a reconciled update carries. A throw there happens in
     * read completion, which sends no failure response, so the query hangs until the coordinator times out.
     */
    @Test
    public void testIndexedRangeReadWhereAReconciledUpdateCarriesAStaticRow()
    {
        String[] writes =
        {
            "1:INSERT INTO %s.tbl (pk0, pk1, ck, s, v) VALUES (1, 'a', 1, 7, 10) USING TIMESTAMP 10",
            "2:UPDATE %s.tbl USING TIMESTAMP 20 SET s = 8, v = 500 WHERE pk0 = 1 AND pk1 = 'a' AND ck = 2"
        };
        String select = "SELECT pk0, pk1, ck, s, v FROM %s.tbl WHERE ck = 2 ALLOW FILTERING";
        assertTrackedMatchesOracle("g_indexed_clustering_with_static", TABLE_WITH_INDEXED_CLUSTERING_AND_STATIC,
                                   writes, select, UNPAGED,
                                   (keyspace, oracle) -> assertDataReplicaCannotAnswerAlone(keyspace, select, oracle));
    }

    /**
     * Legacy 2i decodes a static column index entry to {@code Clustering.STATIC_CLUSTERING}, and the entry the tracked
     * read builds from the reconciled mutation must use the same clustering. Otherwise {@code IndexEntry#compare}
     * orders the two entries for the one static row as distinct, {@code CompositesSearcher} reads (1,'b') once per
     * entry, and the client receives that partition twice.
     * <p>
     * Node 1 must receive the repeated update only through reconciliation, so the filter dropping its mutation is reset
     * after the read under test, not inside the lambda.
     */
    @Test
    public void testIndexedStaticColumnRangeReadWhereAReconciledUpdateRepeatsAStaticRow()
    {
        String[] writes =
        {
            "*:INSERT INTO %s.tbl (pk0, pk1, ck, s, v) VALUES (1, 'a', 1, 3, 10) USING TIMESTAMP 10",
            "*:UPDATE %s.tbl USING TIMESTAMP 20 SET s = 7 WHERE pk0 = 1 AND pk1 = 'b'"
        };
        String select = "SELECT pk0, pk1, ck, s, v FROM %s.tbl WHERE s = 7";
        try
        {
            assertTrackedMatchesOracle("g_indexed_static_repeated", TABLE_WITH_LEGACY_INDEXED_STATIC, writes, select,
                                       UNPAGED, (keyspace, oracle) -> {
                                           repeatTheStaticUpdateAwayFromTheDataReplica(keyspace);
                                           assertEveryReplicaCanAnswerAlone(keyspace, select, oracle);
                                       });
        }
        finally
        {
            cluster.filters().reset();
        }
    }

    private static void repeatTheStaticUpdateAwayFromTheDataReplica(String keyspace)
    {
        cluster.filters().inbound().verbs(org.apache.cassandra.net.Verb.MUTATION_REQ.id).to(1).drop();
        cluster.coordinator(2).execute(withKeyspace("UPDATE %s.tbl USING TIMESTAMP 20 SET s = 7, s2 = 1 "
                                                   + "WHERE pk0 = 1 AND pk1 = 'b'", keyspace), ConsistencyLevel.ONE);
        assertRows(nodeLocal(keyspace, 1, "SELECT pk0, pk1, s, s2 FROM %s.tbl WHERE pk0 = 1 AND pk1 = 'b'"),
                   materializedOn(keyspace, 1, new Object[][]{ row(1, "b", 7, null) }));
    }

    /**
     * A partition key column index also indexes the static row, and that entry decodes to a base clustering of nulls
     * rather than to {@code Clustering.STATIC_CLUSTERING} (see {@code CassandraIndex#createIndexEntry}). The tracked
     * read must build it the same way: CompositesSearcher names the decoded clustering in a
     * {@code ClusteringIndexNamesFilter}, and the static clustering cannot be compared against clusterings that have
     * values. The filter is built inside the read's completion, where a throw sends no failure response.
     */
    @Test
    public void testIndexedPartitionKeyRangeReadWhereAReconciledUpdateCarriesAStaticRow()
    {
        String[] writes =
        {
            "1:INSERT INTO %s.tbl (pk0, pk1, ck, s, v) VALUES (1, 'a', 1, 7, 10) USING TIMESTAMP 10",
            "2:UPDATE %s.tbl USING TIMESTAMP 20 SET s = 8, v = 500 WHERE pk0 = 1 AND pk1 = 'a' AND ck = 2",
            "2:UPDATE %s.tbl USING TIMESTAMP 30 SET s = 9 WHERE pk0 = 1 AND pk1 = 'b'"
        };
        String select = "SELECT pk0, pk1, ck, s, v FROM %s.tbl WHERE pk0 = 1 ALLOW FILTERING";
        assertTrackedMatchesOracle("g_indexed_pk_with_static", TABLE_WITH_INDEXED_PARTITION_KEY_AND_STATIC,
                                   writes, select, UNPAGED,
                                   (keyspace, oracle) -> assertDataReplicaCannotAnswerAlone(keyspace, select, oracle));
    }

    @Test
    public void testIndexedPartitionKeyRangeReadWhereAStaticOnlyPartitionAlsoHasTombstones()
    {
        String[] writes =
        {
            "1:INSERT INTO %s.tbl (pk0, pk1, ck, s, v) VALUES (1, 'a', 1, 7, 10) USING TIMESTAMP 10",
            "2:DELETE v, s FROM %s.tbl USING TIMESTAMP 13 WHERE pk0 = 1 AND pk1 = 'b' AND ck = 1",
            "3:UPDATE %s.tbl USING TIMESTAMP 15 SET s = 9 WHERE pk0 = 1 AND pk1 = 'b'"
        };
        String select = "SELECT pk0, pk1, ck, s, v FROM %s.tbl WHERE pk0 = 1 ALLOW FILTERING";
        assertTrackedMatchesOracle("g_indexed_pk_static_only_with_tombstones", TABLE_WITH_INDEXED_PARTITION_KEY_AND_STATIC,
                                   writes, select, UNPAGED,
                                   (keyspace, oracle) -> assertDataReplicaCannotAnswerAlone(keyspace, select, oracle));
    }

    /**
     * The RTBoundCloser in ReadCommand.completeRead appends the closing range tombstone bound only when asked for more
     * rows after the counter there stops, so ExtendingCompletedRead's counter must not stop on the same row: the
     * {@code PROCESSED} {@code RTBoundValidator} would then see an open range tombstone and throw.
     */
    @Test
    public void testRangeReadWhosePerPartitionLimitFallsInsideARangeTombstone()
    {
        String[] writes =
        {
            "*:INSERT INTO %s.tbl (pk0, pk1, ck, v) VALUES (1, 'a', 1, 10) USING TIMESTAMP 10",
            "*:DELETE FROM %s.tbl USING TIMESTAMP 20 WHERE pk0 = 1 AND pk1 = 'a' AND ck >= 2 AND ck <= 6",
            "!1:INSERT INTO %s.tbl (pk0, pk1, ck, v) VALUES (1, 'a', 3, 30) USING TIMESTAMP 30",
            "*:INSERT INTO %s.tbl (pk0, pk1, ck, v) VALUES (1, 'a', 5, 50) USING TIMESTAMP 50"
        };
        String select = "SELECT pk0, pk1, ck, v FROM %s.tbl PER PARTITION LIMIT 2";
        assertTrackedMatchesOracle("h_per_partition_limit_inside_rt", TABLE, writes, select, UNPAGED,
                                   (keyspace, oracle) -> assertDataReplicaCannotAnswerAlone(keyspace, select, oracle));
    }

    @Test
    public void testIndexedRangeReadHandedAKeyPastTheScannedRange()
    {
        indexedRangeReadHandedAKeyPastTheScannedRange("j_indexed_key_past_the_scan", TABLE_WITH_INDEXED_VALUE);
    }
}
