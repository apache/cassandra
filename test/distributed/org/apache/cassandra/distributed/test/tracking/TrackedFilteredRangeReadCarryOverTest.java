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

import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;

/**
 * Filtered tracked range reads where node 1, the coordinator and data replica, lacks an update to a partition it
 * filtered out. {@code FilteredFollowupRead} re-reads it with a one-row limit, it still does not match, and only a
 * second {@code FilteredFollowupRead} finds the matching row. Under Murmur3, (1,'z') sorts before (1,'a'), which sorts
 * before (1,'j').
 * <p>
 * The {@code overlap_} cases are range reads with no PER PARTITION LIMIT whose response holds partition 3 twice: in
 * node 1's own result and in the result of a follow up range read. Node 1 misses the write that creates partition 3.
 * Murmur3 orders the keys 5, 1, 3, so 3 sorts after every partition node 1 holds. Node 1's read ends before the limit,
 * so reconciliation adds 3 to its result, and the follow up range read after partition 1 returns 3 again. In the
 * filtered cases node 1 also misses the update that makes partition 5 match {@code v = 1}, which is what starts
 * {@code FilteredFollowupRead} and its range read. DISTINCT has an internal per partition limit of one, so
 * {@code ExtendingCompletedRead.followUpReadRequired} does not skip the follow up when node 1's read ends early.
 */
@RunWith(Parameterized.class)
public class TrackedFilteredRangeReadCarryOverTest extends TrackedRangeReadTestBase
{
    private static final String TABLE =
        "CREATE TABLE %s.tbl (pk0 int, pk1 text, ck int, a int, b int, PRIMARY KEY ((pk0, pk1), ck)) WITH read_repair = 'NONE'";

    private static final String[] BOTH_KEYS_FLAGGED =
    {
        "*:INSERT INTO %s.tbl (pk0, pk1, ck, a, b) VALUES (1, 'z', 1, 0, 3) USING TIMESTAMP 10",
        "*:INSERT INTO %s.tbl (pk0, pk1, ck, a, b) VALUES (1, 'a', 1, 0, 2) USING TIMESTAMP 11",
        "!1:UPDATE %s.tbl USING TIMESTAMP 20 SET a = 1 WHERE pk0 = 1 AND pk1 = 'z' AND ck = 1",
        "!1:UPDATE %s.tbl USING TIMESTAMP 21 SET a = 1 WHERE pk0 = 1 AND pk1 = 'a' AND ck = 1"
    };

    private static final String[] FLAGGED_KEY_AND_KEPT_KEY_STALE =
    {
        "*:INSERT INTO %s.tbl (pk0, pk1, ck, a, b) VALUES (1, 'z', 1, 0, 3) USING TIMESTAMP 10",
        "*:INSERT INTO %s.tbl (pk0, pk1, ck, a, b) VALUES (1, 'a', 1, 1, 2) USING TIMESTAMP 11",
        "*:INSERT INTO %s.tbl (pk0, pk1, ck, a, b) VALUES (1, 'j', 1, 1, 2) USING TIMESTAMP 12",
        "!1:UPDATE %s.tbl USING TIMESTAMP 20 SET a = 1 WHERE pk0 = 1 AND pk1 = 'z' AND ck = 1",
        "!1:UPDATE %s.tbl USING TIMESTAMP 21 SET a = 0 WHERE pk0 = 1 AND pk1 = 'a' AND ck = 1"
    };

    private static final String FILTER = "SELECT pk0, pk1, ck, a, b FROM %s.tbl WHERE a = 1 AND b = 2";

    @Test
    public void testFilteredRangeReadWhereAKeyCarriedToTheNextRoundMatches()
    {
        carriedOverKeyMatches("n_carried_over_key_matches", "LIMIT 1");
    }

    /** Without a LIMIT the row counter is never done, so FilteredFollowupRead reads until no partition is left. */
    @Test
    public void testPerPartitionLimitedFilteredRangeReadWhereACarriedKeyMatches()
    {
        carriedOverKeyMatches("n_carried_over_key_matches_ppl", "PER PARTITION LIMIT 1");
    }

    private static void carriedOverKeyMatches(String name, String limit)
    {
        String select = "SELECT pk0, pk1, ck, a, b FROM %s.tbl WHERE a = 1 AND b = 2 " + limit + " ALLOW FILTERING";
        assertTrackedMatchesOracle(name, TABLE, BOTH_KEYS_FLAGGED, select, UNPAGED, (keyspace, oracle) -> {
            assertReadTogetherFromNode1(keyspace, row(1, "z"), row(1, "a"));
            assertRows(nodeLocal(keyspace, 1, "SELECT pk0, pk1, ck, a, b FROM %s.tbl"),
                       row(1, "z", 1, 0, 3), row(1, "a", 1, 0, 2));
            assertDataReplicaCannotAnswerAlone(keyspace, select, oracle);
        });
    }

    @Test
    public void testFilteredRangeReadWhereTheFlaggedKeysSpendTheBudgetReadsTheRestOfTheRange()
    {
        restOfRangeIsRead("o_rest_of_range_limit", FILTER + " LIMIT 1 ALLOW FILTERING", UNPAGED);
    }

    /** An empty page ends paging, so no later page would return (1,'j'). */
    @Test
    public void testPagedFilteredRangeReadWhereTheFlaggedKeysSpendThePageReadsTheRestOfTheRange()
    {
        restOfRangeIsRead("o_rest_of_range_paged", FILTER + " ALLOW FILTERING", 1);
    }

    private static void restOfRangeIsRead(String name, String select, int pageSize)
    {
        String tracked = assertTrackedMatchesOracle(name, TABLE, FLAGGED_KEY_AND_KEPT_KEY_STALE, select, pageSize, (keyspace, oracle) -> {
            assertReadTogetherFromNode1(keyspace, row(1, "z"), row(1, "a"), row(1, "j"));
            assertRows(nodeLocal(keyspace, 1, "SELECT pk0, pk1, ck, a, b FROM %s.tbl"),
                       row(1, "z", 1, 0, 3), row(1, "a", 1, 1, 2), row(1, "j", 1, 1, 2));
            assertRows(oracle, row(1, "j", 1, 1, 2));
            assertDataReplicaCannotAnswerAlone(keyspace, select, oracle);
        });
        assertRows(nodeLocal(tracked, 1, "SELECT pk0, pk1, ck, a, b FROM %s.tbl"),
                   row(1, "z", 1, 1, 3), row(1, "a", 1, 0, 2), row(1, "j", 1, 1, 2));
    }

    private static final String OVERLAP_TABLE =
        "CREATE TABLE %s.tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH read_repair = 'NONE'";

    private static final String[] OVERLAP_FILTERED =
    {
        "*:INSERT INTO %s.tbl (pk, ck, v) VALUES (5, 1, 0) USING TIMESTAMP 10",
        "*:INSERT INTO %s.tbl (pk, ck, v) VALUES (1, 1, 1) USING TIMESTAMP 10",
        "!1:UPDATE %s.tbl USING TIMESTAMP 20 SET v = 1 WHERE pk = 5 AND ck = 1",
        "!1:INSERT INTO %s.tbl (pk, ck, v) VALUES (3, 1, 1) USING TIMESTAMP 20"
    };

    /** As {@link #OVERLAP_FILTERED}, plus a row of 3 that the filter rejects and a row of 3 that is later deleted. */
    private static final String[] OVERLAP_FILTERED_WITH_DELETION =
    {
        "*:INSERT INTO %s.tbl (pk, ck, v) VALUES (5, 1, 0) USING TIMESTAMP 10",
        "*:INSERT INTO %s.tbl (pk, ck, v) VALUES (1, 1, 1) USING TIMESTAMP 10",
        "!1:UPDATE %s.tbl USING TIMESTAMP 20 SET v = 1 WHERE pk = 5 AND ck = 1",
        "!1:INSERT INTO %s.tbl (pk, ck, v) VALUES (3, 1, 1) USING TIMESTAMP 20",
        "!1:INSERT INTO %s.tbl (pk, ck, v) VALUES (3, 2, 1) USING TIMESTAMP 20",
        "!1:INSERT INTO %s.tbl (pk, ck, v) VALUES (3, 3, 0) USING TIMESTAMP 20",
        "!1:DELETE FROM %s.tbl USING TIMESTAMP 30 WHERE pk = 3 AND ck = 2"
    };

    private static final String[] OVERLAP_UNFILTERED =
    {
        "*:INSERT INTO %s.tbl (pk, ck, v) VALUES (5, 1, 0) USING TIMESTAMP 10",
        "*:INSERT INTO %s.tbl (pk, ck, v) VALUES (1, 1, 1) USING TIMESTAMP 10",
        "!1:INSERT INTO %s.tbl (pk, ck, v) VALUES (3, 1, 1) USING TIMESTAMP 20"
    };

    @Test
    public void testFilteredRangeReadWithLimit()
    {
        overlapFiltered("overlap_filtered_limit", OVERLAP_FILTERED, "SELECT pk, ck, v FROM %s.tbl WHERE v = 1 LIMIT 10 ALLOW FILTERING");
    }

    @Test
    public void testFilteredRangeReadWithNoLimit()
    {
        overlapFiltered("overlap_filtered_no_limit", OVERLAP_FILTERED, "SELECT pk, ck, v FROM %s.tbl WHERE v = 1 ALLOW FILTERING");
    }

    @Test
    public void testFilteredRangeReadWithLimitAndDeletion()
    {
        overlapFiltered("overlap_filtered_limit_deletion", OVERLAP_FILTERED_WITH_DELETION, "SELECT pk, ck, v FROM %s.tbl WHERE v = 1 LIMIT 10 ALLOW FILTERING");
    }

    @Test
    public void testFilteredGroupByRangeRead()
    {
        overlapFiltered("overlap_filtered_group_by", OVERLAP_FILTERED, "SELECT pk, count(*) FROM %s.tbl WHERE v = 1 GROUP BY pk ALLOW FILTERING");
    }

    @Test
    public void testDistinctRangeReadWithNoLimit()
    {
        overlapUnfiltered("overlap_distinct_no_limit", "SELECT DISTINCT pk FROM %s.tbl");
    }

    @Test
    public void testDistinctRangeReadWithLimit()
    {
        overlapUnfiltered("overlap_distinct_limit", "SELECT DISTINCT pk FROM %s.tbl LIMIT 10");
    }

    /** A plain LIMIT with no filter skips the follow up range read when node 1's read ends early, so 3 is read once. */
    @Test
    public void testUnfilteredRangeReadWithLimit()
    {
        overlapUnfiltered("overlap_unfiltered_limit", "SELECT pk, ck, v FROM %s.tbl LIMIT 10");
    }

    private static void overlapFiltered(String name, String[] writes, String select)
    {
        assumeFullReplication();
        assertTrackedMatchesOracle(name, OVERLAP_TABLE, writes, select, UNPAGED, (keyspace, oracle) -> {
            assertRows(nodeLocal(keyspace, 1, "SELECT pk, ck, v FROM %s.tbl"), row(5, 1, 0), row(1, 1, 1));
            assertDataReplicaCannotAnswerAlone(keyspace, select, oracle);
        });
    }

    private static void overlapUnfiltered(String name, String select)
    {
        assumeFullReplication();
        assertTrackedMatchesOracle(name, OVERLAP_TABLE, OVERLAP_UNFILTERED, select, UNPAGED, (keyspace, oracle) -> {
            assertRows(nodeLocal(keyspace, 1, "SELECT pk, ck, v FROM %s.tbl"), row(5, 1, 0), row(1, 1, 1));
            assertRows(nodeLocal(keyspace, 2, "SELECT pk, ck, v FROM %s.tbl"), row(5, 1, 0), row(1, 1, 1), row(3, 1, 1));
            assertDataReplicaCannotAnswerAlone(keyspace, select, oracle);
        });
    }

    /** The base class's witness checks need a (pk0, pk1) key, and the int key gives 5, 1, 3. */
    private static void assumeFullReplication()
    {
        Assume.assumeTrue(mode == Mode.FULL);
    }
}
