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
}
