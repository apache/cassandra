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
 * A filtered tracked range read that reads the keys reconciliation flagged over more than one round of follow up
 * reads. {@code FilteredFollowupRead} reads flagged keys in key order while its budget of rows lasts and carries the
 * rest over to the next round; this is the read where the keys it read return nothing and a key it carried over
 * holds the answer.
 * <p>
 * Node 1, which coordinates and is the data replica, holds two partitions neither of which matches, and misses one
 * later write to each. Each write sets {@code a = 1}, which satisfies one of the two expressions, so the scan drops
 * both partitions and flags each key with one potential match, and a budget of one row reads the first key and
 * carries the second over. With the default Murmur3 partitioner (1,'z') sorts before (1,'a'). (1,'z') is the key
 * read first, and its row's {@code b} is 3, so reading it again returns nothing and the answer after that round holds
 * no partition. (1,'a') is the key carried over, and its row's {@code b} is 2, so the correct answer is that row.
 * <p>
 * The same answer, with no partition in it after a round, can also leave the range unread. Every replica holds
 * (1,'z'), (1,'a') and (1,'j'), in that token order. (1,'a') and (1,'j') match; (1,'z') does not. Node 1 misses
 * {@code a = 1} on (1,'z'), which flags it and still leaves it unmatched, and {@code a = 0} on (1,'a'), which makes
 * it stop matching. The scan drops (1,'z'), keeps (1,'a') and stops at the limit, short of (1,'j'), so the range left
 * to read is everything past (1,'a'). Reconciliation removes the only row the scan kept, and the round re-reads
 * (1,'z') with the whole budget of one row, leaving none to read the rest of the range with. The answer after that
 * round is empty, and the correct answer is (1,'j').
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

    /** The budget is what is left of the LIMIT, one row. */
    @Test
    public void testFilteredRangeReadWhereAKeyCarriedToTheNextRoundMatches()
    {
        carriedOverKeyMatches("n_carried_over_key_matches", "LIMIT 1");
    }

    /**
     * With no LIMIT the budget is the per partition limit, one row, and the answer can never be full, so the rounds
     * stop only once no key is left to read.
     */
    @Test
    public void testPerPartitionLimitedFilteredRangeReadWhereACarriedKeyMatches()
    {
        carriedOverKeyMatches("n_carried_over_key_matches_ppl", "PER PARTITION LIMIT 1");
    }

    private static void carriedOverKeyMatches(String name, String limit)
    {
        String select = "SELECT pk0, pk1, ck, a, b FROM %s.tbl WHERE a = 1 AND b = 2 " + limit + " ALLOW FILTERING";
        assertTrackedMatchesOracle(name, TABLE, BOTH_KEYS_FLAGGED, select, UNPAGED, (keyspace, oracle) -> {
            // both keys have to be dropped by the one scan node 1 answers from its own data, or neither is flagged
            assertReadTogetherFromNode1(keyspace, row(1, "z"), row(1, "a"));
            assertRows(nodeLocal(keyspace, 1, "SELECT pk0, pk1, ck, a, b FROM %s.tbl"),
                       row(1, "z", 1, 0, 3), row(1, "a", 1, 0, 2));
            assertDataReplicaCannotAnswerAlone(keyspace, select, oracle);
        });
    }

    /** The budget is what is left of the LIMIT, one row. */
    @Test
    public void testFilteredRangeReadWhereTheFlaggedKeysSpendTheBudgetReadsTheRestOfTheRange()
    {
        restOfRangeIsRead("o_rest_of_range_limit", FILTER + " LIMIT 1 ALLOW FILTERING", UNPAGED);
    }

    /**
     * The same read with a page as its limit. An empty page is the end of the result set to the pager, so the row
     * the round never read is not returned on a later page either.
     */
    @Test
    public void testPagedFilteredRangeReadWhereTheFlaggedKeysSpendThePageReadsTheRestOfTheRange()
    {
        restOfRangeIsRead("o_rest_of_range_paged", FILTER + " ALLOW FILTERING", 1);
    }

    private static void restOfRangeIsRead(String name, String select, int pageSize)
    {
        String tracked = assertTrackedMatchesOracle(name, TABLE, FLAGGED_KEY_AND_KEPT_KEY_STALE, select, pageSize, (keyspace, oracle) -> {
            // the scan that drops (1,'z'), keeps (1,'a') and stops short of (1,'j') is one scan of node 1's own data
            assertReadTogetherFromNode1(keyspace, row(1, "z"), row(1, "a"), row(1, "j"));
            assertRows(nodeLocal(keyspace, 1, "SELECT pk0, pk1, ck, a, b FROM %s.tbl"),
                       row(1, "z", 1, 0, 3), row(1, "a", 1, 1, 2), row(1, "j", 1, 1, 2));
            assertRows(oracle, row(1, "j", 1, 1, 2));
            assertDataReplicaCannotAnswerAlone(keyspace, select, oracle);
        });
        // the read reconciled node 1: both writes it missed are now on it
        assertRows(nodeLocal(tracked, 1, "SELECT pk0, pk1, ck, a, b FROM %s.tbl"),
                   row(1, "z", 1, 1, 3), row(1, "a", 1, 0, 2), row(1, "j", 1, 1, 2));
    }
}
