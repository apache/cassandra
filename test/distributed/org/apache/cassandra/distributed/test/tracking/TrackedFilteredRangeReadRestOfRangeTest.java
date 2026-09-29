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
 * A filtered tracked range read whose follow up round spends its whole budget on the keys reconciliation flagged, so
 * it reads none of the range past the partitions the scan kept, and whose answer after that round holds no partition.
 * The part of the range the scan never reached still has to be read.
 * <p>
 * With the default Murmur3 partitioner (1,'z') sorts before (1,'a'), which sorts before (1,'j'). Every replica holds
 * all three. (1,'a') and (1,'j') match; (1,'z') does not, because its {@code a} is 0. Node 1, which coordinates and
 * is the data replica, misses two writes made at QUORUM: {@code a = 1} on (1,'z'), which satisfies one of the two
 * expressions and still leaves the row unmatched, since its {@code b} is 3, and {@code a = 0} on (1,'a'), which makes
 * it stop matching.
 * <p>
 * On node 1 the scan drops (1,'z'), keeps (1,'a'), counts one row and stops at the limit, so (1,'j') is never
 * reached and the range left to read is everything past (1,'a'). Reconciliation then flags (1,'z') with one potential
 * match and removes the only row the scan kept. (1,'z') sorts before (1,'a'), the last key the scan kept, so a round
 * of follow up reads re-reads it with the budget of one row and has none left to read the rest of the range with. The
 * re-read row does not match, so the answer after the round is empty, and (1,'j') is the correct answer.
 * <p>
 * Hints, background reconciliation and the reconciler's regular priority queue are off for the reason
 * {@link TrackedRangeReadTestBase} gives where it turns them off: each would bring node 1 the two writes it missed on
 * a timer, before the read runs, and the read would then have nothing to reconcile.
 */
@RunWith(Parameterized.class)
public class TrackedFilteredRangeReadRestOfRangeTest extends TrackedRangeReadTestBase
{
    private static final String TABLE =
        "CREATE TABLE %s.tbl (pk0 int, pk1 text, ck int, a int, b int, PRIMARY KEY ((pk0, pk1), ck)) WITH read_repair = 'NONE'";

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
