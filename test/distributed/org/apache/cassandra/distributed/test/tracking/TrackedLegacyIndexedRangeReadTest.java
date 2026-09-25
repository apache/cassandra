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

/**
 * The tracked indexed range read shapes {@link TrackedRangeReadTestBase} defines, asserted against legacy 2i. SAI and
 * legacy 2i reach the same answer by entirely different means - SAI keeps a static term per partition where a legacy
 * index keeps one index row whose clustering is the base partition key, and a tracked read indexes the mutations
 * reconciliation delivers with whichever implementation the table has - so a tracked index read over the two is two
 * different reads, and what one of them returns says little about the other.
 * <p>
 * A query uses one legacy index only, so a predicate of two expressions leaves whichever one the chosen index does
 * not serve to filtering, and CQL will not run it without {@code ALLOW FILTERING}. Which of the two indexes the
 * planner picks is pinned with an index hint rather than left to it, for the reason {@link #STATIC_SELECT} gives.
 */
@RunWith(Parameterized.class)
public class TrackedLegacyIndexedRangeReadTest extends TrackedRangeReadTestBase
{
    private static final String TABLE_WITH_INDEXED_STATIC =
        "CREATE TABLE %s.tbl (pk0 int, pk1 text, ck int, s int static, v int, PRIMARY KEY ((pk0, pk1), ck)) WITH read_repair = 'NONE';" +
        "CREATE INDEX tbl_pk0 ON %s.tbl(pk0) USING 'legacy_local_table';" +
        "CREATE INDEX tbl_s ON %s.tbl(s) USING 'legacy_local_table'";

    /** {@code v} is indexed and {@code w} is not, so a filter on {@code w} is left for the read to apply itself. */
    private static final String TABLE_WITH_INDEXED_VALUE =
        "CREATE TABLE %s.tbl (pk0 int, pk1 text, ck int, v int, w int, PRIMARY KEY ((pk0, pk1), ck)) WITH read_repair = 'NONE';" +
        "CREATE INDEX tbl_v ON %s.tbl(v) USING 'legacy_local_table'";

    /**
     * Two expressions, and two indexes that can each serve one of them, pinned to the one on the partition key column
     * with an index hint. That is the index these cases are about: it matches every partition with {@code pk0 = 1},
     * the static only ones among them, and leaves {@code s = 7} to filtering, so the partition the index matched and
     * the filter rejects is one the read itself has to drop.
     * <p>
     * Left to the planner the choice is not deterministic.
     * {@code SecondaryIndexManager.getBestIndexQueryPlanFor} collects the candidate plans in a {@code HashSet} and
     * takes the maximum under the hint comparator followed by reversed natural order on the plan. With no hints the
     * hint comparator returns zero for every pair, so what decides is the reversed order, which ranks a plan on
     * {@code Index#getEstimatedResultRows} and then on how many indexes it holds. These two plans tie on both: the
     * estimate is zero for each while their index tables are unflushed, and each holds one index.
     * {@code SingletonIndexQueryPlan} defines no {@code equals}, and {@code Stream#max} keeps the element it already
     * holds when the comparison is a tie, so the plan that wins is whichever the set iterated first - identity hash
     * order. The hint orders the two strictly, and {@code SelectOptions#validate} rejects the query outright if the
     * selected plan does not contain an included index, so a read that reached the other index could not pass quietly
     * either.
     */
    private static final String STATIC_SELECT =
        "SELECT pk0, pk1, ck, s, v FROM %s.tbl WHERE pk0 = 1 AND s = 7 ALLOW FILTERING "
        + "WITH included_indexes = {tbl_pk0}";

    @Test
    public void testIndexedRangeReadWhereAStaticOnlyPartitionDoesNotMatch()
    {
        staticOnlyPartitionDoesNotMatch("g_legacy_indexed_static_only", TABLE_WITH_INDEXED_STATIC, STATIC_SELECT);
    }

    /** {@link TrackedRangeReadTestBase#staticOnlyPartitionsAreDropped} with legacy 2i, paged one row at a time. */
    @Test
    public void testPagedIndexedRangeReadWhereStaticOnlyPartitionsAreDropped()
    {
        staticOnlyPartitionsAreDropped("g_legacy_indexed_static_only_paged", TABLE_WITH_INDEXED_STATIC, STATIC_SELECT, 1);
    }

    /** {@link TrackedRangeReadTestBase#staticOnlyPartitionsAreDropped} with legacy 2i, under a LIMIT. */
    @Test
    public void testLimitedIndexedRangeReadWhereStaticOnlyPartitionsAreDropped()
    {
        String select = "SELECT pk0, pk1, ck, s, v FROM %s.tbl WHERE pk0 = 1 AND s = 7 LIMIT 1 ALLOW FILTERING "
                        + "WITH included_indexes = {tbl_pk0}";
        staticOnlyPartitionsAreDropped("g_legacy_indexed_static_only_limited", TABLE_WITH_INDEXED_STATIC, select, UNPAGED);
    }

    @Test
    public void testIndexedRangeReadHandedAKeyPastTheScannedRange()
    {
        indexedRangeReadHandedAKeyPastTheScannedRange("j_legacy_indexed_key_past_the_scan", TABLE_WITH_INDEXED_VALUE);
    }
}
