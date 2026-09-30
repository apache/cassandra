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
 * SAI keeps a static term per partition where a legacy index keeps one index row whose clustering is the base partition
 * key, so a tracked indexed read over one says little about a read over the other.
 */
@RunWith(Parameterized.class)
public class TrackedLegacyIndexedRangeReadTest extends TrackedRangeReadTestBase
{
    private static final String TABLE_WITH_INDEXED_STATIC =
        "CREATE TABLE %s.tbl (pk0 int, pk1 text, ck int, s int static, v int, PRIMARY KEY ((pk0, pk1), ck)) WITH read_repair = 'NONE';" +
        "CREATE INDEX tbl_pk0 ON %s.tbl(pk0) USING 'legacy_local_table';" +
        "CREATE INDEX tbl_s ON %s.tbl(s) USING 'legacy_local_table'";

    private static final String TABLE_WITH_INDEXED_VALUE =
        "CREATE TABLE %s.tbl (pk0 int, pk1 text, ck int, v int, w int, PRIMARY KEY ((pk0, pk1), ck)) WITH read_repair = 'NONE';" +
        "CREATE INDEX tbl_v ON %s.tbl(v) USING 'legacy_local_table'";

    /**
     * Pinned to {@code tbl_pk0} so that {@code s = 7} is left to filtering. Without the hint the two plans tie in
     * {@code SecondaryIndexManager.getBestIndexQueryPlanFor} (each estimates zero rows while its index table is
     * unflushed), so the winner is whichever the {@code HashSet} iterates first.
     */
    private static final String STATIC_SELECT =
        "SELECT pk0, pk1, ck, s, v FROM %s.tbl WHERE pk0 = 1 AND s = 7 ALLOW FILTERING "
        + "WITH included_indexes = {tbl_pk0}";

    @Test
    public void testIndexedRangeReadWhereAStaticOnlyPartitionDoesNotMatch()
    {
        staticOnlyPartitionDoesNotMatch("g_legacy_indexed_static_only", TABLE_WITH_INDEXED_STATIC, STATIC_SELECT);
    }

    @Test
    public void testPagedIndexedRangeReadWhereStaticOnlyPartitionsAreDropped()
    {
        staticOnlyPartitionsAreDropped("g_legacy_indexed_static_only_paged", TABLE_WITH_INDEXED_STATIC, STATIC_SELECT, 1);
    }

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
