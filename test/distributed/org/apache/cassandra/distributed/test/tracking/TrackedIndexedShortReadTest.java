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

import static org.apache.cassandra.distributed.shared.AssertUtils.row;

/**
 * An index read whose short read follows a key that reconciliation handed it, with SAI and with a legacy index.
 */
@RunWith(Parameterized.class)
public class TrackedIndexedShortReadTest extends TrackedRangeReadTestBase
{
    private static final String SAI_TABLE =
        "CREATE TABLE %s.tbl (pk0 int, pk1 text, ck int, v int, w int, PRIMARY KEY ((pk0, pk1), ck)) WITH read_repair = 'NONE';" +
        "CREATE INDEX tbl_v ON %s.tbl(v) USING 'SAI'";

    private static final String LEGACY_TABLE =
        "CREATE TABLE %s.tbl (pk0 int, pk1 text, ck int, v int, w int, PRIMARY KEY ((pk0, pk1), ck)) WITH read_repair = 'NONE';" +
        "CREATE INDEX tbl_v ON %s.tbl(v) USING 'legacy_local_table'";

    /**
     * Node 1 lacks (1,'z') and gets it through reconciliation. (1,'z') sorts between the last key node 1's index scan
     * reached and the next index match the scan did not read, so the read returns (1,'z') and then stops for a short
     * read, and the short read must not return (1,'z') a second time.
     * <p>
     * Murmur3 orders the keys (1,'n'), (1,'u'), (1,'z'), (1,'a'); all match the index on {@code v}. With a page size of
     * two, node 1's index scan stops after (1,'u'), leaving (1,'a') an index match it has not read. (1,'n') and (1,'u')
     * fail the {@code w = 1} filter, so after (1,'z') the page is not full and the read goes on to (1,'a').
     */
    protected static void indexedRangeReadHandedAKeyBeforeTheNextUnreadMatch(String name, String table)
    {
        String[] writes =
        {
        "*:INSERT INTO %s.tbl (pk0, pk1, ck, v, w) VALUES (1, 'n', 1, 100, 0) USING TIMESTAMP 10",
        "*:INSERT INTO %s.tbl (pk0, pk1, ck, v, w) VALUES (1, 'u', 1, 100, 0) USING TIMESTAMP 11",
        "*:INSERT INTO %s.tbl (pk0, pk1, ck, v, w) VALUES (1, 'a', 1, 100, 1) USING TIMESTAMP 12",
        "!1:INSERT INTO %s.tbl (pk0, pk1, ck, v, w) VALUES (1, 'z', 1, 100, 1) USING TIMESTAMP 13"
        };
        String select = "SELECT pk0, pk1, ck, v, w FROM %s.tbl WHERE v = 100 AND w = 1 ALLOW FILTERING";
        assertTrackedMatchesOracle(name, table, writes, select, 2, (keyspace, oracle) -> {
            assertReadTogetherFromNode1(keyspace, row(1, "n"), row(1, "u"), row(1, "z"), row(1, "a"));
            assertDataReplicaCannotAnswerAlone(keyspace, select, oracle);
        });
    }

    /**
     * The same shape as {@link #indexedRangeReadHandedAKeyBeforeTheNextUnreadMatch} with no ALLOW FILTERING: (1,'n')
     * and (1,'u') are rejected because node 1 missed the updates that moved their {@code v} off 100, so node 1's index
     * still returns them and reconciliation delivers the updates.
     */
    protected static void indexedRangeReadWithStaleEntriesHandedAKeyBeforeTheNextUnreadMatch(String name, String table)
    {
        String[] writes =
        {
        "*:INSERT INTO %s.tbl (pk0, pk1, ck, v, w) VALUES (1, 'n', 1, 100, 1) USING TIMESTAMP 10",
        "*:INSERT INTO %s.tbl (pk0, pk1, ck, v, w) VALUES (1, 'u', 1, 100, 1) USING TIMESTAMP 11",
        "*:INSERT INTO %s.tbl (pk0, pk1, ck, v, w) VALUES (1, 'a', 1, 100, 1) USING TIMESTAMP 12",
        "!1:INSERT INTO %s.tbl (pk0, pk1, ck, v, w) VALUES (1, 'z', 1, 100, 1) USING TIMESTAMP 13",
        "!1:UPDATE %s.tbl USING TIMESTAMP 14 SET v = 200 WHERE pk0 = 1 AND pk1 = 'n' AND ck = 1",
        "!1:UPDATE %s.tbl USING TIMESTAMP 15 SET v = 200 WHERE pk0 = 1 AND pk1 = 'u' AND ck = 1"
        };
        String select = "SELECT pk0, pk1, ck, v, w FROM %s.tbl WHERE v = 100";
        assertTrackedMatchesOracle(name, table, writes, select, 2, (keyspace, oracle) -> {
            assertReadTogetherFromNode1(keyspace, row(1, "n"), row(1, "u"), row(1, "z"), row(1, "a"));
            assertDataReplicaCannotAnswerAlone(keyspace, select, oracle);
        });
    }

    @Test
    public void testIndexedRangeReadHandedAKeyBeforeTheNextUnreadMatch()
    {
        indexedRangeReadHandedAKeyBeforeTheNextUnreadMatch("k_indexed_key_before_unread", SAI_TABLE);
    }

    @Test
    public void testLegacyIndexedRangeReadHandedAKeyBeforeTheNextUnreadMatch()
    {
        indexedRangeReadHandedAKeyBeforeTheNextUnreadMatch("k_legacy_indexed_key_before_unread", LEGACY_TABLE);
    }

    @Test
    public void testIndexedRangeReadWithStaleEntriesHandedAKeyBeforeTheNextUnreadMatch()
    {
        indexedRangeReadWithStaleEntriesHandedAKeyBeforeTheNextUnreadMatch("k_indexed_stale_before_unread", SAI_TABLE);
    }

    @Test
    public void testLegacyIndexedRangeReadWithStaleEntriesHandedAKeyBeforeTheNextUnreadMatch()
    {
        indexedRangeReadWithStaleEntriesHandedAKeyBeforeTheNextUnreadMatch("k_legacy_stale_before_unread", LEGACY_TABLE);
    }
}
