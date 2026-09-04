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

package org.apache.cassandra.db.compaction.differential;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;

/**
 * Compacts large multi-cell collections through the cursor path.
 *
 * The other complex-column tests in this suite use small collections. A small collection fits the
 * reader's 32-byte path buffer, the compactor's 4KB copy buffer, and one 16KB compression block.
 * The sizes here pass all three limits.
 */
public class LargeCollectionDifferentialCompactionTest extends DifferentialCompactionTester
{
    private static final int MAP_ELEMENTS = 1200;
    private static final int STATIC_ELEMENTS = 300;
    private static final String KEY_PADDING = "k".repeat(110);

    private static String key(int i)
    {
        return KEY_PADDING + '_' + i;
    }

    private static Map<String, String> bigMap(int elements, int salt)
    {
        Map<String, String> m = new LinkedHashMap<>();
        for (int i = 0; i < elements; i++)
            m.put(key(i), "v" + salt + '_' + i);
        return m;
    }

    @Test
    public void largeCollectionsAcrossSSTables() throws Exception
    {
        String udt = createType("CREATE TYPE %s (a int, b text)");
        createTable("CREATE TABLE %s (pk bigint, ck bigint, m map<text, text>, l list<text>, s set<text>, " +
                    "fu frozen<" + udt + ">, fs frozen<set<int>>, v text, " +
                    "sm map<text, text> static, PRIMARY KEY (pk, ck)) WITH gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        // sstable 1 holds the large collections. The empty set element gives a cell path of zero
        // length. The 80KB map value crosses the copy buffer and the compression block.
        List<String> bigList = new ArrayList<>();
        for (int i = 0; i < 400; i++)
            bigList.add("elem" + i);
        for (long ck : new long[]{ 0, 1, 4, 5 })
        {
            Map<String, String> m = bigMap(MAP_ELEMENTS, (int) ck);
            m.put("", "empty-key");
            m.put("huge", "H".repeat(80 * 1024));
            m.put("zero", "");
            execute("INSERT INTO %s (pk, ck, m, l, s, fu, fs, v) VALUES (?, ?, ?, ?, ?, {a: ?, b: ?}, ?, ?)",
                    0L, ck, m, bigList, set("", "one", "two"), (int) ck, "f" + ck, set(1, 2, 3), "v" + ck);
        }
        execute("UPDATE %s SET sm = ? WHERE pk = ?", bigMap(STATIC_ELEMENTS, 9), 0L);
        execute("INSERT INTO %s (pk, ck, m, v) VALUES (?, ?, ?, ?)", 1L, 0L, bigMap(200, 7), "doomed");
        flush();

        // sstable 2 mixes live and deleted elements into the collections of pk=0. The list
        // prepend sorts before the elements of sstable 1. The append sorts after them. The merge
        // therefore takes cells from both sstables in turn. List cell paths are timeuuids, which
        // sort by time, not by bytes.
        for (int i = 0; i < MAP_ELEMENTS; i += 30)
            execute("DELETE m[?] FROM %s WHERE pk = ? AND ck = ?", key(i), 0L, 0L);
        for (int i = 15; i < MAP_ELEMENTS; i += 90)
            execute("UPDATE %s SET m[?] = ? WHERE pk = ? AND ck = ?", key(i), "overwritten" + i, 0L, 0L);
        execute("UPDATE %s SET l = ? + l WHERE pk = ? AND ck = ?", list("pre0", "pre1", "pre2"), 0L, 0L);
        execute("UPDATE %s SET l = l + ? WHERE pk = ? AND ck = ?", list("post0", "post1"), 0L, 0L);
        for (int i = 0; i < STATIC_ELEMENTS; i += 40)
            execute("DELETE sm[?] FROM %s WHERE pk = ?", key(i), 0L);
        execute("UPDATE %s SET sm[?] = ? WHERE pk = ?", key(5), "static-overwrite", 0L);
        flush();

        // sstable 3 holds the large tombstones. The map overwrite of ck=1 gives one complex
        // deletion that shadows about 1200 cells from sstable 1. The empty set assignment gives
        // a complex deletion with no cells.
        execute("UPDATE %s SET m = ? WHERE pk = ? AND ck = ?", map("survivor", "yes"), 0L, 1L);
        execute("UPDATE %s SET s = {} WHERE pk = ? AND ck = ?", 0L, 4L);
        execute("DELETE FROM %s WHERE pk = ? AND ck >= ?", 0L, 5L);
        execute("DELETE FROM %s WHERE pk = ?", 1L);
        flush();

        assertCursorMatchesIteratorAcrossGenerations(cfs);
    }
}
