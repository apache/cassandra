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
package org.apache.cassandra.cql3;

import java.util.Random;

import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.io.sstable.IndexHelper;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.ByteBufferUtil;

public class TombstonesWithIndexedSSTableTest extends CQLTester
{
    @Test
    public void testTombstoneBoundariesInIndexCached() throws Throwable
    {
        testTombstoneBoundariesInIndex("ALL");
    }

    @Test
    public void testTombstoneBoundariesInIndexNotCached() throws Throwable
    {
        testTombstoneBoundariesInIndex("NONE");
    }

    public void testTombstoneBoundariesInIndex(String cacheKeys) throws Throwable
    {
        // That test reproduces the bug from CASSANDRA-11158 where a range tombstone boundary in the column index would
        // cause an assertion failure.

        int ROWS = 1000;
        int VALUE_LENGTH = 100;

        createTable("CREATE TABLE %s (k int, t int, s text static, v text, PRIMARY KEY (k, t)) WITH caching = { 'keys' : '" + cacheKeys + "' }");

        // We create a partition that is big enough that the underlying sstable will be indexed
        // For that, we use a large-ish number of row, and a value that isn't too small.
        String text = makeRandomString(VALUE_LENGTH);
        for (int i = 0; i < ROWS; i++)
            execute("INSERT INTO %s(k, t, v) VALUES (?, ?, ?)", 0, i, text + i);

        DecoratedKey dk = Util.dk(ByteBufferUtil.bytes(0));
        int minDeleted = ROWS;
        int maxDeleted = 0;

        // Place some range deletions around an indexed location to get a tombstone boundary as the index's firstName.
        // Because we insert a tombstone before it, the index position may move, so repeat procedure until the index
        // boundary hits a tombstone boundary.
        deletionLoop:
        while (true)
        {
            flush();
            compact();

            int indexedRow = -1;
            for (SSTableReader sstable : getCurrentColumnFamilyStore().getLiveSSTables())
            {
                // The line below failed with key caching off (CASSANDRA-11158)
                @SuppressWarnings("unchecked")
                RowIndexEntry<IndexHelper.IndexInfo> indexEntry = sstable.getPosition(dk, SSTableReader.Operator.EQ);
                if (indexEntry != null && indexEntry.isIndexed())
                {
                    ClusteringPrefix firstName = indexEntry.columnsIndex().get(1).firstName;
                    if (firstName.kind().isBoundary())
                        break deletionLoop;
                    indexedRow = Int32Type.instance.compose(firstName.get(0));
                }
            }
            assert indexedRow >= 0;
            minDeleted = Math.min(minDeleted, indexedRow - 2);
            maxDeleted = Math.max(maxDeleted, indexedRow + 5);

            execute("DELETE FROM %s WHERE k = 0 AND t >= ? AND t < ?", indexedRow - 2, indexedRow + 3);
            execute("DELETE FROM %s WHERE k = 0 AND t >= ? AND t < ?", indexedRow, indexedRow + 5);
        }

        flush();
        // The line below failed with key caching on (CASSANDRA-11158)
        compact();

        assertRowCount(execute("SELECT s FROM %s WHERE k = ?", 0), ROWS - (maxDeleted - minDeleted));
        assertRowCount(execute("SELECT s FROM %s WHERE k = ? ORDER BY t DESC", 0), ROWS - (maxDeleted - minDeleted));

        assertRowCount(execute("SELECT DISTINCT s FROM %s WHERE k = ?", 0), 1);
        assertRowCount(execute("SELECT DISTINCT s FROM %s WHERE k = ? ORDER BY t DESC", 0), 1);
    }

    // Creates a random string
    public static String makeRandomString(int length)
    {
        Random random = new Random();
        char[] chars = new char[length];
        for (int i = 0; i < length; ++i)
            chars[i++] = (char) ('a' + random.nextInt('z' - 'a' + 1));
        return new String(chars);
    }
}
