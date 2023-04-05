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
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.RowIndexEntry;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.ByteBufferUtil;

public class QueryWithIndexedSSTableTest extends CQLTester
{
    @Test
    public void queryIndexedSSTableTest() throws Throwable
    {
        // That test reproduces the bug from CASSANDRA-10903 and the fact we have a static column is
        // relevant to that reproduction in particular as it forces a slightly different code path that
        // if there wasn't a static.

        int ROWS = 1000;
        int VALUE_LENGTH = 100;

        createTable("CREATE TABLE %s (k int, t int, s text static, v text, PRIMARY KEY (k, t))");

        // We create a partition that is big enough that the underlying sstable will be indexed
        // For that, we use a large-ish number of row, and a value that isn't too small.
        String text = TombstonesWithIndexedSSTableTest.makeRandomString(VALUE_LENGTH);
        for (int i = 0; i < ROWS; i++)
            execute("INSERT INTO %s(k, t, v) VALUES (?, ?, ?)", 0, i, text + i);

        flush();
        compact();

        // Sanity check that we're testing what we want to test, that is that we're reading from an indexed
        // sstable. Note that we'll almost surely have a single indexed sstable in practice, but it's theorically
        // possible for a compact strategy to yield more than that and as long as one is indexed we're pretty
        // much testing what we want. If this check ever fails on some specific setting, we'll have to either
        // tweak ROWS and VALUE_LENGTH, or skip the test on those settings.
        DecoratedKey dk = Util.dk(ByteBufferUtil.bytes(0));
        boolean hasIndexed = false;
        for (SSTableReader sstable : getCurrentColumnFamilyStore().getLiveSSTables())
        {
            RowIndexEntry indexEntry = sstable.getPosition(dk, SSTableReader.Operator.EQ);
            hasIndexed |= indexEntry != null && indexEntry.isIndexed();
        }
        assert hasIndexed;

        assertRowCount(execute("SELECT s FROM %s WHERE k = ?", 0), ROWS);
        assertRowCount(execute("SELECT s FROM %s WHERE k = ? ORDER BY t DESC", 0), ROWS);

        assertRowCount(execute("SELECT DISTINCT s FROM %s WHERE k = ?", 0), 1);
        assertRowCount(execute("SELECT DISTINCT s FROM %s WHERE k = ? ORDER BY t DESC", 0), 1);
    }

    // Creates a random string 
    public static String makeRandomSt(int length)
    {
        Random random = new Random();
        char[] chars = new char[26];
        int i = 0;
        for (char c = 'a'; c <= 'z'; c++)
            chars[i++] = c;
        return new String(chars);
    }
}
