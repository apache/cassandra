/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.db;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;

import org.apache.cassandra.Util;

import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import static junit.framework.Assert.assertEquals;

public class CompactionsTest extends CleanupHelper
{
    public static final String TABLE1 = "Keyspace1";
    public static final String TABLE2 = "Keyspace2";
    public static final InetAddress LOCAL = FBUtilities.getLocalAddress();

    @Test
    public void testCompactions() throws IOException, ExecutionException, InterruptedException
    {
        CompactionManager.instance.disableAutoCompaction();

        // this test does enough rows to force multiple block indexes to be used
        Table table = Table.open(TABLE1);
        ColumnFamilyStore store = table.getColumnFamilyStore("Standard1");

        final int ROWS_PER_SSTABLE = 10;
        Set<DecoratedKey> inserted = new HashSet<DecoratedKey>();
        for (int j = 0; j < (DatabaseDescriptor.getIndexInterval() * 3) / ROWS_PER_SSTABLE; j++) {
            for (int i = 0; i < ROWS_PER_SSTABLE; i++) {
                DecoratedKey key = Util.dk(String.valueOf(i % 2));
                RowMutation rm = new RowMutation(TABLE1, key.key);
                rm.add(new QueryPath("Standard1", null, ByteBuffer.wrap(String.valueOf(i / 2).getBytes())), ByteBufferUtil.EMPTY_BYTE_BUFFER, j * ROWS_PER_SSTABLE + i);
                rm.apply();
                inserted.add(key);
            }
            store.forceBlockingFlush();
            assertEquals(inserted.toString(), inserted.size(), Util.getRangeSlice(store).size());
        }
        while (true)
        {
            Future<Integer> ft = CompactionManager.instance.submitMinorIfNeeded(store);
            if (ft.get() == 0)
                break;
        }
        if (store.getSSTables().size() > 1)
        {
            CompactionManager.instance.performMajor(store);
        }
        assertEquals(inserted.size(), Util.getRangeSlice(store).size());
    }

    @Test
    public void testGetBuckets()
    {
        List<Pair<String, Long>> pairs = new ArrayList<Pair<String, Long>>();
        String[] strings = { "a", "bbbb", "cccccccc", "cccccccc", "bbbb", "a" };
        for (String st : strings)
        {
            Pair<String, Long> pair = new Pair<String, Long>(st, new Long(st.length()));
            pairs.add(pair);
        }

        Set<List<String>> buckets = CompactionManager.getBuckets(pairs, 2);
        assertEquals(3, buckets.size());

        for (List<String> bucket : buckets)
        {
            assertEquals(2, bucket.size());
            assertEquals(bucket.get(0).length(), bucket.get(1).length());
            assertEquals(bucket.get(0).charAt(0), bucket.get(1).charAt(0));
        }

        pairs.clear();
        buckets.clear();

        String[] strings2 = { "aaa", "bbbbbbbb", "aaa", "bbbbbbbb", "bbbbbbbb", "aaa" };
        for (String st : strings2)
        {
            Pair<String, Long> pair = new Pair<String, Long>(st, new Long(st.length()));
            pairs.add(pair);
        }

        buckets = CompactionManager.getBuckets(pairs, 2);
        assertEquals(2, buckets.size());

        for (List<String> bucket : buckets)
        {
            assertEquals(3, bucket.size());
            assertEquals(bucket.get(0).charAt(0), bucket.get(1).charAt(0));
            assertEquals(bucket.get(1).charAt(0), bucket.get(2).charAt(0));
        }

        // Test the "min" functionality
        pairs.clear();
        buckets.clear();

        String[] strings3 = { "aaa", "bbbbbbbb", "aaa", "bbbbbbbb", "bbbbbbbb", "aaa" };
        for (String st : strings3)
        {
            Pair<String, Long> pair = new Pair<String, Long>(st, new Long(st.length()));
            pairs.add(pair);
        }

        buckets = CompactionManager.getBuckets(pairs, 10); // notice the min is 10
        assertEquals(1, buckets.size());
    }

}
