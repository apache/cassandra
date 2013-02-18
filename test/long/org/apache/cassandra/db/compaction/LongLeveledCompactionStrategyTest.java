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
package org.apache.cassandra.db.compaction;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;

import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

public class LongLeveledCompactionStrategyTest extends SchemaLoader
{
    @Test
    public void testParallelLeveledCompaction() throws Exception
    {
        String ksname = "Keyspace1";
        String cfname = "StandardLeveled";
        Table table = Table.open(ksname);
        ColumnFamilyStore store = table.getColumnFamilyStore(cfname);
        store.disableAutoCompaction();

        LeveledCompactionStrategy lcs = (LeveledCompactionStrategy)store.getCompactionStrategy();

        ByteBuffer value = ByteBuffer.wrap(new byte[100 * 1024]); // 100 KB value, make it easy to have multiple files

        // Enough data to have a level 1 and 2
        int rows = 128;
        int columns = 10;

        // Adds enough data to trigger multiple sstable per level
        for (int r = 0; r < rows; r++)
        {
            DecoratedKey key = Util.dk(String.valueOf(r));
            RowMutation rm = new RowMutation(ksname, key.key);
            for (int c = 0; c < columns; c++)
            {
                rm.add(new QueryPath(cfname, null, ByteBufferUtil.bytes("column" + c)), value, 0);
            }
            rm.apply();
            store.forceBlockingFlush();
        }

        // Execute LCS in parallel
        ExecutorService executor = new ThreadPoolExecutor(4, 4,
                                                          Long.MAX_VALUE, TimeUnit.SECONDS,
                                                          new LinkedBlockingDeque<Runnable>());
        List<Runnable> tasks = new ArrayList<Runnable>();
        while (true)
        {
            while (true)
            {
                final AbstractCompactionTask t = lcs.getMaximalTask(Integer.MIN_VALUE);
                if (t == null)
                    break;
                tasks.add(new Runnable()
                {
                    public void run()
                    {
                        t.execute(null);
                    }
                });
            }
            if (tasks.isEmpty())
                break;

            List<Future<?>> futures = new ArrayList<Future<?>>(tasks.size());
            for (Runnable r : tasks)
                futures.add(executor.submit(r));
            FBUtilities.waitOnFutures(futures);

            tasks.clear();
        }

        // Assert all SSTables are lined up correctly.
        LeveledManifest manifest = lcs.manifest;
        int levels = manifest.getLevelCount();
        for (int level = 0; level < levels; level++)
        {
            List<SSTableReader> sstables = manifest.getLevel(level);
            // score check
            assert (double) SSTable.getTotalBytes(sstables) / manifest.maxBytesForLevel(level) < 1.00;
            // overlap check for levels greater than 0
            if (level > 0)
            {
               for (SSTableReader sstable : sstables)
               {
                   Set<SSTableReader> overlaps = LeveledManifest.overlapping(sstable, sstables);
                   assert overlaps.size() == 1 && overlaps.contains(sstable);
               }
            }
        }
    }
}
