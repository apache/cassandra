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
import java.util.*;
import java.util.concurrent.*;

import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.utils.FBUtilities;

public class LongLeveledCompactionStrategyTest
{
    public static final String KEYSPACE1 = "LongLeveledCompactionStrategyTest";
    public static final String CF_STANDARDLVL = "StandardLeveled";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        Map<String, String> leveledOptions = new HashMap<>();
        leveledOptions.put("sstable_size_in_mb", "1");
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    SimpleStrategy.class,
                                    KSMetaData.optsWithRF(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARDLVL)
                                                .compactionStrategyClass(LeveledCompactionStrategy.class)
                                                .compactionStrategyOptions(leveledOptions));
    }

    @Test
    public void testParallelLeveledCompaction() throws Exception
    {
        String ksname = KEYSPACE1;
        String cfname = "StandardLeveled";
        Keyspace keyspace = Keyspace.open(ksname);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore(cfname);
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
            Mutation rm = new Mutation(ksname, key.getKey());
            for (int c = 0; c < columns; c++)
            {
                rm.add(cfname, Util.cellname("column" + c), value, 0);
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
                final AbstractCompactionTask t = lcs.getMaximalTask(Integer.MIN_VALUE).iterator().next();
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
            assert (double) SSTableReader.getTotalBytes(sstables) / manifest.maxBytesForLevel(level) < 1.00;
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
        for (SSTableReader sstable : store.getSSTables())
        {
            assert sstable.getSSTableLevel() == sstable.getSSTableLevel();
        }
    }
}
