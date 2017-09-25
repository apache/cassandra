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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

import com.google.common.collect.Iterables;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import junit.framework.Assert;
import org.apache.cassandra.config.Config.CommitLogSync;
import org.apache.cassandra.config.Config.DiskAccessMode;
import org.apache.cassandra.cache.ChunkCache;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.FBUtilities;

public class CachingBench extends CQLTester
{
    private static final String STRATEGY = "LeveledCompactionStrategy";

    private static final int DEL_SECTIONS = 1000;
    private static final int FLUSH_FREQ = 10000;
    private static final int SCAN_FREQUENCY_INV = 12000;
    static final int COUNT = 29000;
    static final int ITERS = 9;

    static final int KEY_RANGE = 30;
    static final int CLUSTERING_RANGE = 210000;

    static final int EXTRA_SIZE = 1025;
    static final boolean CONCURRENT_COMPACTIONS = true;

    // The name of this method is important!
    // CommitLog settings must be applied before CQLTester sets up; by using the same name as its @BeforeClass method we
    // are effectively overriding it.
    @BeforeClass
    public static void setUpClass()
    {
        DatabaseDescriptor.setCommitLogSync(CommitLogSync.periodic);
        DatabaseDescriptor.setCommitLogSyncPeriod(100);
        CQLTester.setUpClass();
    }
    
    String hashQuery;

    @Before
    public void before() throws Throwable
    {
        createTable("CREATE TABLE %s(" +
                    "  key int," +
                    "  column int," +
                    "  data int," +
                    "  extra text," +
                    "  PRIMARY KEY(key, column)" +
                    ")"
                   );

        String hashIFunc = parseFunctionName(createFunction(KEYSPACE, "int, int",
                " CREATE FUNCTION %s (state int, val int)" +
                " CALLED ON NULL INPUT" +
                " RETURNS int" +
                " LANGUAGE java" +
                " AS 'return val != null ? state * 17 + val : state;'")).name;
        String hashTFunc = parseFunctionName(createFunction(KEYSPACE, "int, text",
                " CREATE FUNCTION %s (state int, val text)" +
                " CALLED ON NULL INPUT" +
                " RETURNS int" +
                " LANGUAGE java" +
                " AS 'return val != null ? state * 17 + val.hashCode() : state;'")).name;

        String hashInt = createAggregate(KEYSPACE, "int",
                " CREATE AGGREGATE %s (int)" +
                " SFUNC " + hashIFunc +
                " STYPE int" +
                " INITCOND 1");
        String hashText = createAggregate(KEYSPACE, "text",
                " CREATE AGGREGATE %s (text)" +
                " SFUNC " + hashTFunc +
                " STYPE int" +
                " INITCOND 1");

        hashQuery = String.format("SELECT count(column), %s(key), %s(column), %s(data), %s(extra), avg(key), avg(column), avg(data) FROM %%s",
                                  hashInt, hashInt, hashInt, hashText);
    }
    AtomicLong id = new AtomicLong();
    long compactionTimeNanos = 0;

    void pushData(Random rand, int count) throws Throwable
    {
        for (int i = 0; i < count; ++i)
        {
            long ii = id.incrementAndGet();
            if (ii % 1000 == 0)
                System.out.print('.');
            int key = rand.nextInt(KEY_RANGE);
            int column = rand.nextInt(CLUSTERING_RANGE);
            execute("INSERT INTO %s (key, column, data, extra) VALUES (?, ?, ?, ?)", key, column, (int) ii, genExtra(rand));
            maybeCompact(ii);
        }
    }

    private String genExtra(Random rand)
    {
        StringBuilder builder = new StringBuilder(EXTRA_SIZE);
        for (int i = 0; i < EXTRA_SIZE; ++i)
            builder.append((char) ('a' + rand.nextInt('z' - 'a' + 1)));
        return builder.toString();
    }

    void readAndDelete(Random rand, int count) throws Throwable
    {
        for (int i = 0; i < count; ++i)
        {
            int key;
            UntypedResultSet res;
            long ii = id.incrementAndGet();
            if (ii % 1000 == 0)
                System.out.print('-');
            if (rand.nextInt(SCAN_FREQUENCY_INV) != 1)
            {
                do
                {
                    key = rand.nextInt(KEY_RANGE);
                    long cid = rand.nextInt(DEL_SECTIONS);
                    int cstart = (int) (cid * CLUSTERING_RANGE / DEL_SECTIONS);
                    int cend = (int) ((cid + 1) * CLUSTERING_RANGE / DEL_SECTIONS);
                    res = execute("SELECT column FROM %s WHERE key = ? AND column >= ? AND column < ? LIMIT 1", key, cstart, cend);
                } while (res.size() == 0);
                UntypedResultSet.Row r = Iterables.get(res, rand.nextInt(res.size()));
                int clustering = r.getInt("column");
                execute("DELETE FROM %s WHERE key = ? AND column = ?", key, clustering);
            }
            else
            {
                execute(hashQuery);
            }
            maybeCompact(ii);
        }
    }

    private void maybeCompact(long ii)
    {
        if (ii % FLUSH_FREQ == 0)
        {
            System.out.print("F");
            flush();
            if (ii % (FLUSH_FREQ * 10) == 0)
            {
                System.out.println("C");
                long startTime = System.nanoTime();
                getCurrentColumnFamilyStore().enableAutoCompaction(!CONCURRENT_COMPACTIONS);
                long endTime = System.nanoTime();
                compactionTimeNanos += endTime - startTime;
                getCurrentColumnFamilyStore().disableAutoCompaction();
            }
        }
    }

    public void testSetup(String compactionClass, String compressorClass, DiskAccessMode mode, boolean cacheEnabled) throws Throwable
    {
        id.set(0);
        compactionTimeNanos = 0;
        ChunkCache.instance.enable(cacheEnabled);
        DatabaseDescriptor.setDiskAccessMode(mode);
        alterTable("ALTER TABLE %s WITH compaction = { 'class' :  '" + compactionClass + "'  };");
        alterTable("ALTER TABLE %s WITH compression = { 'sstable_compression' : '" + compressorClass + "'  };");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        long onStartTime = System.currentTimeMillis();
        ExecutorService es = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        List<Future<?>> tasks = new ArrayList<>();
        for (int ti = 0; ti < 1; ++ti)
        {
            Random rand = new Random(ti);
            tasks.add(es.submit(() -> 
            {
                for (int i = 0; i < ITERS; ++i)
                    try
                    {
                        pushData(rand, COUNT);
                        readAndDelete(rand, COUNT / 3);
                    }
                    catch (Throwable e)
                    {
                        throw new AssertionError(e);
                    }
            }));
        }
        for (Future<?> task : tasks)
            task.get();

        flush();
        long onEndTime = System.currentTimeMillis();
        int startRowCount = countRows(cfs);
        int startTombCount = countTombstoneMarkers(cfs);
        int startRowDeletions = countRowDeletions(cfs);
        int startTableCount = cfs.getLiveSSTables().size();
        long startSize = SSTableReader.getTotalBytes(cfs.getLiveSSTables());
        System.out.println("\nCompession: " + cfs.getCompressionParameters().toString());
        System.out.println("Reader " + cfs.getLiveSSTables().iterator().next().getFileDataInput(0).toString());
        if (cacheEnabled)
            System.out.format("Cache size %s requests %,d hit ratio %f\n",
                FileUtils.stringifyFileSize(ChunkCache.instance.metrics.size.getValue()),
                ChunkCache.instance.metrics.requests.getCount(),
                ChunkCache.instance.metrics.hitRate.getValue());
        else
        {
            Assert.assertTrue("Chunk cache had requests: " + ChunkCache.instance.metrics.requests.getCount(), ChunkCache.instance.metrics.requests.getCount() < COUNT);
            System.out.println("Cache disabled");
        }
        System.out.println(String.format("Operations completed in %.3fs", (onEndTime - onStartTime) * 1e-3));
        if (!CONCURRENT_COMPACTIONS)
            System.out.println(String.format(", out of which %.3f for non-concurrent compaction", compactionTimeNanos * 1e-9));
        else
            System.out.println();

        String hashesBefore = getHashes();
        long startTime = System.currentTimeMillis();
        CompactionManager.instance.performMaximal(cfs, true);
        long endTime = System.currentTimeMillis();

        int endRowCount = countRows(cfs);
        int endTombCount = countTombstoneMarkers(cfs);
        int endRowDeletions = countRowDeletions(cfs);
        int endTableCount = cfs.getLiveSSTables().size();
        long endSize = SSTableReader.getTotalBytes(cfs.getLiveSSTables());

        System.out.println(String.format("Major compaction completed in %.3fs",
                (endTime - startTime) * 1e-3));
        System.out.println(String.format("At start: %,12d tables %12s %,12d rows %,12d deleted rows %,12d tombstone markers",
                startTableCount, FileUtils.stringifyFileSize(startSize), startRowCount, startRowDeletions, startTombCount));
        System.out.println(String.format("At end:   %,12d tables %12s %,12d rows %,12d deleted rows %,12d tombstone markers",
                endTableCount, FileUtils.stringifyFileSize(endSize), endRowCount, endRowDeletions, endTombCount));
        String hashesAfter = getHashes();

        Assert.assertEquals(hashesBefore, hashesAfter);
    }

    private String getHashes() throws Throwable
    {
        long startTime = System.currentTimeMillis();
        String hashes = Arrays.toString(getRows(execute(hashQuery))[0]);
        long endTime = System.currentTimeMillis();
        System.out.println(String.format("Hashes: %s, retrieved in %.3fs", hashes, (endTime - startTime) * 1e-3));
        return hashes;
    }

    @Test
    public void testWarmup() throws Throwable
    {
        testSetup(STRATEGY, "LZ4Compressor", DiskAccessMode.mmap, false);
    }

    @Test
    public void testLZ4CachedMmap() throws Throwable
    {
        testSetup(STRATEGY, "LZ4Compressor", DiskAccessMode.mmap, true);
    }

    @Test
    public void testLZ4CachedStandard() throws Throwable
    {
        testSetup(STRATEGY, "LZ4Compressor", DiskAccessMode.standard, true);
    }

    @Test
    public void testLZ4UncachedMmap() throws Throwable
    {
        testSetup(STRATEGY, "LZ4Compressor", DiskAccessMode.mmap, false);
    }

    @Test
    public void testLZ4UncachedStandard() throws Throwable
    {
        testSetup(STRATEGY, "LZ4Compressor", DiskAccessMode.standard, false);
    }

    @Test
    public void testCachedStandard() throws Throwable
    {
        testSetup(STRATEGY, "", DiskAccessMode.standard, true);
    }

    @Test
    public void testUncachedStandard() throws Throwable
    {
        testSetup(STRATEGY, "", DiskAccessMode.standard, false);
    }

    @Test
    public void testMmapped() throws Throwable
    {
        testSetup(STRATEGY, "", DiskAccessMode.mmap, false /* doesn't matter */);
    }

    int countTombstoneMarkers(ColumnFamilyStore cfs)
    {
        return count(cfs, x -> x.isRangeTombstoneMarker());
    }

    int countRowDeletions(ColumnFamilyStore cfs)
    {
        return count(cfs, x -> x.isRow() && !((Row) x).deletion().isLive());
    }

    int countRows(ColumnFamilyStore cfs)
    {
        boolean enforceStrictLiveness = cfs.metadata.enforceStrictLiveness();
        int nowInSec = FBUtilities.nowInSeconds();
        return count(cfs, x -> x.isRow() && ((Row) x).hasLiveData(nowInSec, enforceStrictLiveness));
    }

    private int count(ColumnFamilyStore cfs, Predicate<Unfiltered> predicate)
    {
        int count = 0;
        for (SSTableReader reader : cfs.getLiveSSTables())
            count += count(reader, predicate);
        return count;
    }

    int count(SSTableReader reader, Predicate<Unfiltered> predicate)
    {
        int instances = 0;
        try (ISSTableScanner partitions = reader.getScanner())
        {
            while (partitions.hasNext())
            {
                try (UnfilteredRowIterator iter = partitions.next())
                {
                    while (iter.hasNext())
                    {
                        Unfiltered atom = iter.next();
                        if (predicate.test(atom))
                            ++instances;
                    }
                }
            }
        }
        return instances;
    }
}
