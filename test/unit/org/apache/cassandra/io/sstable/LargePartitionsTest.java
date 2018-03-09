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
package org.apache.cassandra.io.sstable;

import java.util.Iterator;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.metrics.CacheMetrics;
import org.apache.cassandra.service.CacheService;

/**
 * Test intended to manually measure GC pressure to write and read partitions of different size
 * for CASSANDRA-11206.
 */
@RunWith(OrderedJUnit4ClassRunner.class)
@Ignore // all these tests take very, very long - so only run them manually
public class LargePartitionsTest extends CQLTester
{

    @FunctionalInterface
    interface Measured
    {
        void measure() throws Throwable;
    }

    private static void measured(String name, Measured measured) throws Throwable
    {
        long t0 = System.currentTimeMillis();
        measured.measure();
        long t = System.currentTimeMillis() - t0;
        System.out.println("LargePartitionsTest-measured: " + name + " took " + t + " ms");
    }

    private static String randomText(int bytes)
    {
        char[] ch = new char[bytes];
        ThreadLocalRandom r = ThreadLocalRandom.current();
        for (int i = 0; i < bytes; i++)
            ch[i] = (char) (32 + r.nextInt(95));
        return new String(ch);
    }

    private static final int rowKBytes = 8;

    private void withPartitionSize(long partitionKBytes, long totalMBytes) throws Throwable
    {
        long totalKBytes = totalMBytes * 1024L;

        createTable("CREATE TABLE %s (pk text, ck text, val text, PRIMARY KEY (pk, ck))");

        String name = "part=" + partitionKBytes + "k total=" + totalMBytes + 'M';

        measured("INSERTs for " + name, () -> {
            for (long writtenKBytes = 0L; writtenKBytes < totalKBytes; writtenKBytes += partitionKBytes)
            {
                String pk = Long.toBinaryString(writtenKBytes);
                for (long kbytes = 0L; kbytes < partitionKBytes; kbytes += rowKBytes)
                {
                    String ck = Long.toBinaryString(kbytes);
                    execute("INSERT INTO %s (pk, ck, val) VALUES (?,?,?)", pk, ck, randomText(rowKBytes * 1024));
                }
            }
        });

        measured("flush for " + name, () -> flush(true));

        CacheService.instance.keyCache.clear();

        measured("compact for " + name, () -> {
            keyCacheMetrics("before compaction");
            compact();
            keyCacheMetrics("after compaction");
        });

        measured("SELECTs 1 for " + name, () -> selects(partitionKBytes, totalKBytes));

        measured("SELECTs 2 for " + name, () -> selects(partitionKBytes, totalKBytes));

        CacheService.instance.keyCache.clear();
        measured("Scan for " + name, () -> scan(partitionKBytes, totalKBytes));
    }

    private void selects(long partitionKBytes, long totalKBytes) throws Throwable
    {
        for (int i = 0; i < 50000; i++)
        {
            long pk = ThreadLocalRandom.current().nextLong(totalKBytes / partitionKBytes) * partitionKBytes;
            long ck = ThreadLocalRandom.current().nextLong(partitionKBytes / rowKBytes) * rowKBytes;
            execute("SELECT val FROM %s WHERE pk=? AND ck=?",
                    Long.toBinaryString(pk),
                    Long.toBinaryString(ck)).one();
            if (i % 1000 == 0)
                keyCacheMetrics("after " + i + " selects");
        }
        keyCacheMetrics("after all selects");
    }

    private void scan(long partitionKBytes, long totalKBytes) throws Throwable
    {
        long pk = ThreadLocalRandom.current().nextLong(totalKBytes / partitionKBytes) * partitionKBytes;
        Iterator<UntypedResultSet.Row> iter = execute("SELECT val FROM %s WHERE pk=?", Long.toBinaryString(pk)).iterator();
        int i = 0;
        while (iter.hasNext())
        {
            iter.next();
            if (i++ % 1000 == 0)
                keyCacheMetrics("after " + i + " iteration");
        }
        keyCacheMetrics("after all iteration");
    }

    private static void keyCacheMetrics(String title)
    {
        CacheMetrics metrics = CacheService.instance.keyCache.getMetrics();
        System.out.println("Key cache metrics " + title + ": capacity:" + metrics.capacity.getValue() +
                           " size:"+metrics.size.getValue()+
                           " entries:" + metrics.entries.getValue() +
                           " hit-rate:"+metrics.hitRate.getValue() +
                           " one-min-rate:"+metrics.oneMinuteHitRate.getValue());
    }

    @Test
    public void prepare() throws Throwable
    {
        for (int i = 0; i < 4; i++)
        {
            withPartitionSize(8L, 32L);
        }
    }

    @Test
    public void test_01_16k() throws Throwable
    {
        withPartitionSize(16L, 1024L);
    }

    @Test
    public void test_02_512k() throws Throwable
    {
        withPartitionSize(512L, 1024L);
    }

    @Test
    public void test_03_1M() throws Throwable
    {
        withPartitionSize(1024L, 1024L);
    }

    @Test
    public void test_04_4M() throws Throwable
    {
        withPartitionSize(4L * 1024L, 1024L);
    }

    @Test
    public void test_05_8M() throws Throwable
    {
        withPartitionSize(8L * 1024L, 1024L);
    }

    @Test
    public void test_06_16M() throws Throwable
    {
        withPartitionSize(16L * 1024L, 1024L);
    }

    @Test
    public void test_07_32M() throws Throwable
    {
        withPartitionSize(32L * 1024L, 1024L);
    }

    @Test
    public void test_08_64M() throws Throwable
    {
        withPartitionSize(64L * 1024L, 1024L);
    }

    @Test
    public void test_09_256M() throws Throwable
    {
        withPartitionSize(256L * 1024L, 4 * 1024L);
    }

    @Test
    public void test_10_512M() throws Throwable
    {
        withPartitionSize(512L * 1024L, 4 * 1024L);
    }

    @Test
    public void test_11_1G() throws Throwable
    {
        withPartitionSize(1024L * 1024L, 8 * 1024L);
    }

    @Test
    public void test_12_2G() throws Throwable
    {
        withPartitionSize(2L * 1024L * 1024L, 8 * 1024L);
    }

    @Test
    public void test_13_4G() throws Throwable
    {
        withPartitionSize(4L * 1024L * 1024L, 16 * 1024L);
    }

    @Test
    public void test_14_8G() throws Throwable
    {
        withPartitionSize(8L * 1024L * 1024L, 32 * 1024L);
    }
}
