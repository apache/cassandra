/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.db.compaction.unified;

import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.db.compaction.BackgroundCompactions;
import org.apache.cassandra.db.compaction.UnifiedCompactionStrategy;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FixedMonotonicClock;
import org.apache.cassandra.utils.MovingAverage;
import org.apache.cassandra.utils.PageAware;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

public class CostsCalculatorTest
{
    private static final double epsilon = 0.00000001;
    private static final Random random = new Random(0L);
    private static final double survivalFactor = 1;

    @Mock
    private Environment environment;

    @Mock
    private UnifiedCompactionStrategy strategy;

    @Mock
    private TableMetadata metadata;

    @Mock
    private BackgroundCompactions backgroundCompactions;

    @Mock
    private ScheduledExecutorService executorService;

    @Mock
    private ScheduledFuture fut;

    @Mock
    private SSTableReader sstable;

    private FixedMonotonicClock clock;

    @Before
    public void setUp()
    {
        MockitoAnnotations.initMocks(this);

        when(executorService.scheduleAtFixedRate(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class))).thenReturn(fut);
        when(strategy.getSSTables()).thenReturn(Sets.newHashSet(sstable));
        when(strategy.getMetadata()).thenReturn(metadata);

        when(sstable.onDiskLength()).thenReturn((long) PageAware.PAGE_SIZE);
        when(backgroundCompactions.getAggregates()).thenReturn(ImmutableList.of());

        clock = new FixedMonotonicClock();
        when(environment.makeExpMovAverage()).thenAnswer(ts -> new MovingAverageMock());
    }

    @Test
    public void testCreateAndClose()
    {
        CostsCalculator cost = new CostsCalculator(environment, strategy, executorService, survivalFactor);
        assertNotNull(cost);
        assertNotNull(cost.toString());

        Mockito.verify(executorService, times(1)).scheduleAtFixedRate(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class));

        cost.close();
        Mockito.verify(fut, times(1)).cancel(anyBoolean());
    }

    @Test
    public void testUpdate() throws InterruptedException
    {
        testCosts(100, 100, PageAware.PAGE_SIZE, 1, 1, 1, 0.01, survivalFactor);
    }

    @Test
    public void testDoubleReadTime() throws InterruptedException
    {
        testCosts(200, 100, PageAware.PAGE_SIZE, 1, 1, 1, 0.01, survivalFactor);
    }

    @Test
    public void testDoubleWriteTime() throws InterruptedException
    {
        testCosts(100, 200, PageAware.PAGE_SIZE, 1, 1, 1, 0.01, survivalFactor);
    }

    @Test
    public void testLargerChunkSize() throws InterruptedException
    {
        testCosts(100, 100, 64 << 10, 1, 1, 1, 0.01, survivalFactor);
    }

    @Test
    public void testHalfCacheMissRatio() throws InterruptedException
    {
        testCosts(100, 100, PageAware.PAGE_SIZE, 0.5, 1, 1, 0.01, survivalFactor);
    }

    @Test
    public void testReadMultiplier() throws InterruptedException
    {
        testCosts(1000, 100, PageAware.PAGE_SIZE, 1, 0.1, 1, 0.01, survivalFactor);
    }

    @Test
    public void testWriteMultiplier() throws InterruptedException
    {
        testCosts(100, 100, PageAware.PAGE_SIZE, 1, 1, 10, 0.01, survivalFactor);
    }

    @Test
    public void testSurvivalRatio() throws InterruptedException
    {
        testCosts(100, 100, PageAware.PAGE_SIZE, 1, 1, 1, 0.01, 0.5);
    }

    private void testCosts(long readTimeMicros,
                           long writeTimeMicros,
                           int chunkSize,
                           double cacheMissRatio,
                           double readMultiplier,
                           double writeMultiplier,
                           double bfprRatio,
                           double survivalFactor) throws InterruptedException
    {
        int blockSize = PageAware.PAGE_SIZE;
        long totPartitionsRead = 1 + random.nextInt(32);
        long totBytesInserted = blockSize + random(blockSize);

        when(environment.partitionsRead()).thenReturn(totPartitionsRead);
        when(environment.bytesInserted()).thenReturn(totBytesInserted);
        when(environment.chunkSize()).thenReturn(chunkSize);
        when(environment.cacheMissRatio()).thenReturn(cacheMissRatio);
        when(environment.bloomFilterFpRatio()).thenReturn(bfprRatio);
        when(environment.sstablePartitionReadLatencyNanos()).thenReturn((double) TimeUnit.MICROSECONDS.toNanos(readTimeMicros));
        when(environment.flushLatencyPerKbInNanos()).thenReturn((double) TimeUnit.MICROSECONDS.toNanos(writeTimeMicros));
        when(environment.compactionLatencyPerKbInNanos()).thenReturn((double) TimeUnit.MICROSECONDS.toNanos(writeTimeMicros));

        CostsCalculator cost = new CostsCalculator(environment, strategy, executorService, survivalFactor, readMultiplier, writeMultiplier);
        assertNotNull(cost);
        assertNotNull(cost.toString());

        cost.sampleValues();
        assertNotNull(cost.toString());

        for (int i = 0; i < 32; i++)
        {
            long bytesInserted = (i * blockSize + random(blockSize));
            totPartitionsRead += (1 + i);
            totBytesInserted += bytesInserted;

            when(environment.partitionsRead()).thenReturn(totPartitionsRead);
            when(environment.bytesInserted()).thenReturn(totBytesInserted);

            clock.setNowInNanos(clock.now() + TimeUnit.MILLISECONDS.toNanos(CostsCalculator.samplingPeriodMs));
            cost.sampleValues();
            assertNotNull(cost.toString());

            // the WA is 2 and the flush and compaction times for now are the same and equal to writeTimeMicros
            double writeCost = ((bytesInserted / (double) (1 << 10)) * TimeUnit.MICROSECONDS.toNanos(writeTimeMicros)) / (double) TimeUnit.MILLISECONDS.toNanos(1);
            assertEquals((writeCost + writeCost * 2) * writeMultiplier, cost.getWriteCostForQueries(2), epsilon);

            // the RA is 2, the delta partitions read is i + 1
            assertEquals((((i + 1) * readTimeMicros) / (double) TimeUnit.MILLISECONDS.toMicros(1)) * Math.min(1 + bfprRatio * 2 / survivalFactor, 2) * readMultiplier, cost.getReadCostForQueries(2), epsilon);
        }
    }

    @Test
    public void testNoBytesInserted()
    {
        int blockSize = PageAware.PAGE_SIZE;
        long totPartitionsRead = 1 + random.nextInt(32);
        long totBytesInserted = blockSize + random(blockSize);

        when(environment.partitionsRead()).thenReturn(totPartitionsRead);
        when(environment.bytesInserted()).thenReturn(totBytesInserted);
        when(environment.chunkSize()).thenReturn(4096);
        when(environment.cacheMissRatio()).thenReturn(0.05);
        when(environment.bloomFilterFpRatio()).thenReturn(0.01);
        when(environment.sstablePartitionReadLatencyNanos()).thenReturn((double) TimeUnit.MICROSECONDS.toNanos(20));
        when(environment.flushLatencyPerKbInNanos()).thenReturn((double) TimeUnit.MICROSECONDS.toNanos(20));
        when(environment.compactionLatencyPerKbInNanos()).thenReturn((double) TimeUnit.MICROSECONDS.toNanos(20));

        CostsCalculator cost = new CostsCalculator(environment, strategy, executorService, survivalFactor, 1, 1);
        assertNotNull(cost);
        assertNotNull(cost.toString());

        cost.sampleValues();
        assertNotNull(cost.toString());

        when(environment.bytesInserted()).thenReturn(0L);
        for (int i = 0; i < 10; i++)
            assertEquals(0, cost.getWriteCostForQueries(i), epsilon);
    }

    @Test
    public void testNoPartitionsRead()
    {
        int blockSize = PageAware.PAGE_SIZE;
        long totPartitionsRead = 1 + random.nextInt(32);
        long totBytesInserted = blockSize + random(blockSize);

        when(environment.partitionsRead()).thenReturn(totPartitionsRead);
        when(environment.bytesInserted()).thenReturn(totBytesInserted);
        when(environment.chunkSize()).thenReturn(4096);
        when(environment.cacheMissRatio()).thenReturn(0.05);
        when(environment.bloomFilterFpRatio()).thenReturn(0.01);
        when(environment.sstablePartitionReadLatencyNanos()).thenReturn((double) TimeUnit.MICROSECONDS.toNanos(20));
        when(environment.flushLatencyPerKbInNanos()).thenReturn((double) TimeUnit.MICROSECONDS.toNanos(20));
        when(environment.compactionLatencyPerKbInNanos()).thenReturn((double) TimeUnit.MICROSECONDS.toNanos(20));

        CostsCalculator cost = new CostsCalculator(environment, strategy, executorService, survivalFactor, 1, 1);
        assertNotNull(cost);
        assertNotNull(cost.toString());

        cost.sampleValues();
        assertNotNull(cost.toString());

        when(environment.partitionsRead()).thenReturn(0L);
        for (int i = 0; i < 10; i++)
            assertEquals(0, cost.getReadCostForQueries(i), epsilon);
    }

    private static long random(int blockSize)
    {
        return 1 + random.nextInt(blockSize - 1);
    }

    private static class MovingAverageMock implements MovingAverage
    {
        private double val = 0;

        @Override
        public MovingAverage update(double val)
        {
            this.val = val;
            return this;
        }

        @Override
        public double get()
        {
            return val;
        }

        @Override
        public String toString()
        {
            return String.format("%.02f", val);
        }
    }
}
