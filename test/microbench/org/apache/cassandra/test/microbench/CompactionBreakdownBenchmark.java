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
package org.apache.cassandra.test.microbench;


import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.compaction.CompactionController;
import org.apache.cassandra.db.compaction.CompactionCursor;
import org.apache.cassandra.db.compaction.CompactionIterator;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.CompactionStrategy;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.compaction.writers.CompactionAwareWriter;
import org.apache.cassandra.db.compaction.writers.DefaultCompactionWriter;
import org.apache.cassandra.db.compaction.writers.SSTableDataSink;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.ScannerList;
import org.apache.cassandra.io.sstable.compaction.SortedStringTableCursor;
import org.apache.cassandra.io.sstable.compaction.IteratorFromCursor;
import org.apache.cassandra.io.sstable.compaction.SSTableCursor;
import org.apache.cassandra.io.sstable.compaction.SSTableCursorMerger;
import org.apache.cassandra.io.sstable.format.PartitionIndexIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.FBUtilities;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.RateLimiter;

public class CompactionBreakdownBenchmark extends BaseCompactionBenchmark
{
    @Benchmark
    public void iterateThroughTableScanner(Blackhole bh) throws Throwable
    {
        long totalRows = 0;
        Iterable<SSTableReader> ssTables = cfs.getSSTables(SSTableSet.LIVE);
        for (SSTableReader reader : ssTables)
        {
            try (ISSTableScanner scanner = reader.getScanner())
            {
                totalRows = consumePartitionIterator(scanner, bh, totalRows);
            }
        }
        if (totalRows != sstableCount * rowsPerSSTable)
            throw new IllegalStateException("Total rows is: " + totalRows + " but should be: " + rowsPerSSTable * sstableCount);
    }

    @Benchmark
    public void iterateThroughCursorToIterator(Blackhole bh) throws Throwable
    {
        long totalRows = 0;
        Iterable<SSTableReader> ssTables = cfs.getSSTables(SSTableSet.LIVE);
        for (SSTableReader reader : ssTables)
        {
            try (UnfilteredPartitionIterator iter = new IteratorFromCursor(reader.metadata(), new SortedStringTableCursor(reader)))
            {
                totalRows = consumePartitionIterator(iter, bh, totalRows);
            }
        }
        if (totalRows != sstableCount * rowsPerSSTable)
            throw new IllegalStateException("Total rows is: " + totalRows + " but should be: " + rowsPerSSTable * sstableCount);
    }

    @Benchmark
    public void iterateThroughCursor(Blackhole bh) throws Throwable
    {
        long totalRows = 0;
        Iterable<SSTableReader> ssTables = cfs.getSSTables(SSTableSet.LIVE);
        for (SSTableReader reader : ssTables)
        {
            try (SSTableCursor cursor = new SortedStringTableCursor(reader))
            {
                totalRows = consumeCursor(cursor, bh, totalRows);
            }
        }
        if (totalRows != sstableCount * rowsPerSSTable)
            throw new IllegalStateException("Total rows is: " + totalRows + " but should be: " + rowsPerSSTable * sstableCount);
    }

    @Benchmark
    public void iterateThroughMergeCursor(Blackhole bh) throws Throwable
    {
        long totalRows = 0;
        Set<SSTableReader> ssTables = cfs.getLiveSSTables();
        try (SSTableCursor cursor = new SSTableCursorMerger(ssTables.stream()
                                                                    .map(SortedStringTableCursor::new)
                                                                    .collect(Collectors.toList()),
                                                            cfs.metadata());
        )
        {
            totalRows = consumeCursor(cursor, bh, totalRows);
        }

        if (totalRows != mergedRows)
            throw new IllegalStateException("Total rows is: " + totalRows + " but should be: " + rowsPerSSTable * sstableCount);
    }

    @Benchmark
    public void iterateThroughPartitionIndexIterator() throws Throwable
    {
        long totalRows = 0;
        Iterable<SSTableReader> ssTables = cfs.getSSTables(SSTableSet.LIVE);
        for (SSTableReader reader : ssTables)
        {
            try (PartitionIndexIterator partitionIndexIterator = reader.allKeysIterator())
            {

                long start = partitionIndexIterator.dataPosition();
                long end = partitionIndexIterator.dataPosition();
                while (end != -1)
                {
                    totalRows++;
                    start = end;
                    partitionIndexIterator.advance();
                    end = partitionIndexIterator.dataPosition();
                }
            }
        }
        if (totalRows != sstableCount * rowsPerSSTable)
            throw new IllegalStateException("Total rows is: " + totalRows + " but should be: " + rowsPerSSTable * sstableCount);
    }

    @Benchmark
    public void iterateThroughMergeIterator(Blackhole bh) throws Throwable
    {
        long totalRows = 0;
        Set<SSTableReader> ssTables = cfs.getLiveSSTables();

        try (UnfilteredPartitionIterator mergedScanner = UnfilteredPartitionIterators.merge(ssTables.stream()
                                                                                                    .map(SSTableReader::getScanner)
                                                                                                    .collect(Collectors.toList()),
                                                                                            UnfilteredPartitionIterators.MergeListener.NOOP))
        {
            totalRows = consumePartitionIterator(mergedScanner, bh, totalRows);
        }

        // this is assuming
        if (totalRows != mergedRows)
            throw new IllegalStateException("Total rows is: " + totalRows + " but should be: " + mergedRows);
    }

    @Benchmark
    public void iterateThroughCompactionIterator(Blackhole bh) throws Throwable
    {
        long totalRows = 0;
        Iterable<SSTableReader> ssTables = cfs.getSSTables(SSTableSet.LIVE);
        Set<SSTableReader> compacting = ImmutableSet.copyOf(ssTables);
        CompactionStrategy strategy = cfs.getCompactionStrategy();
        CompactionController controller = new CompactionController(cfs, compacting, CompactionManager.NO_GC);

        try (ScannerList scanners = strategy.getScanners(compacting);
             CompactionIterator ci = new CompactionIterator(OperationType.COMPACTION, scanners.scanners, controller, FBUtilities.nowInSeconds(), UUID.randomUUID()))
        {
            totalRows = consumePartitionIterator(ci, bh, totalRows);
        }

        // this is assuming
        if (totalRows != mergedRows)
            throw new IllegalStateException("Total rows is: " + totalRows + " but should be: " + mergedRows);
    }

    @Benchmark
    public void iterateThroughCompactionCursor(Blackhole bh) throws Throwable
    {
        long totalRows = 0;
        Iterable<SSTableReader> ssTables = cfs.getLiveSSTables();
        Set<SSTableReader> compacting = ImmutableSet.copyOf(ssTables);
        CompactionController controller = new CompactionController(cfs, compacting, CompactionManager.NO_GC);

        try (CompactionCursor ci = new CompactionCursor(OperationType.COMPACTION,
                                                        compacting,
                                                        controller,
                                                        RateLimiter.create(Double.MAX_VALUE),
                                                        FBUtilities.nowInSeconds(),
                                                        UUID.randomUUID()))
        {
            totalRows = consumeCompactionCursor(ci, bh, totalRows);
        }

        // this is assuming
        if (totalRows != mergedRows)
            throw new IllegalStateException("Total rows is: " + totalRows + " but should be: " + mergedRows);
    }

    @Benchmark
    public void iterateThroughCompactionCursorWithLimiter(Blackhole bh) throws Throwable
    {
        long totalRows = 0;
        Iterable<SSTableReader> ssTables = cfs.getLiveSSTables();
        Set<SSTableReader> compacting = ImmutableSet.copyOf(ssTables);
        CompactionController controller = new CompactionController(cfs, compacting, CompactionManager.NO_GC);

        try (CompactionCursor ci = new CompactionCursor(OperationType.COMPACTION,
                                                        compacting,
                                                        controller,
                                                        RateLimiter.create(Double.MAX_VALUE),
                                                        FBUtilities.nowInSeconds(),
                                                        UUID.randomUUID()))
        {
            totalRows = consumeCompactionCursor(ci, bh, totalRows);
        }

        // this is assuming
        if (totalRows != mergedRows)
            throw new IllegalStateException("Total rows is: " + totalRows + " but should be: " + mergedRows);
    }

    public long consumePartitionIterator(Iterator<UnfilteredRowIterator> partitionIterator, Blackhole bh, long totalRows)
    {
        while (partitionIterator.hasNext())
        {
            UnfilteredRowIterator partition = partitionIterator.next();
            while (partition.hasNext())
            {
                Unfiltered row = partition.next();
                bh.consume(row);
                if (row.isRow())
                    totalRows++;
            }
        }
        return totalRows;
    }

    private long consumeCursor(SSTableCursor cursor, Blackhole bh, long totalRows) throws IOException
    {
        while (true)
        {
            switch (cursor.advance())
            {
                case EXHAUSTED:
                    return totalRows;
                case ROW:
                    ++totalRows;
                default:
                    // skip headers
            }
        }
    }

    private long consumeCompactionCursor(CompactionCursor cursor, Blackhole bh, long totalRows) throws IOException
    {
        class Sink implements SSTableDataSink {
            long rows = 0;

            public boolean append(UnfilteredRowIterator partition)
            {
                return true;
            }

            public boolean startPartition(DecoratedKey partitionKey, DeletionTime deletionTime) throws IOException
            {
                // nothing
                return true;
            }

            public void endPartition() throws IOException
            {
                // nothing
            }

            public void addUnfiltered(Unfiltered unfiltered) throws IOException
            {
                if (unfiltered.isRow())
                    ++rows;
            }
        };

        Sink sink = new Sink();
        while (cursor.copyOne(sink) != SSTableCursor.Type.EXHAUSTED)
        {}

        return sink.rows + totalRows;
    }

    @Benchmark
    public void scannerToCompactionWriter()
    {

        Iterable<SSTableReader> ssTables = cfs.getSSTables(SSTableSet.LIVE);
        final SSTableReader ssTableReader = ssTables.iterator().next();
        Set<SSTableReader> compacting = ImmutableSet.copyOf(cfs.getSSTables(SSTableSet.LIVE));

        try (LifecycleTransaction transaction = cfs.getTracker().tryModify(compacting, OperationType.COMPACTION);
             CompactionAwareWriter writer = new DefaultCompactionWriter(cfs, cfs.getDirectories(), transaction, compacting);)
        {
            try (final ISSTableScanner scanner = ssTableReader.getScanner())
            {
                while (scanner.hasNext())
                {
                    writer.append(scanner.next());
                }
            }
            writer.finish();
        }
    }
}
