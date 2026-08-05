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

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;

import org.junit.After;
import org.junit.Assume;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.sstable.ZeroCopySSTableSplitter.Child;
import org.apache.cassandra.io.sstable.ZeroCopySSTableSplitter.RepairState;
import org.apache.cassandra.io.sstable.ZeroCopySSTableSplitter.Result;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.StatsComponent;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.sstable.format.big.IndexSummaryComponent;
import org.apache.cassandra.io.sstable.indexsummary.IndexSummary;
import org.apache.cassandra.io.sstable.indexsummary.IndexSummarySupport;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.utils.EstimatedHistogram;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * The three things about a split child's {@code Statistics.db} that are wrong silently.
 *
 * <p>Everything else the splitter writes is either verbatim parent bytes or is checked by a CRC on the read
 * path. Statistics.db is neither: a child whose {@code estimatedPartitionSize} buckets differ from the ones a
 * writer would have produced, or whose {@code firstKey}/{@code lastKey} are the parent's, or whose
 * {@code tokenSpaceCoverage} was inherited, opens cleanly, reads back every row correctly, and passes
 * {@code nodetool verify}. The damage shows up much later, in compaction and range-selection decisions.
 *
 * <p>Every test here is BIG-only: the splitter refuses anything else (see
 * {@code ZeroCopySSTableSplitterBtiTest}), and Summary.db exists only for BIG.
 *
 * @see ZeroCopySSTableSplitter#writeStatistics
 * @see ZeroCopySSTableSplitter#writeSummary
 */
public class ZeroCopySplitStatsTest extends CQLTester
{
    /** Scratch directories handed out by {@link #scratchDirectory()}, deleted after each test rather than leaked. */
    private final List<File> scratchDirectories = new ArrayList<>();

    @After
    public void deleteScratchDirectories()
    {
        for (File directory : scratchDirectories)
        {
            try
            {
                directory.deleteRecursive();
            }
            catch (Throwable t)
            {
                // Reported rather than thrown: a throwing @After would replace whatever failure left the
                // directory undeletable in the first place
                logger.warn("Could not delete the scratch directory {}", directory, t);
            }
        }
        scratchDirectories.clear();
    }

    /**
     * The child's partition-size histogram is built by the splitter with its own hard-coded bucket count,
     * because {@code MetadataCollector.defaultPartitionSizeHistogram()} is package-private in another package.
     * Two independent constants that must agree forever, with no compile-time link between them -- and the
     * consequence of drift is invisible: {@code EstimatedHistogram} with different offsets still serializes,
     * still deserializes, and still answers percentile queries, just against a different bucket grid, so every
     * per-table aggregate over a mix of split and flushed sstables silently compares incomparable numbers.
     *
     * <p>The fork this was ported from used 150; trunk's {@code MetadataCollector} uses 155.
     */
    @Test
    public void childPartitionSizeHistogramMatchesAWriterProducedOne() throws Throwable
    {
        Assume.assumeTrue(BigFormat.isSelected());

        SSTableReader parent = compressedSSTable(80, 5, 480);
        // The oracle: a histogram that came out of MetadataCollector, via a real flush.
        long[] writerOffsets = parent.getSSTableMetadata().estimatedPartitionSize.getBucketOffsets();
        assertEquals("MetadataCollector.defaultPartitionSizeHistogram() no longer has " +
                     ZeroCopySSTableSplitter.PARTITION_SIZE_HISTOGRAM_BUCKETS + " buckets; the splitter's " +
                     "PARTITION_SIZE_HISTOGRAM_BUCKETS has to be changed with it",
                     ZeroCopySSTableSplitter.PARTITION_SIZE_HISTOGRAM_BUCKETS, writerOffsets.length);

        Result result = ZeroCopySSTableSplitter.split(parent, 3, null);
        try
        {
            assertEquals(3, result.children.size());

            long totalPartitions = 0;
            for (Child child : result.children)
            {
                EstimatedHistogram histogram = child.reader.getSSTableMetadata().estimatedPartitionSize;
                assertArrayEquals("a split child must bucket partition sizes exactly like a writer would",
                                  writerOffsets, histogram.getBucketOffsets());

                // ...and it must hold the child's own partitions, not a copy of the parent's counts
                assertEquals(child.partitionCount, histogram.count());
                totalPartitions += histogram.count();
            }
            assertEquals(parent.getSSTableMetadata().estimatedPartitionSize.count(), totalPartitions);
        }
        finally
        {
            release(result);
        }
    }

    /**
     * {@code StatsMetadata.firstKey}/{@code lastKey} are new in trunk and take priority over Summary.db:
     * {@code BigSSTableReaderLoadingBuilder.openComponents} sets the reader's first/last from them whenever
     * {@code version.hasKeyRange()}, and only falls back to Summary.db when they are absent. A child that
     * inherited the parent's pair would therefore claim the parent's whole key range the moment it was reopened
     * -- and every range-based sstable selection (reads, cleanup, repair validation, streaming) would believe
     * it.
     */
    @Test
    public void childKeyRangeIsItsOwnAcrossAReopen() throws Throwable
    {
        Assume.assumeTrue(BigFormat.isSelected());

        SSTableReader parent = compressedSSTable(80, 5, 480);
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        Assume.assumeTrue("the key range is only written from version 'oa' on",
                          parent.descriptor.version.hasKeyRange());

        Result result = ZeroCopySSTableSplitter.split(parent, 4, null);
        List<Child> children = result.children;
        try
        {
            assertEquals(4, children.size());

            for (Child child : children)
            {
                StatsMetadata stats = child.reader.getSSTableMetadata();
                assertEquals(child.first, child.reader.getFirst());
                assertEquals(child.last, child.reader.getLast());
                assertEquals("Statistics.db firstKey must be the child's own", child.first.getKey(), stats.firstKey);
                assertEquals("Statistics.db lastKey must be the child's own", child.last.getKey(), stats.lastKey);

                // Not inherited: the parent's coverage is the whole range it spans, so handing it to each of K
                // children multiplies the table's apparent coverage by K and skews the density that drives
                // compaction. NaN is MetadataCollector's own "unknown".
                assertTrue("tokenSpaceCoverage must not be inherited, got " + stats.tokenSpaceCoverage,
                           Double.isNaN(stats.tokenSpaceCoverage));
            }

            // The interesting half of the property: no child may claim the parent's far end.
            assertEquals(parent.getFirst(), children.get(0).first);
            assertEquals(parent.getLast(), children.get(children.size() - 1).last);
            assertNotEquals(parent.getLast().getKey(), children.get(0).reader.getSSTableMetadata().lastKey);
            assertNotEquals(parent.getFirst().getKey(),
                            children.get(children.size() - 1).reader.getSSTableMetadata().firstKey);
        }
        finally
        {
            release(result);
        }

        // Reopen purely from the files. This is the read that consults Statistics.db in preference to
        // Summary.db, so it is the one that would expose an inherited key range.
        List<SSTableReader> reopened = new ArrayList<>(children.size());
        try
        {
            for (Child child : children)
                reopened.add(SSTableReader.open(cfs, child.descriptor, child.components, cfs.metadata));

            for (int i = 0; i < reopened.size(); i++)
            {
                SSTableReader reader = reopened.get(i);
                assertEquals("reopened child " + i + " does not cover its own range",
                             children.get(i).first, reader.getFirst());
                assertEquals("reopened child " + i + " does not cover its own range",
                             children.get(i).last, reader.getLast());
                // spelled out, because this is the exact failure an inherited key range produces
                if (i > 0)
                    assertNotEquals("reopened child " + i + " claims the parent's first key",
                                    parent.getFirst(), reader.getFirst());
                if (i < reopened.size() - 1)
                    assertNotEquals("reopened child " + i + " claims the parent's last key",
                                    parent.getLast(), reader.getLast());
            }
        }
        finally
        {
            for (SSTableReader reader : reopened)
                reader.selfRef().release();
        }
    }

    /**
     * The end-to-end assertion above can only see NaN-in, NaN-out: a flush leaves
     * {@code tokenSpaceCoverage} unknown, since only the sharded UCS writers ever call
     * {@code SSTableWriter.setTokenSpaceCoverage}. This drives {@code writeStatistics} directly with a parent
     * that does carry a coverage, which is the only way to tell "deliberately dropped" from "there was nothing
     * to inherit".
     */
    @Test
    public void tokenSpaceCoverageIsDroppedRatherThanInherited() throws Throwable
    {
        Assume.assumeTrue(BigFormat.isSelected());

        SSTableReader parent = compressedSSTable(20, 2, 200);
        StatsMetadata flushed = parent.getSSTableMetadata();
        List<AbstractType<?>> clusteringTypes = parent.metadata().comparator.subtypes();

        // The copy below is field by field with nothing but review holding it to StatsMetadata's constructor, so a
        // field added to one and not the other is dropped silently -- and then this test would be seeding a record
        // that differs from the parent's in more than the coverage. Copying the coverage unchanged has to give an
        // equal record: which is a real check now that hasUnindexedRegions is part of equals, and is deliberately
        // asserted against a record straight out of a flush rather than a hand-built one.
        assertEquals("withTokenSpaceCoverage has drifted from the StatsMetadata constructor",
                     flushed, withTokenSpaceCoverage(flushed, clusteringTypes, flushed.tokenSpaceCoverage));

        StatsMetadata seeded = withTokenSpaceCoverage(flushed, clusteringTypes, 0.25);
        assertEquals(0.25, seeded.tokenSpaceCoverage, 0.0);
        // The two fields StatsMetadata.equals does not look at, so the assertion above cannot see them
        assertEquals(flushed.isTransient, seeded.isTransient);
        assertEquals(flushed.hasUnindexedRegions, seeded.hasUnindexedRegions);

        Descriptor child = scratchDescriptor(parent);
        writeStatistics(parent, seeded, child, false);

        StatsMetadata written = StatsComponent.load(child).statsMetadata();
        assertTrue("tokenSpaceCoverage must not be inherited, got " + written.tokenSpaceCoverage,
                   Double.isNaN(written.tokenSpaceCoverage));
        // ...and this is a targeted drop, not a wholesale reset: its record neighbours did come through.
        assertEquals(seeded.minTimestamp, written.minTimestamp);
        assertEquals(seeded.maxTimestamp, written.maxTimestamp);
        assertEquals(seeded.originatingHostId, written.originatingHostId);
        assertEquals(seeded.totalRows, written.totalRows);
    }

    /**
     * {@code StatsMetadata.hasUnindexedRegions} marks a Data.db holding partitions its index does not describe, so
     * one that must be read through that index instead of scanned linearly. It is the one field here whose loss is
     * not a statistic being wrong but a read returning rows the sstable does not claim -- and it is now VERSION
     * GATED (BIG {@code pb} and later) rather than inferred from being present at all, so a Statistics.db round
     * trip is the only thing that shows the gate and the field agree.
     *
     * <p>The splitter always passes false, a split child having no interior unindexed regions; the caller that
     * passes true is {@code ZeroCopySSTableSlice}. Both are driven, because a serializer that ignored the argument
     * would satisfy either one on its own.
     */
    @Test
    public void theUnindexedRegionsMarkerSurvivesTheRoundTripThroughStatistics() throws Throwable
    {
        Assume.assumeTrue(BigFormat.isSelected());

        SSTableReader parent = compressedSSTable(20, 2, 200);
        StatsMetadata flushed = parent.getSSTableMetadata();
        List<AbstractType<?>> clusteringTypes = parent.metadata().comparator.subtypes();
        boolean versionCanHoldIt = parent.descriptor.version.hasUnindexedRegionsMarker();

        assertFalse("a flush must never mark an sstable", flushed.hasUnindexedRegions);

        for (boolean requested : new boolean[]{ false, true })
        {
            Descriptor child = scratchDescriptor(parent);
            writeStatistics(parent, flushed, child, requested);
            StatsMetadata written = StatsComponent.load(child).statsMetadata();

            assertEquals("the marker must survive Statistics.db in a version that can hold it, and must read back"
                         + " false in one that cannot -- there is no marker in an older Statistics.db to find,"
                         + " which is why ZeroCopySSTableSlice refuses to produce an sstable in such a version"
                         + " rather than write a marker its own reader would ignore",
                         versionCanHoldIt && requested, written.hasUnindexedRegions);
        }

        // And it is part of the record's identity, so two otherwise identical records that differ only in the
        // marker are neither equal nor equally hashed. Without that, anything that compares or caches a
        // StatsMetadata -- MetadataSerializer's round-trip tests included -- would pass with the field dropped.
        StatsMetadata unmarked = copyOf(flushed, clusteringTypes, flushed.tokenSpaceCoverage, false);
        StatsMetadata marked = copyOf(flushed, clusteringTypes, flushed.tokenSpaceCoverage, true);
        assertNotEquals(unmarked, marked);
        assertNotEquals(unmarked.hashCode(), marked.hashCode());
    }

    /**
     * {@code ZeroCopySSTableSplitter.writeSummary} deliberately does not call
     * {@code IndexSummaryComponent.save}: that method writes the same three things in the same order but does
     * not fsync, which is fine for index-summary redistribution (it can rebuild what it loses) and not fine for
     * a split, whose transaction commit is what unlinks the parent.
     *
     * <p>The cost of that duplication is a second copy of the Summary.db layout that nothing else compiles
     * against. This pins the two together: same summary, same keys, byte-identical files.
     */
    @Test
    public void writeSummaryMatchesIndexSummaryComponentSaveByteForByte() throws Throwable
    {
        Assume.assumeTrue(BigFormat.isSelected());

        SSTableReader parent = compressedSSTable(80, 2, 300);
        IndexSummary summary = ((IndexSummarySupport<?>) parent).getIndexSummary();

        File directory = scratchDirectory();
        Descriptor scratch = new Descriptor(parent.descriptor.version,
                                            directory,
                                            parent.descriptor.ksname,
                                            parent.descriptor.cfname,
                                            parent.descriptor.id);

        ZeroCopySSTableSplitter.writeSummary(scratch, parent.getFirst(), parent.getLast(), summary);
        File written = scratch.fileFor(BigFormat.Components.SUMMARY);

        File oracle = new File(directory, "oracle-Summary.db");
        new IndexSummaryComponent(summary, parent.getFirst(), parent.getLast()).save(oracle, false);

        assertTrue("nothing was written", written.length() > 0);
        assertArrayEquals("ZeroCopySSTableSplitter.writeSummary has drifted from IndexSummaryComponent.save",
                          Files.readAllBytes(oracle.toPath()),
                          Files.readAllBytes(written.toPath()));
    }

    // ----------------------------------------------------------------------------------------------------
    // Helpers
    // ----------------------------------------------------------------------------------------------------

    /**
     * A copy of {@code stats} carrying a real token-space coverage. {@code StatsMetadata.clusteringTypes} is
     * private with no accessor, so it has to be supplied from the table's comparator -- which is exactly what
     * {@code MetadataCollector.finalizeMetadata} and {@code ZeroCopySSTableSplitter.writeStatistics} both pass.
     */
    private static StatsMetadata withTokenSpaceCoverage(StatsMetadata stats,
                                                        List<AbstractType<?>> clusteringTypes,
                                                        double tokenSpaceCoverage)
    {
        return copyOf(stats, clusteringTypes, tokenSpaceCoverage, stats.hasUnindexedRegions);
    }

    /**
     * A field-by-field copy of {@code stats} with the two fields the tests above vary passed in. Every other field
     * is carried over verbatim, including {@code hasUnindexedRegions} -- which the twenty-five argument constructor
     * would quietly default to false, and which is part of {@code equals}/{@code hashCode}, so a copy that dropped
     * it would make the record inequal to its original for a reason nothing here is testing.
     */
    private static StatsMetadata copyOf(StatsMetadata stats,
                                        List<AbstractType<?>> clusteringTypes,
                                        double tokenSpaceCoverage,
                                        boolean hasUnindexedRegions)
    {
        return new StatsMetadata(stats.estimatedPartitionSize,
                                 stats.estimatedCellPerPartitionCount,
                                 stats.commitLogIntervals,
                                 stats.minTimestamp,
                                 stats.maxTimestamp,
                                 stats.minLocalDeletionTime,
                                 stats.maxLocalDeletionTime,
                                 stats.minTTL,
                                 stats.maxTTL,
                                 stats.compressionRatio,
                                 stats.estimatedTombstoneDropTime,
                                 stats.sstableLevel,
                                 clusteringTypes,
                                 stats.coveredClustering,
                                 stats.hasLegacyCounterShards,
                                 stats.repairedAt,
                                 stats.totalColumnsSet,
                                 stats.totalRows,
                                 tokenSpaceCoverage,
                                 stats.originatingHostId,
                                 stats.pendingRepair,
                                 stats.isTransient,
                                 stats.hasPartitionLevelDeletions,
                                 stats.firstKey,
                                 stats.lastKey,
                                 hasUnindexedRegions);
    }

    /**
     * {@code ZeroCopySSTableSplitter.writeStatistics} with everything a caller is free to choose fixed, so that the
     * one argument the tests above vary -- {@code hasUnindexedRegions} -- is named at each call site rather than
     * being a bare boolean eleven positions in.
     */
    private static void writeStatistics(SSTableReader parent,
                                        StatsMetadata parentStats,
                                        Descriptor child,
                                        boolean hasUnindexedRegions) throws IOException
    {
        ZeroCopySSTableSplitter.writeStatistics(child,
                                                parent.metadata(),
                                                ZeroCopySSTableSplitter.readParentMetadata(parent.descriptor),
                                                parentStats,
                                                new EstimatedHistogram(ZeroCopySSTableSplitter.PARTITION_SIZE_HISTOGRAM_BUCKETS),
                                                new HyperLogLogPlus(ZeroCopySSTableSplitter.HLL_P,
                                                                    ZeroCopySSTableSplitter.HLL_SP),
                                                1024,
                                                4096,
                                                parent.getFirst(),
                                                parent.getLast(),
                                                hasUnindexedRegions,
                                                RepairState.inherit(parentStats));
    }

    /** A descriptor in a scratch directory, in the parent's version, so nothing here touches a live sstable. */
    private Descriptor scratchDescriptor(SSTableReader parent) throws IOException
    {
        return new Descriptor(parent.descriptor.version,
                              scratchDirectory(),
                              parent.descriptor.ksname,
                              parent.descriptor.cfname,
                              parent.descriptor.id);
    }

    /** A directory of its own, deleted after the test by {@link #deleteScratchDirectories()}. */
    private File scratchDirectory() throws IOException
    {
        File directory = new File(Files.createTempDirectory("zeroCopySplitStats"));
        scratchDirectories.add(directory);
        return directory;
    }

    /** One flushed, compressed sstable spanning many compression chunks. */
    private SSTableReader compressedSSTable(int partitions, int rowsPerPartition, int valueBytes) throws Throwable
    {
        createTable("CREATE TABLE %s (pk text, ck int, val text, PRIMARY KEY (pk, ck)) " +
                    "WITH compression = {'class': 'LZ4Compressor', 'chunk_length_in_kb': '4'}");
        disableCompaction();
        for (int p = 0; p < partitions; p++)
            for (int c = 0; c < rowsPerPartition; c++)
                execute("INSERT INTO %s (pk, ck, val) VALUES (?, ?, ?)",
                        String.format("k%06d", p), c, randomText(valueBytes));
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        Set<SSTableReader> live = cfs.getLiveSSTables();
        assertEquals("expected exactly one sstable", 1, live.size());
        SSTableReader sstable = live.iterator().next();
        assertTrue(ZeroCopySSTableSplitter.isSupported(sstable));
        return sstable;
    }

    /** Near-incompressible payload, so the sstable really does span many compression chunks. */
    private static String randomText(int length)
    {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        char[] chars = new char[length];
        for (int i = 0; i < length; i++)
            chars[i] = (char) ('!' + random.nextInt(94));
        return new String(chars);
    }

    private static void release(Result result)
    {
        for (Child child : result.children)
            child.reader.selfRef().release();
    }
}
