/*
 * Copyright IBM Corp.
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

package org.apache.cassandra.index.sai.functional;

import java.util.List;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SAIUtil;
import org.apache.cassandra.index.sai.SSTableIndex;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.cql.VectorTester;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.disk.format.IndexComponents;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.disk.vector.CassandraOnHeapGraph;

import static org.junit.Assert.*;

/// Verifies on-disk vector index size behavior across SAI format versions,
/// both preCompaction and postCompaction.
public class VectorFormatDiskUsageTest extends VectorTester
{
    private static final int DIMENSION = 128;

    /// Number of flushes before compaction. Each flush produces one SSTable. With only
    /// [CassandraOnHeapGraph#MIN_PQ_ROWS] rows per flush the memory limit is not reached,
    /// so each SSTable contains exactly one segment — asserted in [#measureDiskUsage].
    private static final int NUM_FLUSHES = 2;

    /// TERMS_DATA delta from EC (jvector format 4) to FB (jvector format 6) per segment:
    /// one extra header copy + FOOTER_SIZE trailer. Independent of dimensions, number of vectors, etc.
    ///
    /// EC header  = (CommonHeader=288) + (feature bitmask int=4)                    = 292 bytes
    /// FB header  = (CommonHeader=288) + (features.size() int=4) + (ordinal int=4)  = 296 bytes
    /// FB writes: start-header + footer-header + FOOTER_SIZE(=Long+Int=12)          = 604 bytes
    /// Δ = 604 − 292 = 312 bytes per segment
    private static final long EXPECTED_TERMS_DATA_DELTA_PER_SEGMENT = 312L;

    @BeforeClass
    public static void setUpClass()
    {
        VectorTester.setUpClass();
    }

    @After
    public void resetVersion()
    {
        SAIUtil.resetCurrentVersion();
    }

    /// Deep structural verification of the EC → FB format upgrade (jvector format 4 → 6,
    /// no FusedPQ, no hierarchy). Asserts exact per-component byte deltas and that total
    /// disk usage grows by less than 5%.
    ///
    /// A SAI vector index is stored as five on-disk component files per SSTable:
    ///
    /// | Component | Content |
    /// |-----------|---------|
    /// | `TERMS_DATA` | The graph: nodes, edges, and per-node inline vector data |
    /// | `PQ` | Product Quantization codebook + per-vector PQ codes |
    /// | `META` | Segment metadata: offsets, lengths, statistics |
    /// | `POSTING_LISTS` | Mapping from row IDs to graph node ordinals |
    /// | `COLUMN_COMPLETION_MARKER` | Fixed-size sentinel indicating the index is complete |
    ///
    /// FB (jvector format 6) writes the graph header twice (start + footer) plus a 12-byte trailer,
    /// vs EC (jvector format 4) which writes it once.
    ///
    /// Header sizes are derived from {@code CommonHeader.size()} and {@code Header.size()} in jvector:
    ///
    /// ```
    /// CommonHeader.size() (both formats):
    ///   int size = 4;                         // size + dimension + entryNode + maxDegree  (always)
    ///   if (version >= 3) size += 2;           // magic + version
    ///   if (version >= 4) size += 2 + 2 * 32; // idUpperBound + numLayers + 32 LayerInfo pairs
    ///   → (4 + 2 + 2 + 64) × 4 = 288 bytes
    ///
    /// Header.size() adds the feature section on top of CommonHeader:
    ///   EC (format 4, version < 6): +Integer.BYTES for a single FeatureId bitmask int  → 288 + 4 = 292 bytes
    ///   FB (format 6):              +Integer.BYTES (features count) + Integer.BYTES (feature ordinal) → 288 + 4 + 4 = 296 bytes
    ///
    /// The only feature present by default on FB is INLINE_VECTORS. With exactly [CassandraOnHeapGraph#MIN_PQ_ROWS]
    /// rows, PQ training is met but FusedPQ is disabled, so no FusedPQ feature is written.
    /// InlineVectors.headerSize() == 0 because dimension is already stored in CommonHeader.
    ///
    /// FOOTER_SIZE = FOOTER_MAGIC_SIZE(Integer.BYTES=4) + FOOTER_OFFSET_SIZE(Long.BYTES=8) = 12 bytes.
    /// These constants and writeFooter() are in {@code AbstractGraphIndexWriter} in jvector.
    /// writeFooter() writes: full header copy + 8-byte offset (pointing back to the header) + 4-byte magic.
    /// Cassandra always passes useFooter=false to OnDiskGraphIndex.load(), so the footer is written
    /// as part of the format but never read — see [CassandraDiskAnn] constructor.
    ///
    /// FB writes: start-header(296) + footer-header(296) + FOOTER_SIZE(Long+Int=12) = 604 bytes
    /// EC writes: start-header(292)                                                  = 292 bytes
    /// Δ = 604 − 292 = 312 bytes per segment
    /// ```
    ///
    /// `META` grows by exactly 8 bytes per segment (a `totalTermCount` long added in ED).
    /// All other components (`PQ`, `POSTING_LISTS`, `COLUMN_COMPLETION_MARKER`) are unchanged.
    @Test
    public void testDiskUsageECvsFB()
    {
        testDiskUsageECvsFB(false);
        testDiskUsageECvsFB(true);
    }

    private void testDiskUsageECvsFB(boolean compact)
    {
        String phase = compact ? "postCompaction" : "preCompaction";

        DiskMeasurement ec = measureDiskUsage(Version.EC, "EC-" + phase, compact);
        DiskMeasurement fb = measureDiskUsage(Version.FB, "FB-" + phase, compact);

        assertTrue("EC index must have non-zero disk usage", ec.totalBytes > 0);
        assertTrue("FB index must have non-zero disk usage", fb.totalBytes > 0);

        long termsDataDelta = fb.termsDataBytes - ec.termsDataBytes;
        double diskGrowthPercent = 100.0 * (fb.totalBytes - ec.totalBytes) / ec.totalBytes;

        logger.debug("  EC {}  diskUsage() : {} ({} segments)", phase, ec.totalBytes, ec.segmentCount);
        logger.debug("  FB {}  diskUsage() : {} ({} segments)", phase, fb.totalBytes, fb.segmentCount);
        logger.debug("  Total disk usage growth {}  : +{} bytes ({} %)",
                phase, fb.totalBytes - ec.totalBytes, String.format("%.4f", diskGrowthPercent));
        logger.debug("  TERMS_DATA delta {}  : {} (expected {} × {} = {})",
                phase, termsDataDelta, NUM_FLUSHES, EXPECTED_TERMS_DATA_DELTA_PER_SEGMENT,
                NUM_FLUSHES * EXPECTED_TERMS_DATA_DELTA_PER_SEGMENT);

        verifyComponentAccounting(ec, fb, phase);
        verifyTermsDataDelta(termsDataDelta, ec.segmentCount, phase);
        verifyUnchangedComponents(ec, fb, phase);
        verifyConservation(ec, fb, termsDataDelta, phase);
        verifyTotalDiskGrowthUnder5Percent(diskGrowthPercent, phase);
    }

    /// Regression guard: for every vector-capable version from [Version#JVECTOR_EARLIEST] (CA)
    /// up to but not including [Version#LATEST], asserts that [Version#LATEST] uses less than
    /// 5% more disk than that older version. Catches accidental large regressions introduced
    /// by a new format version.
    @Test
    public void testDiskGrowthAcrossVersions()
    {
        testDiskGrowthAcrossVersions(false);
        testDiskGrowthAcrossVersions(true);
    }

    private void testDiskGrowthAcrossVersions(boolean compact)
    {
        String name = compact ? "postCompaction" : "preCompaction";
        DiskMeasurement latest = measureDiskUsage(Version.LATEST, Version.LATEST + "-" + name, compact);

        for (Version version : Version.ALL)
        {
            if (version == Version.LATEST || !version.onOrAfter(Version.JVECTOR_EARLIEST))
                continue;

            DiskMeasurement older = measureDiskUsage(version, version + "-" + name, compact);

            double diskGrowthPercent = 100.0 * (latest.totalBytes - older.totalBytes) / older.totalBytes;

            logger.debug("  {} → {} {}  : {} → {} bytes ({} %)",
                         version, Version.LATEST, name, older.totalBytes, latest.totalBytes,
                         String.format("%.4f", diskGrowthPercent));

            verifyTotalDiskGrowthUnder5Percent(diskGrowthPercent, version + " → " + Version.LATEST + ' ' + name);
        }
    }

    /// Sanity: totalBytes must equal the sum of all five components (nothing missed or double-counted).
    private static void verifyComponentAccounting(DiskMeasurement ec, DiskMeasurement fb, String phase)
    {
        assertEquals("EC " + phase + ": totalBytes must equal sum of all per-index components",
                ec.totalBytes, ec.termsDataBytes + ec.pqBytes + ec.metaBytes + ec.postingListsBytes + ec.completionMarkerBytes);
        assertEquals("FB " + phase + ": totalBytes must equal sum of all per-index components",
                fb.totalBytes, fb.termsDataBytes + fb.pqBytes + fb.metaBytes + fb.postingListsBytes + fb.completionMarkerBytes);
    }

    /// The TERMS_DATA delta must equal exactly [#EXPECTED_TERMS_DATA_DELTA_PER_SEGMENT] × segmentCount.
    /// Each segment contributes one graph to the TERMS_DATA file, and each graph carries one header delta.
    private static void verifyTermsDataDelta(long actualTermsDataDelta, int segmentCount, String phase)
    {
        long expected = EXPECTED_TERMS_DATA_DELTA_PER_SEGMENT * segmentCount;
        assertEquals("TERMS_DATA delta " + phase + " must equal " + EXPECTED_TERMS_DATA_DELTA_PER_SEGMENT
                        + " bytes × " + segmentCount + " segment(s) = " + expected
                        + " (jvector format 4→6 header layout change only)",
                expected, actualTermsDataDelta);
    }

    private static void verifyUnchangedComponents(DiskMeasurement ec, DiskMeasurement fb, String phase)
    {
        assertEquals("PQ component size must be the same for EC and FB (" + phase + ')',
                ec.pqBytes, fb.pqBytes);
        assertEquals("POSTING_LISTS must be the same size for EC and FB (" + phase + ')',
                ec.postingListsBytes, fb.postingListsBytes);
        assertEquals("COLUMN_COMPLETION_MARKER must be the same size for EC and FB (" + phase + ')',
                ec.completionMarkerBytes, fb.completionMarkerBytes);
    }

    /// META grows by exactly 8 bytes per segment EC → FB (a `totalTermCount` long added in ED).
    /// Because all other components except TERMS_DATA are identical, the net total delta equals
    /// the TERMS_DATA delta plus 8 × segmentCount bytes.
    private static void verifyConservation(DiskMeasurement ec, DiskMeasurement fb,
                                           long actualTermsDataDelta, String phase)
    {
        assertEquals("FB META must be exactly " + Long.BYTES + " bytes × " + ec.segmentCount
                        + " segment(s) larger than EC META (" + phase + ')',
                ec.metaBytes + (long) Long.BYTES * ec.segmentCount, fb.metaBytes);
        assertEquals("FB.totalBytes must equal ec.totalBytes + TERMS_DATA delta + 8 × segmentCount(" + phase + ')',
                ec.totalBytes + actualTermsDataDelta + (long) Long.BYTES * ec.segmentCount,
                fb.totalBytes);
    }

    /// Total disk usage must grow by less than 5% between consecutive format versions.
    /// The fixed per-segment overhead (header delta + META) is negligible relative to
    /// the graph node data for any realistic dataset.
    private static void verifyTotalDiskGrowthUnder5Percent(double diskGrowthPercent, String phase)
    {
        assertTrue(String.format("Total disk usage growth %s must be < 5%% but was %.4f%%",
                        phase, diskGrowthPercent),
                diskGrowthPercent < 5.0);
    }

    /// Snapshot of per-component file sizes and segment count for one index build.
    private static class DiskMeasurement
    {
        final long totalBytes;
        final long termsDataBytes;
        final long pqBytes;
        final long metaBytes;
        final long postingListsBytes;
        final long completionMarkerBytes;
        final int segmentCount; // total segments across all SSTables

        private DiskMeasurement(Builder b)
        {
            this.totalBytes = b.totalBytes;
            this.termsDataBytes = b.termsDataBytes;
            this.pqBytes = b.pqBytes;
            this.metaBytes = b.metaBytes;
            this.postingListsBytes = b.postingListsBytes;
            this.completionMarkerBytes = b.completionMarkerBytes;
            this.segmentCount = b.segmentCount;
        }

        static class Builder
        {
            long totalBytes;
            long termsDataBytes;
            long pqBytes;
            long metaBytes;
            long postingListsBytes;
            long completionMarkerBytes;
            int segmentCount;

            Builder totalBytes(long v)
            {
                this.totalBytes = v;
                return this;
            }

            Builder termsDataBytes(long v)
            {
                this.termsDataBytes = v;
                return this;
            }

            Builder pqBytes(long v)
            {
                this.pqBytes = v;
                return this;
            }

            Builder metaBytes(long v)
            {
                this.metaBytes = v;
                return this;
            }

            Builder postingListsBytes(long v)
            {
                this.postingListsBytes = v;
                return this;
            }

            Builder completionMarkerBytes(long v)
            {
                this.completionMarkerBytes = v;
                return this;
            }

            Builder segmentCount(int v)
            {
                this.segmentCount = v;
                return this;
            }

            DiskMeasurement build()
            {
                return new DiskMeasurement(this);
            }
        }
    }

    /// Builds a fresh table at `version`, writes [#NUM_FLUSHES] × [CassandraOnHeapGraph#MIN_PQ_ROWS]
    /// vectors in separate flushes, then either returns the pre-compaction measurement
    /// (`compact == false`) or runs major compaction first (`compact == true`).
    ///
    /// Each flush produces one SSTable with one segment (one graph in TERMS_DATA), so
    /// pre-compaction there are [#NUM_FLUSHES] segments across [#NUM_FLUSHES] SSTables;
    /// post-compaction there is exactly 1 segment in 1 SSTable.
    private DiskMeasurement measureDiskUsage(Version version, String label, boolean compact)
    {
        SAIUtil.setCurrentVersion(version);
        createTable("CREATE TABLE %s (pk int, v vector<float, " + DIMENSION + ">, PRIMARY KEY(pk))");
        String indexName = createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        disableCompaction();

        for (int flush = 0; flush < NUM_FLUSHES; flush++)
        {
            for (int i = 0; i < CassandraOnHeapGraph.MIN_PQ_ROWS; i++)
                execute("INSERT INTO %s (pk, v) VALUES (?, ?)",
                        flush * CassandraOnHeapGraph.MIN_PQ_ROWS + i, randomVectorBoxed(DIMENSION));
            flush();
        }

        if (compact)
            compact();

        StorageAttachedIndex sai = (StorageAttachedIndex) getCurrentColumnFamilyStore().indexManager.getIndexByName(indexName);
        assertNotNull("Index not found: " + indexName, sai);
        IndexContext indexContext = sai.getIndexContext();

        long totalDiskBytes = indexContext.diskUsage();
        long graphComponentBytes = componentSize(indexContext, IndexComponentType.TERMS_DATA);
        long pqComponentBytes = componentSize(indexContext, IndexComponentType.PQ);
        long metaComponentBytes = componentSize(indexContext, IndexComponentType.META);
        long postingListsBytes = componentSize(indexContext, IndexComponentType.POSTING_LISTS);
        long completionMarkerBytes = componentSize(indexContext, IndexComponentType.COLUMN_COMPLETION_MARKER);

        List<SSTableIndex> sstableIndexes = List.copyOf(indexContext.getView().getIndexes());
        int totalSegments = sstableIndexes.stream().mapToInt(s -> s.getSegments().size()).sum();
        int expectedSegments = compact ? 1 : NUM_FLUSHES;
        assertEquals("Expected " + expectedSegments + " segment(s) " + label,
                expectedSegments, totalSegments);

        logger.debug("[{}] diskUsage()                : {} ({} segment(s))", label, totalDiskBytes, totalSegments);
        logger.debug("[{}] TERMS_DATA component bytes : {}", label, graphComponentBytes);
        logger.debug("[{}] PQ component bytes         : {}", label, pqComponentBytes);
        logger.debug("[{}] META component bytes       : {}", label, metaComponentBytes);
        logger.debug("[{}] POSTING_LISTS bytes        : {}", label, postingListsBytes);
        logger.debug("[{}] COMPLETION_MARKER bytes    : {}", label, completionMarkerBytes);

        return new DiskMeasurement.Builder()
                .totalBytes(totalDiskBytes)
                .termsDataBytes(graphComponentBytes)
                .pqBytes(pqComponentBytes)
                .metaBytes(metaComponentBytes)
                .postingListsBytes(postingListsBytes)
                .completionMarkerBytes(completionMarkerBytes)
                .segmentCount(totalSegments)
                .build();
    }

    private long componentSize(IndexContext indexContext,
                               IndexComponentType type)
    {
        return indexContext.getView().getIndexes()
                .stream()
                .mapToLong(idx -> {
                    IndexComponents.ForRead perIndex = idx.usedPerIndexComponents();
                    return perIndex.has(type) ? perIndex.get(type).file().length() : 0L;
                })
                .sum();
    }
}
