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

package org.apache.cassandra.io.compress;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.sun.management.OperatingSystemMXBean;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;

public class SimpleRecompressTest
{
    private static final Logger logger = LoggerFactory.getLogger(SimpleRecompressTest.class);
    private static final String KEYSPACE = "p01_clouddb";
    private static final String TABLE = "custom_zone_records";
    private static final String SSTABLE_PATH = "/Users/minalkyada/Desktop/p01_clouddb/custom_zone_records-5fb29180edd8300084c8712786214e10";

    private static ColumnFamilyStore store;

    @BeforeClass
    public static void setup()
    {
        try
        {
            // Initialize Cassandra
            DatabaseDescriptor.daemonInitialization();
            DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
            ServerTestUtils.prepareServerNoRegister();

            Keyspace.setInitialized();
            StorageService.instance.initServer();
            createTable();
            importSSTables();
        }
        catch (Exception e)
        {
            logger.error("FATAL ERROR during setup", e);
            throw new RuntimeException("Setup failed", e);
        }
    }

    private static void createTable()
    {
        logger.info("Creating keyspace {} and table {}", KEYSPACE, TABLE);

        TableMetadata customZone = CreateTableStatement.parse("CREATE TABLE " + KEYSPACE + ".custom_zone_records (\n" +
                                "    container text,\n" +
                                "    owner_dsid bigint,\n" +
                                "    virtual_owner_dsid bigint,\n" +
                                "    zone text,\n" +
                                "    column_type ascii,\n" +
                                "    ref text,\n" +
                                "    rev bigint,\n" +
                                "    idx_name text,\n" +
                                "    idx_val blob,\n" +
                                "    asset blob,\n" +
                                "    val blob,\n" +
                                "    PRIMARY KEY ((container, owner_dsid, virtual_owner_dsid, zone), column_type, ref, rev, idx_name, idx_val)\n" +
                                ") WITH CLUSTERING ORDER BY (column_type ASC, ref ASC, rev ASC, idx_name ASC, idx_val ASC)\n" +
                                "    AND compaction = {'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}\n" +
                                "    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor', 'lz4_compressor_type': 'high', 'lz4_high_compressor_level': '17'};"
                        , KEYSPACE)
                .build();

        SchemaLoader.createKeyspace(KEYSPACE, KeyspaceParams.simple(1), customZone);
        logger.info("Keyspace and table created successfully");
    }

    private static void importSSTables()
    {
        logger.info("Import path: {}", SSTABLE_PATH);

        // Check if directory exists
        File sstableDir = new File(SSTABLE_PATH);
        if (!sstableDir.exists())
        {
            logger.error("ERROR: SSTable directory does not exist: {}", SSTABLE_PATH);
            throw new RuntimeException("SSTable directory not found: " + SSTABLE_PATH);
        }

        File[] files = sstableDir.listFiles();
        if (files == null || files.length == 0)
        {
            logger.error("ERROR: SSTable directory is empty: {}", SSTABLE_PATH);
            throw new RuntimeException("SSTable directory is empty: " + SSTABLE_PATH);
        }

        logger.info("Found {} files in SSTable directory:", files.length);
        for (File file : files)
        {
            logger.info("  - {}", file.getName());
        }

        try
        {
            store = Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE);
            logger.info("Opened ColumnFamilyStore for {}.{}", KEYSPACE, TABLE);
            logger.info("Starting SSTable import...");
            store.importNewSSTables(
                Collections.singleton(SSTABLE_PATH),
                true,  // resetLevel
                true,  // clearRepaired
                false, // verifySSTables
                false, // verifyTokens
                true,  // invalidateCaches
                false, // extendedVerify
                true   // copyData
            );

            int sstableCount = store.getLiveSSTables().size();
            logger.info("Import completed. Live SSTables count: {}", sstableCount);
        }
        catch (Exception e)
        {
            logger.error("ERROR during SSTable import", e);
            throw new RuntimeException("SSTable import failed", e);
        }
    }

    /**
     * Helper class to store compression test results
     */
    private static class CompressionResult
    {
        final String type;                   // "LZ4", "Zstd", or "Zstd+Dict"
        final int level;
        final long compressedSizeBytes;      // On-disk size of compressed SSTable
        final long uncompressedSizeBytes;    // Original uncompressed data size
        final double compressionRatio;
        final long durationMs;
        final long cpuTimeMs;

        CompressionResult(String type, int level, long compressedSizeBytes, long uncompressedSizeBytes,
                          long durationMs, long cpuTimeMs)
        {
            this.type = type;
            this.level = level;
            this.compressedSizeBytes = compressedSizeBytes;
            this.uncompressedSizeBytes = uncompressedSizeBytes;
            this.compressionRatio = (double) compressedSizeBytes / uncompressedSizeBytes;
            this.durationMs = durationMs;
            this.cpuTimeMs = cpuTimeMs;
        }

        @Override
        public String toString()
        {
            return String.format("%s Level %2d: Ratio=%.4f, Compressed=%,d MB, Uncompressed=%,d MB, Duration=%,d ms, CPU=%,d ms",
                                 type,
                                 level,
                                 compressionRatio,
                                 compressedSizeBytes / 1024 / 1024,
                                 uncompressedSizeBytes / 1024 / 1024,
                                 durationMs,
                                 cpuTimeMs);
        }
    }

    /**
     * Capture baseline LZ4 compression metrics
     */
    private CompressionResult captureBaselineMetrics() throws Exception
    {
        logger.info("========================================");
        logger.info("Capturing LZ4 Baseline Metrics");
        logger.info("========================================");

        long compressedSize = 0;
        long uncompressedSize = 0;

        for (org.apache.cassandra.io.sstable.format.SSTableReader sstable : store.getLiveSSTables())
        {
            compressedSize += sstable.onDiskLength();
            uncompressedSize += sstable.uncompressedLength();
        }

        CompressionResult baseline = new CompressionResult("LZ4", 17, compressedSize, uncompressedSize, 0, 0);

        logger.info("Baseline (LZ4 High Level 17):");
        logger.info("  - Compressed size: {} bytes ({} MB)", compressedSize, compressedSize / 1024 / 1024);
        logger.info("  - Uncompressed size: {} bytes ({} MB)", uncompressedSize, uncompressedSize / 1024 / 1024);
        logger.info("  - Compression ratio: {}", String.format("%.4f", baseline.compressionRatio));
        logger.info("");

        return baseline;
    }

    /**
     * Test Zstd compression (without dictionary) at a specific level
     */
    private CompressionResult testZstdCompression(int compressionLevel) throws Exception
    {
        logger.info("========================================");
        logger.info("Testing Zstd Compression Level {}", compressionLevel);
        logger.info("========================================");

        // Set Zstd compression parameters (without dictionary)
        logger.info("Setting compression to Zstd level {} (no dictionary)", compressionLevel);
        store.setCompressionParametersJson(
            "{\"chunk_length_in_kb\": \"64\", " +
            "\"class\": \"org.apache.cassandra.io.compress.ZstdCompressor\", " +
            "\"compression_level\": \"" + compressionLevel + "\"}"
        );

        return performRecompression("Zstd", compressionLevel);
    }

    /**
     * Test Zstd compression with dictionary at a specific level
     */
    private CompressionResult testZstdDictionaryCompression(int compressionLevel) throws Exception
    {
        logger.info("========================================");
        logger.info("Testing Zstd+Dictionary Compression Level {}", compressionLevel);
        logger.info("========================================");

        // Set Zstd dictionary compression parameters
        logger.info("Setting compression to Zstd level {} WITH dictionary", compressionLevel);
        store.setCompressionParametersJson(
            "{\"chunk_length_in_kb\": \"64\", " +
            "\"class\": \"org.apache.cassandra.io.compress.ZstdDictionaryCompressor\", " +
            "\"compression_level\": \"" + compressionLevel + "\"}"
        );

        return performRecompression("Zstd+Dict", compressionLevel);
    }

    /**
     * Common method to perform recompression and collect metrics
     */
    private CompressionResult performRecompression(String compressionType, int compressionLevel) throws Exception
    {
        // Get CPU time tracker
        OperatingSystemMXBean osBean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        long cpuTimeBefore = osBean.getProcessCpuTime();

        // Perform recompression
        logger.info("Starting SSTable rewrite with {} level {}...", compressionType, compressionLevel);
        long startTime = System.nanoTime();

        StorageService.instance.upgradeSSTables(KEYSPACE, false, TABLE);

        long duration = System.nanoTime() - startTime;
        long durationMs = duration / 1_000_000;

        // Capture CPU time
        long cpuTimeAfter = osBean.getProcessCpuTime();
        long cpuTimeMs = (cpuTimeAfter - cpuTimeBefore) / 1_000_000;

        // Get SSTable sizes (THESE ARE DETERMINISTIC - same input + same compression level = same output)
        long compressedSizeBytes = 0;
        long uncompressedSizeBytes = 0;
        int sstableCount = 0;

        for (org.apache.cassandra.io.sstable.format.SSTableReader sstable : store.getLiveSSTables())
        {
            long sstableCompressed = sstable.onDiskLength();
            long sstableUncompressed = sstable.uncompressedLength();

            compressedSizeBytes += sstableCompressed;
            uncompressedSizeBytes += sstableUncompressed;
            sstableCount++;

            logger.info("  SSTable #{}: compressed={} bytes, uncompressed={} bytes",
                       sstableCount, sstableCompressed, sstableUncompressed);
        }

        double compressionRatio = (double) compressedSizeBytes / uncompressedSizeBytes;
        long compressedMB = compressedSizeBytes / 1024 / 1024;
        long uncompressedMB = uncompressedSizeBytes / 1024 / 1024;
        long savedBytes = uncompressedSizeBytes - compressedSizeBytes;
        long savedMB = savedBytes / 1024 / 1024;
        double spaceReductionPercent = ((double) savedBytes / uncompressedSizeBytes) * 100;

        logger.info("Recompression completed in {} ms", durationMs);
        logger.info("Results:");
        logger.info("  - SSTable count: {}", sstableCount);
        logger.info("  - Compressed size: {} bytes ({} MB)", compressedSizeBytes, compressedMB);
        logger.info("  - Uncompressed size: {} bytes ({} MB)", uncompressedSizeBytes, uncompressedMB);
        logger.info("  - Space saved: {} bytes ({} MB = {}%)", savedBytes, savedMB, String.format("%.2f", spaceReductionPercent));
        logger.info("  - Compression ratio: {}", String.format("%.4f", compressionRatio));
        logger.info("  - CPU time used: {} ms", cpuTimeMs);
        logger.info("  - Wall clock duration: {} ms", durationMs);

        return new CompressionResult(compressionType, compressionLevel, compressedSizeBytes, uncompressedSizeBytes,
                                     durationMs, cpuTimeMs);
    }

    /**
     * Print a summary table of all compression results
     */
    private void printResultsSummary(List<CompressionResult> zstdResults,
                                     List<CompressionResult> zstdDictResults,
                                     CompressionResult baseline)
    {
        logger.info("");
        logger.info("================================================================================");
        logger.info("                    COMPRESSION RESULTS SUMMARY");
        logger.info("================================================================================");
        logger.info("");
        logger.info("Baseline (LZ4 High Level 17):");
        logger.info("  - Compressed size: {} MB", baseline.compressedSizeBytes / 1024 / 1024);
        logger.info("  - Uncompressed size: {} MB", baseline.uncompressedSizeBytes / 1024 / 1024);
        logger.info("  - Compression ratio: {}", String.format("%.4f", baseline.compressionRatio));
        logger.info("");
        logger.info("--------------------------------------------------------------------------------");
        logger.info(String.format("%-12s | %-6s | %-10s | %-16s | %-12s | %-12s | %-16s",
                                  "Type", "Level", "Ratio", "Compressed (MB)", "Duration (ms)", "CPU Time (ms)", "vs Baseline"));
        logger.info("--------------------------------------------------------------------------------");

        // Print Zstd results
        for (CompressionResult result : zstdResults)
        {
            printResultRow(result, baseline);
        }

        // Print Zstd+Dict results
        for (CompressionResult result : zstdDictResults)
        {
            printResultRow(result, baseline);
        }

        logger.info("================================================================================");
        logger.info("");

        // Print analysis
        logger.info("ANALYSIS:");
        logger.info("");

        // Combine all results for analysis
        List<CompressionResult> allResults = new ArrayList<>();
        allResults.addAll(zstdResults);
        allResults.addAll(zstdDictResults);

        // Find best compression ratio
        CompressionResult bestRatio = allResults.stream()
            .min((r1, r2) -> Double.compare(r1.compressionRatio, r2.compressionRatio))
            .orElse(null);
        if (bestRatio != null)
        {
            long savedBytes = baseline.compressedSizeBytes - bestRatio.compressedSizeBytes;
            logger.info("  Best compression ratio: {} Level {} with {} ({} MB compressed, saved {} MB vs baseline)",
                        bestRatio.type, bestRatio.level, String.format("%.4f", bestRatio.compressionRatio),
                        bestRatio.compressedSizeBytes / 1024 / 1024,
                        savedBytes / 1024 / 1024);
        }

        // Find fastest compression
        CompressionResult fastest = allResults.stream()
            .min((r1, r2) -> Long.compare(r1.durationMs, r2.durationMs))
            .orElse(null);
        if (fastest != null)
        {
            logger.info("  Fastest compression: {} Level {} in {} ms",
                        fastest.type, fastest.level, fastest.durationMs);
        }

        // Find lowest CPU time
        CompressionResult lowestCpu = allResults.stream()
            .min((r1, r2) -> Long.compare(r1.cpuTimeMs, r2.cpuTimeMs))
            .orElse(null);
        if (lowestCpu != null)
        {
            logger.info("  Lowest CPU usage: {} Level {} with {} ms CPU time",
                        lowestCpu.level, lowestCpu.type, lowestCpu.cpuTimeMs);
        }

        logger.info("");
        logger.info("NOTE: Compressed/uncompressed sizes are DETERMINISTIC (same every run).");
        logger.info("      Duration and CPU metrics may vary between runs.");
        logger.info("");
        logger.info("================================================================================");
    }

    /**
     * Helper method to print a single result row
     */
    private void printResultRow(CompressionResult result, CompressionResult baseline)
    {
        double sizeDiffPercent = ((double) (result.compressedSizeBytes - baseline.compressedSizeBytes) / baseline.compressedSizeBytes) * 100;
        long savedMB = (baseline.compressedSizeBytes - result.compressedSizeBytes) / 1024 / 1024;

        logger.info(String.format("%-12s | %-6d | %-10.4f | %,16d | %,12d | %,12d | %+6d MB (%+.2f%%)",
                                  result.type,
                                  result.level,
                                  result.compressionRatio,
                                  result.compressedSizeBytes / 1024 / 1024,
                                  result.durationMs,
                                  result.cpuTimeMs,
                                  savedMB,
                                  -sizeDiffPercent));
    }

    @Test
    public void testZstdCompressionLevels8To15() throws Exception
    {
        logger.info("================================================================================");
        logger.info("   COMPRESSION COMPARISON TEST: LZ4 vs Zstd vs Zstd+Dictionary (Levels 8-15)");
        logger.info("================================================================================");
        logger.info("");

        if (store.getLiveSSTables().isEmpty())
        {
            logger.error("ABORTING: No SSTables available for testing");
            throw new RuntimeException("No SSTables to compress");
        }

        // 1. Capture baseline LZ4 metrics
        CompressionResult baseline = captureBaselineMetrics();

        // 2. Test Zstd compression (without dictionary) for levels 8-15
        logger.info("================================================================================");
        logger.info("PHASE 1: Testing Zstd Compression (NO Dictionary)");
        logger.info("================================================================================");
        logger.info("");

        List<CompressionResult> zstdResults = new ArrayList<>();
        for (int level = 8; level <= 15; level++)
        {
            try
            {
                CompressionResult result = testZstdCompression(level);
                zstdResults.add(result);

                // Brief pause between tests to allow system to stabilize
                Thread.sleep(2000);
            }
            catch (Exception e)
            {
                logger.error("ERROR testing Zstd compression level {}", level, e);
                throw e;
            }
        }

        // 3. Test Zstd+Dictionary compression for levels 8-15
        logger.info("");
        logger.info("================================================================================");
        logger.info("PHASE 2: Testing Zstd Compression WITH Dictionary");
        logger.info("================================================================================");
        logger.info("");

        List<CompressionResult> zstdDictResults = new ArrayList<>();
        for (int level = 8; level <= 15; level++)
        {
            try
            {
                CompressionResult result = testZstdDictionaryCompression(level);
                zstdDictResults.add(result);

                // Brief pause between tests to allow system to stabilize
                Thread.sleep(2000);
            }
            catch (Exception e)
            {
                logger.error("ERROR testing Zstd+Dict compression level {}", level, e);
                throw e;
            }
        }

        // 4. Print comprehensive summary comparing all three approaches
        printResultsSummary(zstdResults, zstdDictResults, baseline);
    }
}