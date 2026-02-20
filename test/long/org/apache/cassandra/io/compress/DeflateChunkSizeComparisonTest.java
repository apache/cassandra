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

/**
 * Test Deflate (zlib) compression with different chunk sizes
 * to find optimal chunk_length_in_kb for compression ratio and performance
 */
public class DeflateChunkSizeComparisonTest
{
    private static final Logger logger = LoggerFactory.getLogger(DeflateChunkSizeComparisonTest.class);
    private static final String KEYSPACE = "p115_clouddb";
    private static final String TABLE = "custom_zone_records";
    private static final String SSTABLE_PATH = "/Users/minalkyada/Desktop/github/forked/cassandra/test/long/org/apache/cassandra/io/compress/level4SST/p115_clouddb/custom_zone_records-00000000000000000000000003986882";

    private static ColumnFamilyStore store;

    /**
     * Helper class to store compression test results with chunk size
     */
    private static class CompressionResult
    {
        final int chunkSizeKB;
        final int compressionLevel;
        final long compressedSizeBytes;
        final long uncompressedSizeBytes;
        final double compressionRatio;
        final long durationMs;
        final long cpuTimeMs;
        final int iterations;

        CompressionResult(int chunkSizeKB, int compressionLevel, long compressedSizeBytes, long uncompressedSizeBytes,
                          long durationMs, long cpuTimeMs, int iterations)
        {
            this.chunkSizeKB = chunkSizeKB;
            this.compressionLevel = compressionLevel;
            this.compressedSizeBytes = compressedSizeBytes;
            this.uncompressedSizeBytes = uncompressedSizeBytes;
            this.compressionRatio = (double) compressedSizeBytes / uncompressedSizeBytes;
            this.durationMs = durationMs;
            this.cpuTimeMs = cpuTimeMs;
            this.iterations = iterations;
        }
    }

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
     * Test a compression level with specific chunk size multiple times and return averaged results
     */
    private CompressionResult testDeflateCompressionWithChunkSize(int chunkSizeKB, int compressionLevel, int iterations) throws Exception
    {
        logger.info("Testing chunk_size={}KB, level={} with {} iterations", chunkSizeKB, compressionLevel, iterations);

        long totalCompressedSize = 0;
        long totalUncompressedSize = 0;
        long totalDuration = 0;
        long totalCpuTime = 0;

        for (int i = 1; i <= iterations; i++)
        {
            // Set Deflate compression parameters with specific chunk size
            store.setCompressionParametersJson(
                "{\"chunk_length_in_kb\": \"" + chunkSizeKB + "\", " +
                "\"class\": \"org.apache.cassandra.io.compress.DeflateCompressor\", " +
                "\"compression_level\": \"" + compressionLevel + "\"}"
            );

            // Get CPU time tracker
            OperatingSystemMXBean osBean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
            long cpuTimeBefore = osBean.getProcessCpuTime();

            // Perform recompression
            long startTime = System.nanoTime();
            StorageService.instance.upgradeSSTables(KEYSPACE, false, TABLE);
            long duration = System.nanoTime() - startTime;
            long durationMs = duration / 1_000_000;

            // Capture CPU time
            long cpuTimeAfter = osBean.getProcessCpuTime();
            long cpuTimeMs = (cpuTimeAfter - cpuTimeBefore) / 1_000_000;

            // Get SSTable sizes
            long compressedSizeBytes = 0;
            long uncompressedSizeBytes = 0;

            for (org.apache.cassandra.io.sstable.format.SSTableReader sstable : store.getLiveSSTables())
            {
                compressedSizeBytes += sstable.onDiskLength();
                uncompressedSizeBytes += sstable.uncompressedLength();
            }

            // Accumulate totals
            totalCompressedSize += compressedSizeBytes;
            totalUncompressedSize += uncompressedSizeBytes;
            totalDuration += durationMs;
            totalCpuTime += cpuTimeMs;

            logger.info("  Iteration {}/{}: compressed={}MB, duration={}ms, cpu={}ms",
                       i, iterations,
                       compressedSizeBytes / 1024 / 1024,
                       durationMs,
                       cpuTimeMs);

            // Brief pause between iterations
            if (i < iterations)
            {
                Thread.sleep(1000);
            }
        }

        // Calculate averages
        long avgCompressedSize = totalCompressedSize / iterations;
        long avgDuration = totalDuration / iterations;
        long avgCpuTime = totalCpuTime / iterations;

        return new CompressionResult(chunkSizeKB, compressionLevel, avgCompressedSize, totalUncompressedSize,
                                     avgDuration, avgCpuTime, iterations);
    }

    @Test
    public void testDeflateCompressionChunkSizes() throws Exception
    {
        final int ITERATIONS = 5;
        final int COMPRESSION_LEVEL = 6;  // Use level 6 (standard default) for fair comparison

        logger.info("================================================================================");
        logger.info("   DEFLATE (ZLIB) CHUNK SIZE COMPARISON TEST");
        logger.info("   Compression Level: {} (fixed)", COMPRESSION_LEVEL);
        logger.info("   Iterations per chunk size: {}", ITERATIONS);
        logger.info("================================================================================");
        logger.info("");

        if (store.getLiveSSTables().isEmpty())
        {
            throw new RuntimeException("No SSTables to compress");
        }

        // Test different chunk sizes: 16KB, 32KB, 64KB, 128KB, 256KB
        int[] chunkSizes = {16, 32, 64, 128, 256};
        List<CompressionResult> results = new ArrayList<>();

        for (int chunkSize : chunkSizes)
        {
            logger.info("Testing chunk size: {}KB", chunkSize);
            CompressionResult result = testDeflateCompressionWithChunkSize(chunkSize, COMPRESSION_LEVEL, ITERATIONS);
            results.add(result);
            Thread.sleep(2000);
            logger.info("");
        }

        // Use 64KB as baseline (industry standard)
        CompressionResult baseline = results.stream()
                                            .filter(r -> r.chunkSizeKB == 64)
                                            .findFirst()
                                            .orElse(results.get(0));

        // Print summary
        printChunkSizeComparisonSummary(results, baseline);
    }

    @Test
    public void testDeflateCompressionChunkSizesWithMultipleLevels() throws Exception
    {
        final int ITERATIONS = 3;

        logger.info("================================================================================");
        logger.info("   DEFLATE (ZLIB) CHUNK SIZE + COMPRESSION LEVEL COMPARISON TEST");
        logger.info("   Testing multiple chunk sizes at different compression levels");
        logger.info("   Iterations per configuration: {}", ITERATIONS);
        logger.info("================================================================================");
        logger.info("");

        if (store.getLiveSSTables().isEmpty())
        {
            throw new RuntimeException("No SSTables to compress");
        }

        // Test combinations of chunk sizes and compression levels
        int[] chunkSizes = {32, 64, 128};
        int[] compressionLevels = {1, 6, 9};  // Fast, Default, Best
        List<CompressionResult> results = new ArrayList<>();

        for (int chunkSize : chunkSizes)
        {
            for (int level : compressionLevels)
            {
                logger.info("Testing chunk_size={}KB, level={}", chunkSize, level);
                CompressionResult result = testDeflateCompressionWithChunkSize(chunkSize, level, ITERATIONS);
                results.add(result);
                Thread.sleep(2000);
                logger.info("");
            }
        }

        // Use 64KB + level 6 as baseline
        CompressionResult baseline = results.stream()
                                            .filter(r -> r.chunkSizeKB == 64 && r.compressionLevel == 6)
                                            .findFirst()
                                            .orElse(results.get(0));

        // Print summary
        printDetailedComparisonSummary(results, baseline);
    }

    private void printChunkSizeComparisonSummary(List<CompressionResult> results, CompressionResult baseline)
    {
        logger.info("");
        logger.info("================================================================================");
        logger.info("    DEFLATE CHUNK SIZE COMPARISON RESULTS (AVERAGED OVER {} ITERATIONS)", baseline.iterations);
        logger.info("    Compression Level: {} (fixed for all tests)", baseline.compressionLevel);
        logger.info("================================================================================");
        logger.info("");
        logger.info("BASELINE ({}KB chunks):", baseline.chunkSizeKB);
        logger.info("  Compressed: {} MB  |  Uncompressed: {} MB  |  Ratio: {}  |  CPU: {} ms  |  Duration: {} ms",
                    baseline.compressedSizeBytes / 1024 / 1024,
                    baseline.uncompressedSizeBytes / 1024 / 1024,
                    String.format("%.4f", baseline.compressionRatio),
                    baseline.cpuTimeMs,
                    baseline.durationMs);
        logger.info("");
        logger.info("--------------------------------------------------------------------------------");
        logger.info(String.format("%-10s | %-10s | %-16s | %-12s | %-12s | %-20s",
                                  "Chunk (KB)", "Ratio", "Compressed (MB)", "Duration (ms)", "CPU Time (ms)", "vs Baseline"));
        logger.info("--------------------------------------------------------------------------------");

        for (CompressionResult result : results)
        {
            double sizeDiffPercent = ((double) (result.compressedSizeBytes - baseline.compressedSizeBytes) / baseline.compressedSizeBytes) * 100;
            long sizeDiffMB = (result.compressedSizeBytes - baseline.compressedSizeBytes) / 1024 / 1024;

            logger.info(String.format("%-10d | %-10.4f | %,16d | %,12d | %,12d | %+6d MB (%+.2f%%)",
                                      result.chunkSizeKB,
                                      result.compressionRatio,
                                      result.compressedSizeBytes / 1024 / 1024,
                                      result.durationMs,
                                      result.cpuTimeMs,
                                      sizeDiffMB,
                                      sizeDiffPercent));
        }

        logger.info("================================================================================");
        logger.info("");
        logger.info("ANALYSIS:");
        logger.info("  - Larger chunks typically give better compression ratios");
        logger.info("  - Smaller chunks are better for random read performance");
        logger.info("  - 64KB is a common industry standard balance");
        logger.info("  - Consider your workload: sequential scans vs random reads");
        logger.info("================================================================================");
    }

    private void printDetailedComparisonSummary(List<CompressionResult> results, CompressionResult baseline)
    {
        logger.info("");
        logger.info("================================================================================");
        logger.info("    DEFLATE DETAILED COMPARISON RESULTS (AVERAGED OVER {} ITERATIONS)", baseline.iterations);
        logger.info("================================================================================");
        logger.info("");
        logger.info("BASELINE ({}KB chunks, level {}):", baseline.chunkSizeKB, baseline.compressionLevel);
        logger.info("  Compressed: {} MB  |  Ratio: {}  |  CPU: {} ms  |  Duration: {} ms",
                    baseline.compressedSizeBytes / 1024 / 1024,
                    String.format("%.4f", baseline.compressionRatio),
                    baseline.cpuTimeMs,
                    baseline.durationMs);
        logger.info("");
        logger.info("--------------------------------------------------------------------------------");
        logger.info(String.format("%-10s | %-6s | %-10s | %-16s | %-12s | %-12s | %-20s",
                                  "Chunk (KB)", "Level", "Ratio", "Compressed (MB)", "Duration (ms)", "CPU (ms)", "vs Baseline"));
        logger.info("--------------------------------------------------------------------------------");

        for (CompressionResult result : results)
        {
            double sizeDiffPercent = ((double) (result.compressedSizeBytes - baseline.compressedSizeBytes) / baseline.compressedSizeBytes) * 100;
            long sizeDiffMB = (result.compressedSizeBytes - baseline.compressedSizeBytes) / 1024 / 1024;

            logger.info(String.format("%-10d | %-6d | %-10.4f | %,16d | %,12d | %,12d | %+6d MB (%+.2f%%)",
                                      result.chunkSizeKB,
                                      result.compressionLevel,
                                      result.compressionRatio,
                                      result.compressedSizeBytes / 1024 / 1024,
                                      result.durationMs,
                                      result.cpuTimeMs,
                                      sizeDiffMB,
                                      sizeDiffPercent));
        }

        logger.info("================================================================================");
        logger.info("");
        logger.info("KEY FINDINGS:");
        logger.info("  - Compression level has MORE impact than chunk size on ratio");
        logger.info("  - Larger chunks + higher levels = best compression ratio");
        logger.info("  - Smaller chunks + lower levels = best performance");
        logger.info("  - 64KB + level 6 is the standard balanced configuration");
        logger.info("================================================================================");
    }
}