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

import com.sun.management.OperatingSystemMXBean;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compression.*;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.schema.*;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.db.compaction.OperationType;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.util.*;

public class ZstdDictionaryCompressionTest {
    private static final Logger logger = LoggerFactory.getLogger(ZstdDictionaryCompressionTest.class);
    private static final String KEYSPACE = "p115_clouddb";
    private static final String TABLE = "custom_zone_records";
    private static final String SSTABLE_PATH = "/Users/minalkyada/Desktop/github/forked/cassandra/test/long/org/apache/cassandra/io/compress/level4SST/p115_clouddb/custom_zone_records-00000000000000000000000003986882";

    private static final int DICT_SIZE = 64 * 1024;  // 64KB dictionary
    private static final int MAX_SAMPLES = 1000;     // Maximum number of samples to collect
    private static final int SAMPLE_SIZE = 16 * 1024; // 16KB per sample

    private static ColumnFamilyStore store;
    private static CompressionDictionary dictionary;

    /**
     * Helper class to store compression test results
     */
    private static class CompressionResult
    {
        final int level;
        final long compressedSizeBytes;
        final long uncompressedSizeBytes;
        final double compressionRatio;
        final long durationMs;
        final long cpuTimeMs;
        final int iterations;

        CompressionResult(int level, long compressedSizeBytes, long uncompressedSizeBytes,
                          long durationMs, long cpuTimeMs, int iterations)
        {
            this.level = level;
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
        // Create table with ZstdDictionaryCompressor enabled
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
                                "    AND compression = {" +
                                "        'chunk_length_in_kb': '64', " +
                                "        'class': 'org.apache.cassandra.io.compress.ZstdDictionaryCompressor', " +
                                "        'compression_level': '3', " +
                                "        'enabled': 'true'" +
                                "    };"
                        , KEYSPACE)
                .build();

        SchemaLoader.createKeyspace(KEYSPACE, KeyspaceParams.simple(1), customZone);
    }

    private static void importSSTables()
    {
        File sstableDir = new File(SSTABLE_PATH);
        if (!sstableDir.exists())
        {
            throw new RuntimeException("SSTable directory not found: " + SSTABLE_PATH);
        }

        File[] files = sstableDir.listFiles();
        if (files == null || files.length == 0)
        {
            throw new RuntimeException("SSTable directory is empty: " + SSTABLE_PATH);
        }

        logger.info("Found {} files in SSTable directory: {}", files.length, SSTABLE_PATH);
        for (File file : files)
        {
            logger.info("  - {}", file.getName());
        }

        try
        {
            store = Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE);
            logger.info("Importing SSTables from: {}", SSTABLE_PATH);

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

            if (sstableCount == 0)
            {
                throw new RuntimeException("No SSTables imported! Check that SSTable files are valid and match the schema.");
            }

            // Train dictionary from imported SSTables
            trainDictionary();
        }
        catch (Exception e)
        {
            logger.error("ERROR during SSTable import", e);
            throw new RuntimeException("SSTable import failed", e);
        }
    }

    /**
     * Train a compression dictionary from the imported SSTable data and register it with Cassandra
     */
    private static void trainDictionary()
    {
        logger.info("================================================================================");
        logger.info("TRAINING ZSTD COMPRESSION DICTIONARY FROM SSTABLE DATA");
        logger.info("================================================================================");

        try
        {
            CompressionParams compressionParams = store.metadata().params.compression;
            CompressionDictionaryTrainingConfig config = CompressionDictionaryTrainingConfig.builder()
                    .maxDictionarySize(DICT_SIZE)
                    .maxTotalSampleSize(MAX_SAMPLES * SAMPLE_SIZE)
                    .samplingRate(1.0f)
                    .chunkSize(SAMPLE_SIZE)
                    .build();

            ICompressionDictionaryTrainer trainer = ICompressionDictionaryTrainer.create(
                    KEYSPACE,
                    TABLE,
                    compressionParams,
                    config
            );

            if (!trainer.start(true))
            {
                throw new RuntimeException("Failed to start dictionary trainer");
            }

            SSTableChunkSampler.sampleFromSSTables(
                    store.getLiveSSTables(),
                    trainer,
                    config
            );

            dictionary = trainer.trainDictionary(false);

            if (dictionary == null)
            {
                throw new RuntimeException("Dictionary training failed - returned null");
            }

            logger.info("Dictionary trained successfully:");
            logger.info("  - Dictionary size: {} bytes ({} KB)",
                    dictionary.rawDictionary().length,
                    dictionary.rawDictionary().length / 1024);
            logger.info("  - Dictionary ID: {}", dictionary.dictId());
            logger.info("  - Dictionary Kind: {}", dictionary.kind());

            SystemDistributedKeyspace.storeCompressionDictionary(KEYSPACE, TABLE, dictionary);
            store = Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE);

            CompressionDictionaryManager dictionaryManager = store.compressionDictionaryManager();
            if (dictionaryManager != null && dictionaryManager.isEnabled())
            {
                logger.info("Waiting 10 seconds for dictionary to load into cache...");
                Thread.sleep(10000);
            }
            else
            {
                logger.warn("Dictionary manager is not enabled or not available");
            }

            logger.info("================================================================================");

            trainer.close();
        }
        catch (Exception e)
        {
            logger.error("FATAL ERROR during dictionary training", e);
            throw new RuntimeException("Dictionary training failed", e);
        }
    }

    @AfterClass
    public static void tearDown()
    {
        if (dictionary != null)
        {
            dictionary.close();
        }
    }

    /**
     * Update compression parameters for the table with specified level
     */
    private void updateCompressionLevel(int compressionLevel) throws Exception
    {
        Map<String, String> compressionOptions = new HashMap<>();
        compressionOptions.put("class", "org.apache.cassandra.io.compress.ZstdDictionaryCompressor");
        compressionOptions.put("chunk_length_in_kb", "64");
        compressionOptions.put("compression_level", String.valueOf(compressionLevel));
        compressionOptions.put("enabled", "true");

        CompressionParams newCompressionParams = CompressionParams.fromMap(compressionOptions);

        TableMetadata currentMetadata = store.metadata();
        TableMetadata.Builder metadataBuilder = currentMetadata.unbuild();
        metadataBuilder.compression(newCompressionParams);
        TableMetadata newMetadata = metadataBuilder.build();

        KeyspaceMetadata keyspaceMetadata = Keyspace.open(KEYSPACE).getMetadata();
        Tables updatedTables = keyspaceMetadata.tables.withSwapped(newMetadata);
        KeyspaceMetadata updatedKeyspace = keyspaceMetadata.withSwapped(updatedTables);
        SchemaTestUtil.submit(metadata -> metadata.schema.getKeyspaces().withAddedOrUpdated(updatedKeyspace));
    }

    /**
     * Perform recompression and collect metrics for a single iteration
     */
    private SingleIterationMetrics performSingleCompression() throws Exception
    {
        OperatingSystemMXBean osBean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        long cpuTimeBefore = osBean.getProcessCpuTime();

        long startTime = System.nanoTime();
        StorageService.instance.upgradeSSTables(KEYSPACE, false, TABLE);
        long duration = System.nanoTime() - startTime;
        long durationMs = duration / 1_000_000;

        long cpuTimeAfter = osBean.getProcessCpuTime();
        long cpuTimeMs = (cpuTimeAfter - cpuTimeBefore) / 1_000_000;

        long compressedSizeBytes = 0;
        long uncompressedSizeBytes = 0;

        for (org.apache.cassandra.io.sstable.format.SSTableReader sstable : store.getLiveSSTables())
        {
            compressedSizeBytes += sstable.onDiskLength();
            uncompressedSizeBytes += sstable.uncompressedLength();
        }

        return new SingleIterationMetrics(compressedSizeBytes, uncompressedSizeBytes, durationMs, cpuTimeMs);
    }

    /**
     * Verify dictionary attachment to compressor
     */
    private void verifyDictionaryAttachment(int compressionLevel)
    {
        for (org.apache.cassandra.io.sstable.format.SSTableReader sstable : store.getLiveSSTables())
        {
            CompressionMetadata sstableCompression = sstable.getCompressionMetadata();
            if (sstableCompression != null)
            {
                ICompressor compressor = sstableCompression.compressor();
                if (compressor instanceof ZstdDictionaryCompressor)
                {
                    ZstdDictionaryCompressor dictCompressor = (ZstdDictionaryCompressor) compressor;
                    boolean hasDictionary = dictCompressor.dictionary() != null;
                    String dictId = hasDictionary ? String.valueOf(dictCompressor.dictionary().dictId()) : "NONE";

                    logger.info("Level {}: compressor={}, level={}, hasDictionary={}, dictId={}",
                            compressionLevel,
                            compressor.getClass().getSimpleName(),
                            dictCompressor.compressionLevel(),
                            hasDictionary,
                            dictId);

                    if (!hasDictionary)
                    {
                        logger.warn("WARNING [Level {}]: Dictionary NOT attached to compressor!", compressionLevel);
                    }
                }
            }
        }
    }

    /**
     * Test compression at specified level with multiple iterations and average the results
     */
    private CompressionResult testZstdDictionaryCompressionWithIterations(int compressionLevel, int iterations) throws Exception
    {
        logger.info("Testing level {} with {} iterations", compressionLevel, iterations);

        updateCompressionLevel(compressionLevel);

        long totalCompressedSize = 0;
        long totalDuration = 0;
        long totalCpuTime = 0;
        long uncompressedSizeBytes = 0;  // Uncompressed size is the same for all iterations

        for (int i = 1; i <= iterations; i++)
        {
            logger.info("  Iteration {}/{} for level {}", i, iterations, compressionLevel);

            SingleIterationMetrics metrics = performSingleCompression();

            totalCompressedSize += metrics.compressedSizeBytes;
            totalDuration += metrics.durationMs;
            totalCpuTime += metrics.cpuTimeMs;

            // Uncompressed size is deterministic and same for all iterations, so just capture it once
            if (i == 1)
            {
                uncompressedSizeBytes = metrics.uncompressedSizeBytes;
            }

            if (i < iterations)
            {
                ZstdDictionaryCompressor.invalidateCache();
                Thread.sleep(1000);
            }
        }

        // Verify dictionary attachment (check once at the end)
        verifyDictionaryAttachment(compressionLevel);

        // Calculate averages
        long avgCompressedSize = totalCompressedSize / iterations;
        long avgDuration = totalDuration / iterations;
        long avgCpuTime = totalCpuTime / iterations;

        return new CompressionResult(compressionLevel, avgCompressedSize, uncompressedSizeBytes,
                                     avgDuration, avgCpuTime, iterations);
    }

    /**
     * Helper class to store metrics from a single compression iteration
     */
    private static class SingleIterationMetrics
    {
        final long compressedSizeBytes;
        final long uncompressedSizeBytes;
        final long durationMs;
        final long cpuTimeMs;

        SingleIterationMetrics(long compressedSizeBytes, long uncompressedSizeBytes, long durationMs, long cpuTimeMs)
        {
            this.compressedSizeBytes = compressedSizeBytes;
            this.uncompressedSizeBytes = uncompressedSizeBytes;
            this.durationMs = durationMs;
            this.cpuTimeMs = cpuTimeMs;
        }
    }

    @Test
    public void testZstdDictionaryCompressionLevels8To19() throws Exception
    {
        final int ITERATIONS = 5;

        logger.info("================================================================================");
        logger.info("   ZSTD+DICTIONARY COMPRESSION TEST - LEVELS 8-19 ({} iterations per level)", ITERATIONS);
        logger.info("================================================================================");
        logger.info("");

        if (store.getLiveSSTables().isEmpty())
        {
            throw new RuntimeException("No SSTables to compress");
        }

        // Capture baseline with level 8 (averaged over 5 iterations)
        CompressionResult baseline = testZstdDictionaryCompressionWithIterations(8, ITERATIONS);
        ZstdDictionaryCompressor.invalidateCache();
        Thread.sleep(2000);

        // Test compression levels 9 through 19 (each averaged over 5 iterations)
        List<CompressionResult> results = new ArrayList<>();
        for (int level = 9; level <= 19; level++)
        {
            try
            {
                CompressionResult result = testZstdDictionaryCompressionWithIterations(level, ITERATIONS);
                results.add(result);

                ZstdDictionaryCompressor.invalidateCache();
                Thread.sleep(2000);
            }
            catch (Exception e)
            {
                logger.error("ERROR testing Zstd+Dictionary compression level {}", level, e);
                throw e;
            }
        }

        printResultsSummary(results, baseline);
    }

    private void printResultsSummary(List<CompressionResult> results, CompressionResult baseline)
    {
        logger.info("");
        logger.info("================================================================================");
        logger.info("       ZSTD+DICTIONARY COMPRESSION RESULTS (AVERAGED OVER {} ITERATIONS)", baseline.iterations);
        logger.info("================================================================================");
        logger.info("");
        logger.info("BASELINE (Zstd+Dictionary Level 8):");
        logger.info("  Compressed: {} MB  |  Uncompressed: {} MB  |  Ratio: {}  |  CPU: {} ms  |  Duration: {} ms",
                baseline.compressedSizeBytes / 1024 / 1024,
                baseline.uncompressedSizeBytes / 1024 / 1024,
                String.format("%.4f", baseline.compressionRatio),
                baseline.cpuTimeMs,
                baseline.durationMs);
        logger.info("");
        logger.info("--------------------------------------------------------------------------------");
        logger.info(String.format("%-6s | %-10s | %-16s | %-12s | %-12s | %-20s",
                "Level", "Ratio", "Compressed (MB)", "Duration (ms)", "CPU Time (ms)", "vs Baseline"));
        logger.info("--------------------------------------------------------------------------------");

        for (CompressionResult result : results)
        {
            double sizeDiffPercent = ((double) (result.compressedSizeBytes - baseline.compressedSizeBytes) / baseline.compressedSizeBytes) * 100;
            long savedMB = (baseline.compressedSizeBytes - result.compressedSizeBytes) / 1024 / 1024;

            logger.info(String.format("%-6d | %-10.4f | %,16d | %,12d | %,12d | %+6d MB (%+.2f%%)",
                    result.level,
                    result.compressionRatio,
                    result.compressedSizeBytes / 1024 / 1024,
                    result.durationMs,
                    result.cpuTimeMs,
                    savedMB,
                    -sizeDiffPercent));
        }

        logger.info("================================================================================");
        logger.info("");
        logger.info("NOTE: All metrics (Compressed size, Duration, CPU time) are averaged over {} iterations.", baseline.iterations);
        logger.info("      Compressed size is deterministic; Duration and CPU time may vary between runs.");
        logger.info("================================================================================");
    }

    @Test
    public void testCrossSStableZstdDictionaryCompression() throws Exception
    {
        final int ITERATIONS = 5;
        final String TRAINING_SSTABLE_PATH = "/Users/minalkyada/Desktop/github/forked/cassandra/test/long/org/apache/cassandra/io/compress/level4SST/p115_clouddb/custom_zone_records-00000000000000000000000003986882";
        final String COMPRESSION_SSTABLE_PATH = "/Users/minalkyada/Desktop/github/forked/cassandra/test/long/org/apache/cassandra/io/compress/level4SST/p115_clouddb/custom_zone_records-00000000000000000000000005640577";

        logger.info("================================================================================");
        logger.info("  CROSS-SSTABLE DICTIONARY COMPRESSION TEST - LEVELS 8-19");
        logger.info("  Training Dictionary: custom_zone_records-3986882");
        logger.info("  Compressing SSTable: custom_zone_records-5640577");
        logger.info("================================================================================");
        logger.info("");

        try
        {
            // Step 1: Import training SSTable and train dictionary
            importAndTrainDictionary(TRAINING_SSTABLE_PATH);

            // Step 2: Clear SSTables and import the target SSTable for compression
            clearAndImportTargetSSTable(COMPRESSION_SSTABLE_PATH);

            // Step 3: Run compression tests on all levels (8-19)
            logger.info("Starting compression tests on target SSTable with trained dictionary");
            logger.info("");

            // Capture baseline with level 8
            CompressionResult baseline = testZstdDictionaryCompressionWithIterations(8, ITERATIONS);
            ZstdDictionaryCompressor.invalidateCache();
            Thread.sleep(2000);

            // Test compression levels 9 through 19
            List<CompressionResult> results = new ArrayList<>();
            for (int level = 9; level <= 19; level++)
            {
                try
                {
                    CompressionResult result = testZstdDictionaryCompressionWithIterations(level, ITERATIONS);
                    results.add(result);

                    ZstdDictionaryCompressor.invalidateCache();
                    Thread.sleep(2000);
                }
                catch (Exception e)
                {
                    logger.error("ERROR testing Zstd+Dictionary compression level {}", level, e);
                    throw e;
                }
            }

            printCrossSSTableResultsSummary(results, baseline);
        }
        catch (Exception e)
        {
            logger.error("FATAL ERROR in cross-SSTable dictionary compression test", e);
            throw e;
        }
    }

    /**
     * Import SSTable for training and train dictionary from it
     */
    private void importAndTrainDictionary(String trainingSSTablePath) throws Exception
    {
        logger.info("================================================================================");
        logger.info("STEP 1: IMPORTING TRAINING SSTABLE AND TRAINING DICTIONARY");
        logger.info("Training SSTable: {}", trainingSSTablePath);
        logger.info("================================================================================");

        // Clear all existing SSTables first (from initial setup)
        logger.info("Clearing existing SSTables from setup...");
        for (org.apache.cassandra.io.sstable.format.SSTableReader sstable : store.getLiveSSTables())
        {
            store.markObsolete(Collections.singleton(sstable), OperationType.UNKNOWN);
        }
        logger.info("Existing SSTables cleared");

        File sstableDir = new File(trainingSSTablePath);
        if (!sstableDir.exists())
        {
            throw new RuntimeException("Training SSTable directory not found: " + trainingSSTablePath);
        }

        // Import training SSTable
        store.importNewSSTables(
                Collections.singleton(trainingSSTablePath),
                true,  // resetLevel
                true,  // clearRepaired
                false, // verifySSTables
                false, // verifyTokens
                true,  // invalidateCaches
                false, // extendedVerify
                true   // copyData
        );

        int sstableCount = store.getLiveSSTables().size();
        logger.info("Imported {} SSTable(s) for training", sstableCount);

        if (sstableCount == 0)
        {
            throw new RuntimeException("No training SSTables imported!");
        }

        if (sstableCount != 1)
        {
            logger.warn("Expected 1 SSTable for training, but found {}. Proceeding with training...", sstableCount);
        }

        // Train dictionary from the imported SSTable
        trainDictionary();
    }

    /**
     * Clear existing SSTables and import the target SSTable for compression
     */
    private void clearAndImportTargetSSTable(String targetSSTablePath) throws Exception
    {
        logger.info("================================================================================");
        logger.info("STEP 2: IMPORTING TARGET SSTABLE FOR COMPRESSION");
        logger.info("Target SSTable: {}", targetSSTablePath);
        logger.info("================================================================================");

        // Clear all existing SSTables (training SSTable)
        logger.info("Clearing training SSTables...");
        for (org.apache.cassandra.io.sstable.format.SSTableReader sstable : store.getLiveSSTables())
        {
            store.markObsolete(Collections.singleton(sstable), OperationType.UNKNOWN);
        }
        logger.info("Training SSTables cleared");

        // Import target SSTable for compression
        File targetDir = new File(targetSSTablePath);
        if (!targetDir.exists())
        {
            throw new RuntimeException("Target SSTable directory not found: " + targetSSTablePath);
        }

        store.importNewSSTables(
                Collections.singleton(targetSSTablePath),
                true,  // resetLevel
                true,  // clearRepaired
                false, // verifySSTables
                false, // verifyTokens
                true,  // invalidateCaches
                false, // extendedVerify
                true   // copyData
        );

        int sstableCount = store.getLiveSSTables().size();
        logger.info("Imported {} SSTable(s) for compression", sstableCount);

        if (sstableCount == 0)
        {
            throw new RuntimeException("No target SSTables imported!");
        }

        logger.info("================================================================================");
        logger.info("");
    }

    /**
     * Print results summary for cross-SSTable compression test
     */
    private void printCrossSSTableResultsSummary(List<CompressionResult> results, CompressionResult baseline)
    {
        logger.info("");
        logger.info("================================================================================");
        logger.info("   CROSS-SSTABLE DICTIONARY COMPRESSION RESULTS (AVERAGED OVER {} ITERATIONS)", baseline.iterations);
        logger.info("================================================================================");
        logger.info("");
        logger.info("Dictionary trained on: custom_zone_records-3986882");
        logger.info("Compression applied to: custom_zone_records-5640577");
        logger.info("");
        logger.info("BASELINE (Zstd+Dictionary Level 8):");
        logger.info("  Compressed: {} MB  |  Uncompressed: {} MB  |  Ratio: {}  |  CPU: {} ms  |  Duration: {} ms",
                baseline.compressedSizeBytes / 1024 / 1024,
                baseline.uncompressedSizeBytes / 1024 / 1024,
                String.format("%.4f", baseline.compressionRatio),
                baseline.cpuTimeMs,
                baseline.durationMs);
        logger.info("");
        logger.info("--------------------------------------------------------------------------------");
        logger.info(String.format("%-6s | %-10s | %-16s | %-12s | %-12s | %-20s",
                "Level", "Ratio", "Compressed (MB)", "Duration (ms)", "CPU Time (ms)", "vs Baseline"));
        logger.info("--------------------------------------------------------------------------------");

        for (CompressionResult result : results)
        {
            double sizeDiffPercent = ((double) (result.compressedSizeBytes - baseline.compressedSizeBytes) / baseline.compressedSizeBytes) * 100;
            long savedMB = (baseline.compressedSizeBytes - result.compressedSizeBytes) / 1024 / 1024;

            logger.info(String.format("%-6d | %-10.4f | %,16d | %,12d | %,12d | %+6d MB (%+.2f%%)",
                    result.level,
                    result.compressionRatio,
                    result.compressedSizeBytes / 1024 / 1024,
                    result.durationMs,
                    result.cpuTimeMs,
                    savedMB,
                    -sizeDiffPercent));
        }

        logger.info("================================================================================");
        logger.info("");
        logger.info("NOTE: Dictionary trained on one SSTable, applied to compress a different SSTable.");
        logger.info("      All metrics averaged over {} iterations.", baseline.iterations);
        logger.info("      This tests dictionary generalization across different SSTables.");
        logger.info("================================================================================");
    }
}
