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
import java.util.Collections;

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
 * Test to measure LZ4 High Level 17 compression metrics (baseline)
 */
public class LZ4CompressionTest
{
    private static final Logger logger = LoggerFactory.getLogger(LZ4CompressionTest.class);
//    private static final String KEYSPACE = "p01_clouddb";
    private static final String KEYSPACE = "p115_clouddb";
    private static final String TABLE = "custom_zone_records";
//    private static final String SSTABLE_PATH = "/Users/minalkyada/Desktop/p01_clouddb/custom_zone_records-5fb29180edd8300084c8712786214e10";
    private static final String SSTABLE_PATH = "/Users/minalkyada/Desktop/github/forked/cassandra/test/long/org/apache/cassandra/io/compress/level4SST/p115_clouddb/custom_zone_records-00000000000000000000000003986882";

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

    @Test
    public void testLZ4HighLevel17Metrics() throws Exception
    {
        logger.info("================================================================================");
        logger.info("              LZ4 HIGH LEVEL 17 COMPRESSION METRICS (BASELINE)");
        logger.info("================================================================================");
        logger.info("");

        if (store.getLiveSSTables().isEmpty())
        {
            logger.error("ABORTING: No SSTables available for testing");
            throw new RuntimeException("No SSTables to measure");
        }

        // Capture baseline metrics
        long compressedSize = 0;
        long uncompressedSize = 0;
        int sstableCount = 0;

        for (org.apache.cassandra.io.sstable.format.SSTableReader sstable : store.getLiveSSTables())
        {
            long sstableCompressed = sstable.onDiskLength();
            long sstableUncompressed = sstable.uncompressedLength();

            compressedSize += sstableCompressed;
            uncompressedSize += sstableUncompressed;
            sstableCount++;

            logger.info("SSTable #{}: compressed={} bytes, uncompressed={} bytes",
                       sstableCount, sstableCompressed, sstableUncompressed);
        }

        double compressionRatio = (double) compressedSize / uncompressedSize;
        long compressedMB = compressedSize / 1024 / 1024;
        long uncompressedMB = uncompressedSize / 1024 / 1024;
        long savedBytes = uncompressedSize - compressedSize;
        long savedMB = savedBytes / 1024 / 1024;
        double spaceReductionPercent = ((double) savedBytes / uncompressedSize) * 100;

        logger.info("");
        logger.info("================================================================================");
        logger.info("                          RESULTS SUMMARY");
        logger.info("================================================================================");
        logger.info("Compression Type: LZ4 High Level 17");
        logger.info("SSTable count: {}", sstableCount);
        logger.info("Compressed size: {} bytes ({} MB)", compressedSize, compressedMB);
        logger.info("Uncompressed size: {} bytes ({} MB)", uncompressedSize, uncompressedMB);
        logger.info("Space saved: {} bytes ({} MB = {}%)", savedBytes, savedMB, String.format("%.2f", spaceReductionPercent));
        logger.info("Compression ratio: {}", String.format("%.4f", compressionRatio));
        logger.info("");
        logger.info("NOTE: These metrics represent the baseline for comparison with Zstd.");
        logger.info("================================================================================");
    }
}