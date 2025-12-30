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

package org.apache.cassandra.cql3.validation.operations;

import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.io.compress.LZ4Compressor;
import org.apache.cassandra.io.compress.ZstdCompressor;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;

public class EnforceZstdCompressionTest extends CQLTester
{
    public static final boolean originalEnforceZstdCompression = DatabaseDescriptor.getEnforceZstdCompression();
    public static final int originalEnforceZstdCompressionLevel = DatabaseDescriptor.getEnforceZstdCompressionLevel();

    @Before
    public void init()
    {
        DatabaseDescriptor.setEnforceZstdCompression(originalEnforceZstdCompression);
        DatabaseDescriptor.setEnforceZstdCompressionLevel(originalEnforceZstdCompressionLevel);
    }

    @Test
    public void testCreateTableWithEnforceZstdCompressionEnabled() throws Throwable
    {
        DatabaseDescriptor.setEnforceZstdCompression(true);

        // Create table without specifying compression - should get ZstdCompressor
        String table1 = createTable("CREATE TABLE %s (id text PRIMARY KEY, content text);");
        assertZstdCompression(KEYSPACE, table1);

        // Create table with LZ4 compression - should be overridden to ZstdCompressor
        String table2 = createTable("CREATE TABLE %s (id text PRIMARY KEY, content text) " +
                                   "WITH compression = {'class': 'LZ4Compressor'};");
        assertZstdCompression(KEYSPACE, table2);

        // Create table with explicit zstd compression_level - should be overridden to level 5
        String table3 = createTable("CREATE TABLE %s (id text PRIMARY KEY, content text) " +
                                   "WITH compression = {'class': 'ZstdCompressor', 'compression_level': '9'};");
        assertZstdCompression(KEYSPACE, table3);

        // Create table with other compression options - should be overridden
        String table4 = createTable("CREATE TABLE %s (id text PRIMARY KEY, content text) " +
                                   "WITH compression = {'class': 'LZ4Compressor', 'chunk_length_in_kb': '32'};");
        assertZstdCompression(KEYSPACE, table4);
    }

    @Test
    public void testCreateTableWithEnforceZstdCompressionDisabled() throws Throwable
    {
        DatabaseDescriptor.setEnforceZstdCompression(false);

        // Create table without specifying compression - should get default behavior (LZ4 in tests)
        String table1 = createTable("CREATE TABLE %s (id text PRIMARY KEY, content text);");
        assertDefaultCompression(KEYSPACE, table1);

        // Create table with LZ4 compression - should remain LZ4
        String table2 = createTable("CREATE TABLE %s (id text PRIMARY KEY, content text) " +
                                   "WITH compression = {'class': 'LZ4Compressor'};");
        assertLZ4Compression(KEYSPACE, table2);

        // Create table with zstd compression - should remain zstd with specified level
        String table3 = createTable("CREATE TABLE %s (id text PRIMARY KEY, content text) " +
                                   "WITH compression = {'class': 'ZstdCompressor', 'compression_level': '9'};");
        assertZstdCompressionLevel(KEYSPACE, table3, 9);
    }

    @Test
    public void testCreateIfNotExistsWithEnforceZstdCompression() throws Throwable
    {
        DatabaseDescriptor.setEnforceZstdCompression(false);

        // Create initial table with LZ4 compression when enforcement is disabled
        String table1 = createTable("CREATE TABLE %s (id text PRIMARY KEY, content text) " +
                                   "WITH compression = {'class': 'LZ4Compressor'};");
        assertLZ4Compression(KEYSPACE, table1);

        // Enable zstd compression enforcement
        DatabaseDescriptor.setEnforceZstdCompression(true);

        // Try CREATE IF NOT EXISTS - should succeed without error and NOT change compression
        execute("CREATE TABLE IF NOT EXISTS " + KEYSPACE + "." + table1 + " (id text PRIMARY KEY, content text);");

        // Table should still have LZ4 compression (no change because table already exists)
        assertLZ4Compression(KEYSPACE, table1);

        // Try CREATE IF NOT EXISTS with different compression - should succeed and not change existing table
        execute("CREATE TABLE IF NOT EXISTS " + KEYSPACE + "." + table1 + " (id text PRIMARY KEY, content text) " +
                "WITH compression = {'class': 'ZstdCompressor', 'compression_level': '9'};");

        // Table should still have LZ4 compression (no change because table already exists)
        assertLZ4Compression(KEYSPACE, table1);

        // Create a new table to verify enforcement still works for new tables
        String table2 = createTable("CREATE TABLE %s (id text PRIMARY KEY, content text);");
        assertZstdCompression(KEYSPACE, table2);
    }

    @Test
    public void testCreateTableWithVariousCompressionOptions() throws Throwable
    {
        DatabaseDescriptor.setEnforceZstdCompression(true);

        // Test with no compression specified
        String table1 = createTable("CREATE TABLE %s (id text PRIMARY KEY, content text) " +
                                   "WITH compression = {'enabled': 'false'};");
        assertZstdCompression(KEYSPACE, table1);

        // Test with SnappyCompressor
        String table2 = createTable("CREATE TABLE %s (id text PRIMARY KEY, content text) " +
                                   "WITH compression = {'class': 'SnappyCompressor'};");
        assertZstdCompression(KEYSPACE, table2);

        // Test with DeflateCompressor
        String table3 = createTable("CREATE TABLE %s (id text PRIMARY KEY, content text) " +
                                   "WITH compression = {'class': 'DeflateCompressor'};");
        assertZstdCompression(KEYSPACE, table3);
    }

    @Test
    public void testConfigurableCompressionLevel() throws Throwable
    {
        DatabaseDescriptor.setEnforceZstdCompression(true);

        // Test with default compression level (5)
        DatabaseDescriptor.setEnforceZstdCompressionLevel(5);
        String table1 = createTable("CREATE TABLE %s (id text PRIMARY KEY, content text);");
        assertZstdCompressionLevel(KEYSPACE, table1, 5);

        // Test with different compression level (9)
        DatabaseDescriptor.setEnforceZstdCompressionLevel(9);
        String table2 = createTable("CREATE TABLE %s (id text PRIMARY KEY, content text);");
        assertZstdCompressionLevel(KEYSPACE, table2, 9);

        // Test with lowest compression level (1)
        DatabaseDescriptor.setEnforceZstdCompressionLevel(1);
        String table3 = createTable("CREATE TABLE %s (id text PRIMARY KEY, content text);");
        assertZstdCompressionLevel(KEYSPACE, table3, 1);

        // Reset to default
        DatabaseDescriptor.setEnforceZstdCompressionLevel(5);
    }

    @Test
    public void testNonSpecifiedCompressionForCreateMV() throws Throwable
    {
        // Create base table
        String baseTable = createTable("CREATE TABLE %s (id text PRIMARY KEY, content text);");

        DatabaseDescriptor.setEnforceZstdCompression(true);
        String mv1 = createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM " + KEYSPACE + "." + baseTable +
                                " WHERE id IS NOT NULL AND content IS NOT NULL PRIMARY KEY (content, id);", false);
        assertZstdCompression(KEYSPACE, mv1);

        DatabaseDescriptor.setEnforceZstdCompression(false);
        String mv2 = createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM " + KEYSPACE + "." + baseTable +
                                " WHERE id IS NOT NULL AND content IS NOT NULL PRIMARY KEY (content, id);", false);
        assertDefaultCompression(KEYSPACE, mv2);
    }

    @Test
    public void testSpecifiedCompressionForCreateMV() throws Throwable
    {
        // Create base table
        String baseTable = createTable("CREATE TABLE %s (id text PRIMARY KEY, content text);");

        DatabaseDescriptor.setEnforceZstdCompression(true);
        // Create MV with LZ4 compression - should be overridden to Zstd
        String mv1 = createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM " + KEYSPACE + "." + baseTable +
                                " WHERE id IS NOT NULL AND content IS NOT NULL PRIMARY KEY (content, id) " +
                                "WITH compression={'class': 'LZ4Compressor'};", false);
        assertZstdCompression(KEYSPACE, mv1);

        // Create MV with Snappy compression - should be overridden to Zstd
        String mv2 = createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM " + KEYSPACE + "." + baseTable +
                                " WHERE id IS NOT NULL AND content IS NOT NULL PRIMARY KEY (content, id) " +
                                "WITH compression={'class': 'SnappyCompressor'};", false);
        assertZstdCompression(KEYSPACE, mv2);

        DatabaseDescriptor.setEnforceZstdCompression(false);
        String mv3 = createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM " + KEYSPACE + "." + baseTable +
                                " WHERE id IS NOT NULL AND content IS NOT NULL PRIMARY KEY (content, id) " +
                                "WITH compression={'class': 'LZ4Compressor'};", false);
        assertLZ4Compression(KEYSPACE, mv3);
    }

    @Test
    public void testConfigurableCompressionLevelForMV() throws Throwable
    {
        // Create base table
        String baseTable = createTable("CREATE TABLE %s (id text PRIMARY KEY, content text);");

        DatabaseDescriptor.setEnforceZstdCompression(true);

        // Test with default compression level (5)
        DatabaseDescriptor.setEnforceZstdCompressionLevel(5);
        String mv1 = createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM " + KEYSPACE + "." + baseTable +
                                " WHERE id IS NOT NULL AND content IS NOT NULL PRIMARY KEY (content, id);", false);
        assertZstdCompressionLevel(KEYSPACE, mv1, 5);

        // Test with different compression level (9)
        DatabaseDescriptor.setEnforceZstdCompressionLevel(9);
        String mv2 = createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM " + KEYSPACE + "." + baseTable +
                                " WHERE id IS NOT NULL AND content IS NOT NULL PRIMARY KEY (content, id);", false);
        assertZstdCompressionLevel(KEYSPACE, mv2, 9);

        // Test with lowest compression level (1)
        DatabaseDescriptor.setEnforceZstdCompressionLevel(1);
        String mv3 = createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM " + KEYSPACE + "." + baseTable +
                                " WHERE id IS NOT NULL AND content IS NOT NULL PRIMARY KEY (content, id);", false);
        assertZstdCompressionLevel(KEYSPACE, mv3, 1);

        // Reset to default
        DatabaseDescriptor.setEnforceZstdCompressionLevel(5);
    }

    @Test
    public void testPreserveChunkLengthWithZstdEnforcement() throws Throwable
    {
        DatabaseDescriptor.setEnforceZstdCompression(true);
        DatabaseDescriptor.setEnforceZstdCompressionLevel(5);

        // Create table with custom chunk_length_in_kb - should be preserved
        String table1 = createTable("CREATE TABLE %s (id text PRIMARY KEY, content text) " +
                                   "WITH compression = {'class': 'LZ4Compressor', 'chunk_length_in_kb': '32'};");
        assertZstdCompressionWithChunkLength(KEYSPACE, table1, 5, "32");

        // Create table with different chunk_length_in_kb
        String table2 = createTable("CREATE TABLE %s (id text PRIMARY KEY, content text) " +
                                   "WITH compression = {'class': 'LZ4Compressor', 'chunk_length_in_kb': '64'};");
        assertZstdCompressionWithChunkLength(KEYSPACE, table2, 5, "64");

        // Create table without chunk_length_in_kb - should get Zstd with default chunk length
        String table3 = createTable("CREATE TABLE %s (id text PRIMARY KEY, content text) " +
                                   "WITH compression = {'class': 'LZ4Compressor'};");
        assertZstdCompressionLevel(KEYSPACE, table3, 5);
    }

    @Test
    public void testRemoveCompressorSpecificOptions() throws Throwable
    {
        DatabaseDescriptor.setEnforceZstdCompression(true);
        DatabaseDescriptor.setEnforceZstdCompressionLevel(5);

        // Create table with LZ4-specific options - they should be removed when enforcing Zstd
        String table1 = createTable("CREATE TABLE %s (id text PRIMARY KEY, content text) " +
                                   "WITH compression = {'class': 'LZ4Compressor', 'lz4_high_compressor_level': '9'};");
        assertZstdCompressionLevel(KEYSPACE, table1, 5);
        assertOptionNotPresent(KEYSPACE, table1, "lz4_high_compressor_level");

        // Create table with multiple LZ4-specific options - all should be removed
        String table2 = createTable("CREATE TABLE %s (id text PRIMARY KEY, content text) " +
                                   "WITH compression = {'class': 'LZ4Compressor', 'lz4_compressor_type': 'high', 'lz4_high_compressor_level': '12'};");
        assertZstdCompressionLevel(KEYSPACE, table2, 5);
        assertOptionNotPresent(KEYSPACE, table2, "lz4_compressor_type");
        assertOptionNotPresent(KEYSPACE, table2, "lz4_high_compressor_level");
    }

    @Test
    public void testPreserveChunkLengthButRemoveCompressorOptions() throws Throwable
    {
        DatabaseDescriptor.setEnforceZstdCompression(true);
        DatabaseDescriptor.setEnforceZstdCompressionLevel(5);

        // Create table with both chunk_length_in_kb and LZ4-specific options
        // chunk_length_in_kb should be preserved, LZ4 options should be removed
        String table1 = createTable("CREATE TABLE %s (id text PRIMARY KEY, content text) " +
                                   "WITH compression = {'class': 'LZ4Compressor', 'chunk_length_in_kb': '32', 'lz4_high_compressor_level': '9'};");
        assertZstdCompressionWithChunkLength(KEYSPACE, table1, 5, "32");
        assertOptionNotPresent(KEYSPACE, table1, "lz4_high_compressor_level");

        // Create table with chunk_length_in_kb, min_compress_ratio and LZ4-specific options
        // Standard options should be preserved, LZ4 options should be removed
        String table2 = createTable("CREATE TABLE %s (id text PRIMARY KEY, content text) " +
                                   "WITH compression = {'class': 'LZ4Compressor', 'chunk_length_in_kb': '64', 'min_compress_ratio': '1.2', 'lz4_compressor_type': 'high'};");
        assertZstdCompressionWithChunkLengthAndMinRatio(KEYSPACE, table2, 5, "64", "1.2");
        assertOptionNotPresent(KEYSPACE, table2, "lz4_compressor_type");
    }

    private void assertZstdCompression(String keyspace, String table)
    {
        assertZstdCompressionLevel(keyspace, table, DatabaseDescriptor.getEnforceZstdCompressionLevel());
    }

    private void assertZstdCompressionLevel(String keyspace, String table, int expectedLevel)
    {
        KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(keyspace);
        Assert.assertNotNull("Keyspace should exist", ksm);

        TableMetadata tm = ksm.getTableOrViewNullable(table);
        Assert.assertNotNull("Table should exist", tm);

        CompressionParams compression = tm.params.compression;
        Assert.assertTrue("Table should have compression enabled", compression.isEnabled());
        Assert.assertEquals("Should use ZstdCompressor", ZstdCompressor.class, compression.klass());

        // Check compression level in the options
        Map<String, String> options = compression.asMap();
        String compressionLevel = options.get(ZstdCompressor.COMPRESSION_LEVEL_OPTION_NAME);
        Assert.assertEquals("Compression level should be " + expectedLevel,
                           String.valueOf(expectedLevel), compressionLevel);
    }

    private void assertDefaultCompression(String keyspace, String table)
    {
       assertLZ4Compression(keyspace, table);
    }

    private void assertLZ4Compression(String keyspace, String table)
    {
        KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(keyspace);
        Assert.assertNotNull("Keyspace should exist", ksm);

        TableMetadata tm = ksm.getTableOrViewNullable(table);
        Assert.assertNotNull("Table should exist", tm);

        CompressionParams compression = tm.params.compression;
        Assert.assertTrue("Table should have compression enabled", compression.isEnabled());
        Assert.assertEquals("Should use LZ4Compressor", LZ4Compressor.class, compression.klass());
    }

    private void assertZstdCompressionWithChunkLength(String keyspace, String table, int expectedLevel, String expectedChunkLength)
    {
        KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(keyspace);
        Assert.assertNotNull("Keyspace should exist", ksm);

        TableMetadata tm = ksm.getTableOrViewNullable(table);
        Assert.assertNotNull("Table should exist", tm);

        CompressionParams compression = tm.params.compression;
        Assert.assertTrue("Table should have compression enabled", compression.isEnabled());
        Assert.assertEquals("Should use ZstdCompressor", ZstdCompressor.class, compression.klass());

        // Check compression level and chunk_length_in_kb in the options
        Map<String, String> options = compression.asMap();
        String compressionLevel = options.get(ZstdCompressor.COMPRESSION_LEVEL_OPTION_NAME);
        Assert.assertEquals("Compression level should be " + expectedLevel,
                           String.valueOf(expectedLevel), compressionLevel);

        String chunkLength = options.get("chunk_length_in_kb");
        Assert.assertEquals("chunk_length_in_kb should be " + expectedChunkLength,
                           expectedChunkLength, chunkLength);
    }

    private void assertZstdCompressionWithChunkLengthAndMinRatio(String keyspace, String table, int expectedLevel, String expectedChunkLength, String expectedMinRatio)
    {
        KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(keyspace);
        Assert.assertNotNull("Keyspace should exist", ksm);

        TableMetadata tm = ksm.getTableOrViewNullable(table);
        Assert.assertNotNull("Table should exist", tm);

        CompressionParams compression = tm.params.compression;
        Assert.assertTrue("Table should have compression enabled", compression.isEnabled());
        Assert.assertEquals("Should use ZstdCompressor", ZstdCompressor.class, compression.klass());

        // Check compression level, chunk_length_in_kb, and min_compress_ratio in the options
        Map<String, String> options = compression.asMap();
        String compressionLevel = options.get(ZstdCompressor.COMPRESSION_LEVEL_OPTION_NAME);
        Assert.assertEquals("Compression level should be " + expectedLevel,
                           String.valueOf(expectedLevel), compressionLevel);

        String chunkLength = options.get("chunk_length_in_kb");
        Assert.assertEquals("chunk_length_in_kb should be " + expectedChunkLength,
                           expectedChunkLength, chunkLength);

        String minRatio = options.get("min_compress_ratio");
        Assert.assertEquals("min_compress_ratio should be " + expectedMinRatio,
                           expectedMinRatio, minRatio);
    }

    private void assertOptionNotPresent(String keyspace, String table, String optionName)
    {
        KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(keyspace);
        Assert.assertNotNull("Keyspace should exist", ksm);

        TableMetadata tm = ksm.getTableOrViewNullable(table);
        Assert.assertNotNull("Table should exist", tm);

        CompressionParams compression = tm.params.compression;
        Map<String, String> options = compression.asMap();

        Assert.assertFalse("Option " + optionName + " should not be present",
                          options.containsKey(optionName));
    }
}
