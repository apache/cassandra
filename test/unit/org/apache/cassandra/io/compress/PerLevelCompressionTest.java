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

import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.LeveledCompactionStrategy;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.CompressionParams;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for the per-table {@code per_level_compression} compaction option on
 * {@code LeveledCompactionStrategy}. See {@link LeveledCompactionStrategy#PER_LEVEL_COMPRESSION_OPTION}.
 */
public class PerLevelCompressionTest extends CQLTester
{
    private static Config.FlushCompression defaultFlush;

    @BeforeClass
    public static void setUpClass()
    {
        CQLTester.setUpClass();
        defaultFlush = DatabaseDescriptor.getFlushCompression();
    }

    @Before
    public void setUpCompressionState()
    {
        // Flush as the table codec so flushed sstables are distinguishable from compaction output.
        DatabaseDescriptor.setFlushCompression(Config.FlushCompression.table);
    }

    @After
    public void tearDownCompressionState()
    {
        DatabaseDescriptor.setFlushCompression(defaultFlush);
    }

    // ---- Pure parsing / building unit tests ----

    @Test
    public void parseAndResolveTest()
    {
        assertTrue(CompactionParams.parsePerLevelCompression(null).isEmpty());
        assertTrue(CompactionParams.parsePerLevelCompression("   ").isEmpty());

        Map<Integer, CompressionParams> parsed = CompactionParams.parsePerLevelCompression(
            "{\"0\":{\"class\":\"LZ4Compressor\"},\"2\":{\"class\":\"ZstdCompressor\",\"compression_level\":\"9\"}}");
        assertEquals(2, parsed.size());
        assertTrue(parsed.get(0).getSstableCompressor() instanceof LZ4Compressor);
        assertTrue(parsed.get(2).getSstableCompressor() instanceof ZstdCompressor);
        assertEquals("9", parsed.get(2).getOtherOptions().get("compression_level"));

        // Each level is built independently, like the 'compression' option: unspecified chunk length uses the
        // compression default; an explicit chunk_length_in_kb is honored (nothing is inherited from the table).
        assertEquals(CompressionParams.DEFAULT_CHUNK_LENGTH, parsed.get(0).chunkLength());
        assertEquals(1024 * 64, CompactionParams.parsePerLevelCompression(
            "{\"1\":{\"class\":\"LZ4Compressor\",\"chunk_length_in_kb\":\"64\"}}").get(1).chunkLength());

        // Resolution goes through the cached map on the immutable CompactionParams instance (no re-parsing).
        assertTrue(lcsParams("{\"0\":{\"class\":\"LZ4Compressor\"}}").perLevelCompression(0)
                       .getSstableCompressor() instanceof LZ4Compressor);
        assertNull(lcsParams("{\"0\":{\"class\":\"LZ4Compressor\"}}").perLevelCompression(1));

        // A malformed spec must degrade gracefully to the table default (null) rather than throw.
        assertNull(lcsParams("bogus").perLevelCompression(0));
        assertNull(lcsParams("{\"0\":{}}").perLevelCompression(0));   // missing class -> ignored
    }

    private static CompactionParams lcsParams(String perLevelSpec)
    {
        return CompactionParams.create(LeveledCompactionStrategy.class,
                                       ImmutableMap.of(LeveledCompactionStrategy.PER_LEVEL_COMPRESSION_OPTION,
                                                       perLevelSpec));
    }

    @Test
    public void invalidSpecsRejectedTest()
    {
        assertBadSpec("not json");
        assertBadSpec("[]");                                          // not a JSON object
        assertBadSpec("{\"0\":\"LZ4Compressor\"}");                  // level value is not an object
        assertBadSpec("{\"x\":{\"class\":\"LZ4Compressor\"}}");      // non-integer level
        assertBadSpec("{\"-1\":{\"class\":\"LZ4Compressor\"}}");     // negative level
        assertBadSpec("{\"0\":{}}");                                 // missing class
        assertBadSpec("{\"0\":{\"class\":\"NotARealCompressor\"}}"); // class does not resolve
        assertBadSpec("{\"0\":{\"class\":\"LZ4Compressor\",\"bogus_option\":\"1\"}}"); // unsupported option
    }

    private static void assertBadSpec(String spec)
    {
        try
        {
            CompactionParams.parsePerLevelCompression(spec);
            fail("Expected ConfigurationException for spec: " + spec);
        }
        catch (ConfigurationException expected)
        {
            // expected
        }
    }

    // ---- End-to-end tests through a real compaction ----

    // NOTE: a maximal compaction (compact()) on LCS uses MajorLeveledCompactionWriter, which writes output
    // starting at level 1. For a tiny dataset that yields a single L1 sstable, so the tests below target
    // level 1 as the deterministic compaction-output level.

    @Test
    public void overriddenLevelUsesFastCodecTest() throws Throwable
    {
        createTable("CREATE TABLE %s (k text PRIMARY KEY, v text) WITH " +
                    "compaction = {'class':'LeveledCompactionStrategy'," +
                    "'per_level_compression':'{\"1\":{\"class\":\"LZ4Compressor\"}}'} " +
                    "AND compression = {'class':'ZstdCompressor'}");

        ColumnFamilyStore store = flushTwiceAsZstd();
        compact();

        assertHasLevel(store, 1);
        assertLevelCompressionInvariant(store, 1);
    }

    @Test
    public void unlistedLevelFallsBackToTableCodecTest() throws Throwable
    {
        // Only level 0 is overridden; the compaction output (L1) must fall back to the table's Zstd.
        createTable("CREATE TABLE %s (k text PRIMARY KEY, v text) WITH " +
                    "compaction = {'class':'LeveledCompactionStrategy'," +
                    "'per_level_compression':'{\"0\":{\"class\":\"LZ4Compressor\"}}'} " +
                    "AND compression = {'class':'ZstdCompressor'}");

        ColumnFamilyStore store = flushTwiceAsZstd();
        compact();

        assertHasLevel(store, 1);
        assertLevelCompressionInvariant(store, 0);
    }

    @Test
    public void perLevelCompressorOptionsAppliedTest() throws Throwable
    {
        // L1 override uses Zstd with a non-default compression_level; the table default Zstd carries no level.
        createTable("CREATE TABLE %s (k text PRIMARY KEY, v text) WITH " +
                    "compaction = {'class':'LeveledCompactionStrategy'," +
                    "'per_level_compression':'{\"1\":{\"class\":\"ZstdCompressor\",\"compression_level\":\"9\"}}'} " +
                    "AND compression = {'class':'ZstdCompressor'}");

        ColumnFamilyStore store = flushTwiceAsZstd();
        compact();

        assertHasLevel(store, 1);
        store.getLiveSSTables().stream()
             .filter(s -> s.getSSTableLevel() == 1)
             .forEach(s -> {
                 CompressionParams params = s.getCompressionMetadata().parameters;
                 assertTrue("Level 1 should be Zstd", params.getSstableCompressor() instanceof ZstdCompressor);
                 assertEquals("per-level compression_level should be applied and persisted",
                              "9", params.getOtherOptions().get("compression_level"));
             });
    }

    @Test
    public void perLevelChunkLengthOverrideAppliedTest() throws Throwable
    {
        // Table uses the default 16 KB chunk length; L1 overrides chunk_length_in_kb to 64.
        createTable("CREATE TABLE %s (k text PRIMARY KEY, v text) WITH " +
                    "compaction = {'class':'LeveledCompactionStrategy'," +
                    "'per_level_compression':'{\"1\":{\"class\":\"LZ4Compressor\",\"chunk_length_in_kb\":\"64\"}}'} " +
                    "AND compression = {'class':'ZstdCompressor'}");

        ColumnFamilyStore store = flushTwiceAsZstd();
        compact();

        assertHasLevel(store, 1);
        store.getLiveSSTables().stream()
             .filter(s -> s.getSSTableLevel() == 1)
             .forEach(s -> assertEquals("L1 chunk length should be the per-level override (64 KB)",
                                        1024 * 64, s.getCompressionMetadata().chunkLength()));
    }

    @Test
    public void levelChunkLengthIsIndependentOfTableTest() throws Throwable
    {
        // Table uses a non-default 64 KB chunk length; L1 overrides only the class, so it must fall back to
        // the compression default chunk length (16 KB), NOT inherit the table's 64 KB.
        createTable("CREATE TABLE %s (k text PRIMARY KEY, v text) WITH " +
                    "compaction = {'class':'LeveledCompactionStrategy'," +
                    "'per_level_compression':'{\"1\":{\"class\":\"LZ4Compressor\"}}'} " +
                    "AND compression = {'class':'ZstdCompressor','chunk_length_in_kb':'64'}");

        ColumnFamilyStore store = flushTwiceAsZstd();
        compact();

        assertHasLevel(store, 1);
        store.getLiveSSTables().stream()
             .filter(s -> s.getSSTableLevel() == 1)
             .forEach(s -> assertEquals("L1 chunk length should be the compression default, not the table's",
                                        CompressionParams.DEFAULT_CHUNK_LENGTH,
                                        s.getCompressionMetadata().chunkLength()));
    }

    @Test
    public void badOptionRejectedAtDdlTest() throws Throwable
    {
        try
        {
            createTable("CREATE TABLE %s (k text PRIMARY KEY, v text) WITH " +
                        "compaction = {'class':'LeveledCompactionStrategy','per_level_compression':'bogus'} " +
                        "AND compression = {'class':'ZstdCompressor'}");
            fail("Expected ConfigurationException for a malformed per_level_compression option");
        }
        catch (RuntimeException e)
        {
            assertTrue("Unexpected exception: " + e, hasConfigurationException(e));
        }
    }

    @Test
    public void badOptionRejectedAtAlterTest() throws Throwable
    {
        createTable("CREATE TABLE %s (k text PRIMARY KEY, v text) WITH " +
                    "compaction = {'class':'LeveledCompactionStrategy'} " +
                    "AND compression = {'class':'ZstdCompressor'}");

        try
        {
            alterTableMayThrow("ALTER TABLE %s WITH compaction = " +
                               "{'class':'LeveledCompactionStrategy','per_level_compression':'bogus'}");
            fail("Expected ConfigurationException for a malformed per_level_compression option on ALTER");
        }
        catch (Throwable t)
        {
            assertTrue("Unexpected exception: " + t, hasConfigurationException(t));
        }
    }

    /**
     * Returns true if {@code t} or any exception in its cause chain is a {@link ConfigurationException}.
     * The validation failure is wrapped at different depths on CREATE vs ALTER, so walk the whole chain.
     */
    private static boolean hasConfigurationException(Throwable t)
    {
        for (Throwable cause = t; cause != null; cause = cause.getCause())
        {
            if (cause instanceof ConfigurationException)
                return true;
        }
        return false;
    }

    @Test
    public void validOptionAcceptedAtAlterTest() throws Throwable
    {
        createTable("CREATE TABLE %s (k text PRIMARY KEY, v text) WITH " +
                    "compaction = {'class':'LeveledCompactionStrategy'} " +
                    "AND compression = {'class':'ZstdCompressor'}");

        String spec = "{\"1\":{\"class\":\"LZ4Compressor\"}}";
        alterTable("ALTER TABLE %s WITH compaction = " +
                   "{'class':'LeveledCompactionStrategy','per_level_compression':'" + spec + "'}");

        Map<String, String> compaction = getCurrentColumnFamilyStore().metadata().params.compaction.options();
        assertEquals(spec, compaction.get(LeveledCompactionStrategy.PER_LEVEL_COMPRESSION_OPTION));
    }

    private ColumnFamilyStore flushTwiceAsZstd() throws Throwable
    {
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        // Keep the two flushed L0 sstables around for the explicit compact() below; otherwise background
        // LCS compaction can merge them first and make the sstable count non-deterministic.
        cfs.disableAutoCompaction();

        execute("INSERT INTO %s (k, v) values (?, ?)", "k1", "v1");
        flush();
        execute("INSERT INTO %s (k, v) values (?, ?)", "k2", "v2");
        flush();
        assertEquals(2, cfs.getLiveSSTables().size());

        // Flushed sstables use the table codec (flush_compression=table) so they are Zstd.
        cfs.getLiveSSTables().forEach(sstable ->
            assertTrue(sstable.getCompressionMetadata().parameters.getSstableCompressor() instanceof ZstdCompressor));
        return cfs;
    }

    private static void assertHasLevel(ColumnFamilyStore store, int level)
    {
        boolean found = store.getLiveSSTables().stream().anyMatch(s -> s.getSSTableLevel() == level);
        assertTrue("Expected at least one sstable at level " + level, found);
    }

    /**
     * Asserts every live sstable's compressor matches the override: sstables at {@code fastLevel} use LZ4,
     * every other level uses the table's Zstd.
     */
    private static void assertLevelCompressionInvariant(ColumnFamilyStore store, int fastLevel)
    {
        Set<SSTableReader> sstables = store.getLiveSSTables();
        assertFalse("Expected at least one sstable after compaction", sstables.isEmpty());
        sstables.forEach(sstable -> {
            Object compressor = sstable.getCompressionMetadata().parameters.getSstableCompressor();
            if (sstable.getSSTableLevel() == fastLevel)
                assertTrue("Level " + fastLevel + " should be LZ4 but was " + compressor.getClass().getSimpleName(),
                           compressor instanceof LZ4Compressor);
            else
                assertTrue("Level " + sstable.getSSTableLevel() + " should be Zstd but was "
                           + compressor.getClass().getSimpleName(),
                           compressor instanceof ZstdCompressor);
        });
    }
}
