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
package org.apache.cassandra.io.compress;

import java.lang.reflect.Field;
import java.util.Set;

import org.mockito.Mockito;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.schema.DefaultCompressionSelector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class CompressionSelectorTest extends CQLTester
{
    @BeforeClass
    public static void setupClass()
    {
        CQLTester.setUpClass();
    }

    @Before
    public void prepare() throws Exception
    {
        DatabaseDescriptor.setFlushCompression(null);
        CassandraRelevantProperties.SSTABLE_COMPRESSION_SELECTOR_CLASS.reset();
    }

    // --- CompressionParams.Selector loading via system property ---

    @Test
    public void testFromPropertyLoadsDefaultSelectorWhenPropertyEmpty()
    {
        CompressionParams.Selector selector = CompressionParams.Selector.fromProperty();
        assertSame(DefaultCompressionSelector.class, selector.getClass());
    }

    @Test
    public void testFromPropertyLoadsCustomSelectorClass()
    {
        System.setProperty(CassandraRelevantProperties.SSTABLE_COMPRESSION_SELECTOR_CLASS.getKey(),
                           TestCustomSelector.class.getName());
        CompressionParams.Selector selector = CompressionParams.Selector.fromProperty();
        assertSame(TestCustomSelector.class, selector.getClass());
    }

    // --- Most common default configuration ---

    @Test
    public void testDefaultCompressionWhenNoPropertiesSet()
    {
        assertSame(CompressionParams.FAST, CompressionParams.forNewTables("any_keyspace"));
        assertSame(CompressionParams.FAST, CompressionParams.deflate().forFlush("any_keyspace"));
    }

    // --- Additional flushCompression tests ---

    @Test
    public void testFlushCompressionReturnsNoneIfNewTableCompressionNotSet()
    {
        assertEquals(CompressionParams.noCompression(),
                     CompressionParams.noCompression().forFlush("any_keyspace"));
    }

    @Test
    public void testFlushCompressionReturnsNoopIfGlobalFlushCompressionSetToNone()
    {
        DatabaseDescriptor.setFlushCompression(Config.FlushCompression.none);
        DefaultCompressionSelector selector = new DefaultCompressionSelector();
        // regardless of what the table uses, none → NOOP
        assertSame(CompressionParams.NOOP, selector.flushCompression("ks", CompressionParams.FAST));
        assertSame(CompressionParams.NOOP, selector.flushCompression("ks", CompressionParams.ADAPTIVE));
        assertSame(CompressionParams.NOOP, selector.flushCompression("ks", CompressionParams.FAST_ADAPTIVE));
    }

    @Test
    public void testFlushCompressionReturnsFastWhenTableCompressorDoesntSupportFastCompression()
    {
        // ADAPTIVE (general AdaptiveCompressor) does not advertise FAST_COMPRESSION use
        DatabaseDescriptor.setFlushCompression(Config.FlushCompression.fast);
        DefaultCompressionSelector selector = new DefaultCompressionSelector();
        assertSame(CompressionParams.FAST, selector.flushCompression("ks", CompressionParams.ADAPTIVE));
    }

    @Test
    public void testFlushCompressionFallsThroughToTableWhenTableCompressorSupportsFastCompression()
    {
        // LZ4 fast compressor advertises FAST_COMPRESSION; forUse returns itself → result is tableParams
        DatabaseDescriptor.setFlushCompression(Config.FlushCompression.fast);
        DefaultCompressionSelector selector = new DefaultCompressionSelector();
        assertSame(CompressionParams.FAST, selector.flushCompression("ks", CompressionParams.FAST));

        DatabaseDescriptor.setFlushCompression(Config.FlushCompression.adaptive);
        // FAST_ADAPTIVE also advertises FAST_COMPRESSION
        assertSame(CompressionParams.FAST_ADAPTIVE, selector.flushCompression("ks", CompressionParams.FAST_ADAPTIVE));
    }

    @Test
    public void testFlushCompressionAdaptiveReturnsFastAdaptive()
    {
        DatabaseDescriptor.setFlushCompression(Config.FlushCompression.adaptive);
        DefaultCompressionSelector selector = new DefaultCompressionSelector();
        assertSame(CompressionParams.FAST_ADAPTIVE, selector.flushCompression("ks", CompressionParams.ADAPTIVE));
    }

    @Test
    public void testFlushCompressionFallsBackToNewTableCompression()
    {
        // Slow compressor + flush compression set to table -> use the table compressor as-is
        DatabaseDescriptor.setFlushCompression(Config.FlushCompression.table);
        DefaultCompressionSelector selector = new DefaultCompressionSelector();
        assertEquals(CompressionParams.deflate(), selector.flushCompression("ks", CompressionParams.deflate()));
    }

    @Test
    public void testFlushCompressionUsesFasterVariantOfGeneralCompressionIfAvailable()
    {
        // If slow compressor is selected, check we attempt to adapt it for fast operation by callling
        // forUse(FAST_COMPRESSION).
        DatabaseDescriptor.setFlushCompression(Config.FlushCompression.table);
        DefaultCompressionSelector selector = new DefaultCompressionSelector();
        assertEquals(CompressionParams.ADAPTIVE.forUse(ICompressor.Uses.FAST_COMPRESSION),
                     selector.flushCompression("ks", CompressionParams.ADAPTIVE));
    }

    // --- compactionCompression tests ---

    @Test
    public void testCompactionCompressionDefaultReturnsTableParams()
    {
        DefaultCompressionSelector selector = new DefaultCompressionSelector();
        assertSame(CompressionParams.FAST, selector.compactionCompression("ks", CompressionParams.FAST));
        assertSame(CompressionParams.ADAPTIVE, selector.compactionCompression("ks", CompressionParams.ADAPTIVE));
        assertSame(CompressionParams.NOOP, selector.compactionCompression("ks", CompressionParams.NOOP));
    }

    @Test
    public void testForCompactionDelegatesToSelector()
    {
        assertSame(CompressionParams.FAST, CompressionParams.FAST.forCompaction("any_keyspace"));
        assertSame(CompressionParams.ADAPTIVE, CompressionParams.ADAPTIVE.forCompaction("any_keyspace"));
    }

    // --- End-to-end selector integration tests (flush and compaction through real I/O) ---

    @Test
    public void testCompressionSelectorAppliedToNewTable() throws Throwable
    {
        CompressionParams.Selector selector = Mockito.mock(CompressionParams.Selector.class);

        Field selectorField = CompressionParams.class.getDeclaredField("SELECTOR");
        selectorField.setAccessible(true);
        CompressionParams.Selector original = (CompressionParams.Selector) selectorField.get(null);
        try
        {
            selectorField.set(null, selector);
            Mockito.when(selector.newTableCompression(Mockito.any())).thenReturn(CompressionParams.NOOP);
            createTable("CREATE TABLE %s (k text PRIMARY KEY, v text)");
            assertSame(CompressionParams.NOOP,
                       getCurrentColumnFamilyStore().metadata().params.compression);

            var compression = CompressionParams.snappy(8 * 1024);
            Mockito.when(selector.newTableCompression(Mockito.any())).thenReturn(compression);
            createTable("CREATE TABLE %s (k text PRIMARY KEY, v text)");
            assertEquals(compression, getCurrentColumnFamilyStore().metadata().params.compression);
        }
        finally
        {
            selectorField.set(null, original);
        }
    }

    @Test
    public void testCompressionSelectorAppliedToFlush() throws Throwable
    {
        CompressionParams.Selector selector = Mockito.mock(CompressionParams.Selector.class);

        Field selectorField = CompressionParams.class.getDeclaredField("SELECTOR");
        selectorField.setAccessible(true);
        CompressionParams.Selector original = (CompressionParams.Selector) selectorField.get(null);
        try
        {
            selectorField.set(null, selector);

            CompressionParams flushParams = CompressionParams.NOOP;
            Mockito.when(selector.newTableCompression(Mockito.any())).thenReturn(CompressionParams.lz4());
            Mockito.when(selector.flushCompression(Mockito.any(), Mockito.any())).thenReturn(flushParams);

            createTable("CREATE TABLE %s (k text PRIMARY KEY, v text)");
            ColumnFamilyStore store = flushTwice();

            // Every flushed SSTable should use the compressor returned by flushCompression()
            Set<SSTableReader> sstables = store.getLiveSSTables();
            assertEquals(2, sstables.size());
            sstables.forEach(sstable ->
                assertSame(flushParams, sstable.getCompressionMetadata().parameters)
            );
        }
        finally
        {
            selectorField.set(null, original);
        }
    }

    @Test
    public void testCompressionSelectorAppliedToCompaction() throws Throwable
    {
        CompressionParams.Selector selector = Mockito.mock(CompressionParams.Selector.class);

        Field selectorField = CompressionParams.class.getDeclaredField("SELECTOR");
        selectorField.setAccessible(true);
        CompressionParams.Selector original = (CompressionParams.Selector) selectorField.get(null);
        try
        {
            selectorField.set(null, selector);

            CompressionParams compactionParams = CompressionParams.snappy(8 * 1024);
            Mockito.when(selector.newTableCompression(Mockito.any())).thenReturn(CompressionParams.lz4());
            Mockito.when(selector.flushCompression(Mockito.any(), Mockito.any())).thenReturn(CompressionParams.lz4());
            Mockito.when(selector.compactionCompression(Mockito.any(), Mockito.any())).thenReturn(compactionParams);

            createTable("CREATE TABLE %s (k text PRIMARY KEY, v text)");
            flushTwice();

            // After compaction the single output SSTable should use compactionCompression()
            compact();

            Set<SSTableReader> sstables = getCurrentColumnFamilyStore().getLiveSSTables();
            assertEquals(1, sstables.size());
            sstables.forEach(sstable ->
                assertEquals(compactionParams, sstable.getCompressionMetadata().parameters)
            );
        }
        finally
        {
            selectorField.set(null, original);
        }
    }

    private ColumnFamilyStore flushTwice()
    {
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        execute("INSERT INTO %s (k, v) values (?, ?)", "k1", "v1");
        flush();
        assertEquals(1, cfs.getLiveSSTables().size());

        execute("INSERT INTO %s (k, v) values (?, ?)", "k2", "v2");
        flush();
        assertEquals(2, cfs.getLiveSSTables().size());

        return cfs;
    }

    /**
     * A test-only selector that always returns NOOP compression.
     * We need this to test if the selector can be picked up by class name;
     * in other cases mocking does the job.
     */
    public static class TestCustomSelector implements CompressionParams.Selector
    {
        @Override
        public CompressionParams newTableCompression(String keyspace)
        {
            return CompressionParams.NOOP;
        }

        @Override
        public CompressionParams flushCompression(String keyspace, CompressionParams tableParams)
        {
            return CompressionParams.NOOP;
        }

        @Override
        public CompressionParams compactionCompression(String keyspace, CompressionParams tableParams)
        {
            return CompressionParams.NOOP;
        }
    }
}
