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

import java.util.HashSet;
import java.util.Set;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Compressor used by flushed SSTables for each combination of the {@code flush_compression} table option
 * and the {@code flush_compression} yaml setting. Compaction always uses the table compressor.
 */
public class FlushCompressionTableOptionFlushTest extends CQLTester
{
    private static Config.FlushCompression defaultFlush;

    @BeforeClass
    public static void setUpFlushCompression()
    {
        defaultFlush = DatabaseDescriptor.getFlushCompression();
    }

    @After
    public void resetFlushCompression()
    {
        DatabaseDescriptor.setFlushCompression(defaultFlush);
    }

    @Test
    public void noneOverridesYaml() throws Throwable
    {
        for (Config.FlushCompression yaml : Config.FlushCompression.values())
        {
            DatabaseDescriptor.setFlushCompression(yaml);
            createTable("CREATE TABLE %s (k text PRIMARY KEY, v text) WITH compression = {'class': 'LZ4Compressor'} AND flush_compression = 'none'");
            ColumnFamilyStore cfs = flushTwice();
            assertFlushedWith(cfs, NoopCompressor.class);
            assertCompactedWith(cfs, LZ4Compressor.class);
        }
    }

    @Test
    public void tableOverridesYaml() throws Throwable
    {
        for (Config.FlushCompression yaml : Config.FlushCompression.values())
        {
            DatabaseDescriptor.setFlushCompression(yaml);
            createTable("CREATE TABLE %s (k text PRIMARY KEY, v text) WITH compression = {'class': 'ZstdCompressor'} AND flush_compression = 'table'");
            ColumnFamilyStore cfs = flushTwice();
            assertFlushedWith(cfs, ZstdCompressor.class);
            assertCompactedWith(cfs, ZstdCompressor.class);
        }
    }

    @Test
    public void fastOverridesYamlForSlowCompressor() throws Throwable
    {
        for (Config.FlushCompression yaml : Config.FlushCompression.values())
        {
            DatabaseDescriptor.setFlushCompression(yaml);
            createTable("CREATE TABLE %s (k text PRIMARY KEY, v text) WITH compression = {'class': 'ZstdCompressor'} AND flush_compression = 'fast'");
            ColumnFamilyStore cfs = flushTwice();
            assertFlushedWith(cfs, LZ4Compressor.class);
            assertCompactedWith(cfs, ZstdCompressor.class);
        }
    }

    @Test
    public void fastReplacesDeflateWithLz4() throws Throwable
    {
        DatabaseDescriptor.setFlushCompression(Config.FlushCompression.table);
        createTable("CREATE TABLE %s (k text PRIMARY KEY, v text) WITH compression = {'class': 'DeflateCompressor'} AND flush_compression = 'fast'");
        ColumnFamilyStore cfs = flushTwice();
        assertFlushedWith(cfs, LZ4Compressor.class);
        assertCompactedWith(cfs, DeflateCompressor.class);
    }

    @Test
    public void fastKeepsFastTableCompressor() throws Throwable
    {
        for (Config.FlushCompression yaml : Config.FlushCompression.values())
        {
            DatabaseDescriptor.setFlushCompression(yaml);
            createTable("CREATE TABLE %s (k text PRIMARY KEY, v text) WITH compression = {'class': 'LZ4Compressor', 'chunk_length_in_kb': 64} AND flush_compression = 'fast'");
            ColumnFamilyStore cfs = flushTwice();
            assertFlushedWith(cfs, LZ4Compressor.class);
            for (SSTableReader sstable : cfs.getLiveSSTables())
                assertThat(sstable.getCompressionMetadata().parameters.chunkLength()).isEqualTo(64 * 1024);
        }
    }

    @Test
    public void autoFollowsYamlNone() throws Throwable
    {
        DatabaseDescriptor.setFlushCompression(Config.FlushCompression.none);
        createTable("CREATE TABLE %s (k text PRIMARY KEY, v text) WITH compression = {'class': 'LZ4Compressor'} AND flush_compression = 'auto'");
        ColumnFamilyStore cfs = flushTwice();
        assertFlushedWith(cfs, NoopCompressor.class);
        assertCompactedWith(cfs, LZ4Compressor.class);
    }

    @Test
    public void autoFollowsYamlFast() throws Throwable
    {
        DatabaseDescriptor.setFlushCompression(Config.FlushCompression.fast);
        createTable("CREATE TABLE %s (k text PRIMARY KEY, v text) WITH compression = {'class': 'ZstdCompressor'} AND flush_compression = 'auto'");
        ColumnFamilyStore cfs = flushTwice();
        assertFlushedWith(cfs, LZ4Compressor.class);
        assertCompactedWith(cfs, ZstdCompressor.class);
    }

    @Test
    public void autoFollowsYamlTable() throws Throwable
    {
        DatabaseDescriptor.setFlushCompression(Config.FlushCompression.table);
        createTable("CREATE TABLE %s (k text PRIMARY KEY, v text) WITH compression = {'class': 'ZstdCompressor'} AND flush_compression = 'auto'");
        ColumnFamilyStore cfs = flushTwice();
        assertFlushedWith(cfs, ZstdCompressor.class);
        assertCompactedWith(cfs, ZstdCompressor.class);
    }

    @Test
    public void omittedOptionIsAuto() throws Throwable
    {
        DatabaseDescriptor.setFlushCompression(Config.FlushCompression.none);
        createTable("CREATE TABLE %s (k text PRIMARY KEY, v text) WITH compression = {'class': 'ZstdCompressor'}");
        ColumnFamilyStore cfs = flushTwice();
        assertFlushedWith(cfs, NoopCompressor.class);
        assertCompactedWith(cfs, ZstdCompressor.class);
    }

    @Test
    public void alterTakesEffectOnNextFlush() throws Throwable
    {
        DatabaseDescriptor.setFlushCompression(Config.FlushCompression.fast);
        createTable("CREATE TABLE %s (k text PRIMARY KEY, v text) WITH compression = {'class': 'ZstdCompressor'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        execute("INSERT INTO %s (k, v) values (?, ?)", "k1", "v1");
        flush();
        assertThat(cfs.getLiveSSTables()).hasSize(1);
        assertFlushedWith(cfs, LZ4Compressor.class);

        alterTable("ALTER TABLE %s WITH flush_compression = 'table'");
        execute("INSERT INTO %s (k, v) values (?, ?)", "k2", "v2");
        flush();
        assertThat(cfs.getLiveSSTables()).hasSize(2);
        assertThat(compressorsOf(cfs)).containsExactlyInAnyOrder(LZ4Compressor.class, ZstdCompressor.class);

        alterTable("ALTER TABLE %s WITH flush_compression = 'none'");
        execute("INSERT INTO %s (k, v) values (?, ?)", "k3", "v3");
        flush();
        assertThat(cfs.getLiveSSTables()).hasSize(3);
        assertThat(compressorsOf(cfs)).containsExactlyInAnyOrder(LZ4Compressor.class, ZstdCompressor.class, NoopCompressor.class);

        assertCompactedWith(cfs, ZstdCompressor.class);
    }

    @Test
    public void uncompressedTableUnaffected() throws Throwable
    {
        DatabaseDescriptor.setFlushCompression(Config.FlushCompression.fast);
        for (String option : new String[]{ "auto", "none", "fast", "table" })
        {
            createTable("CREATE TABLE %s (k text PRIMARY KEY, v text) WITH compression = {'enabled': false} AND flush_compression = '" + option + '\'');
            ColumnFamilyStore cfs = flushTwice();
            for (SSTableReader sstable : cfs.getLiveSSTables())
                assertThat(sstable.compression).describedAs("option %s", option).isFalse();
        }
    }

    private ColumnFamilyStore flushTwice() throws Throwable
    {
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        execute("INSERT INTO %s (k, v) values (?, ?)", "k1", "v1");
        flush();
        assertThat(cfs.getLiveSSTables()).hasSize(1);

        execute("INSERT INTO %s (k, v) values (?, ?)", "k2", "v2");
        flush();
        assertThat(cfs.getLiveSSTables()).hasSize(2);

        return cfs;
    }

    private static void assertFlushedWith(ColumnFamilyStore cfs, Class<? extends ICompressor> expected)
    {
        Set<SSTableReader> sstables = cfs.getLiveSSTables();
        assertThat(sstables).isNotEmpty();
        for (SSTableReader sstable : sstables)
            assertThat(sstable.getCompressionMetadata().parameters.getSstableCompressor())
            .describedAs("compressor of flushed sstable %s (yaml flush_compression=%s)", sstable, DatabaseDescriptor.getFlushCompression())
            .isInstanceOf(expected);
    }

    private void assertCompactedWith(ColumnFamilyStore cfs, Class<? extends ICompressor> expected)
    {
        forceCompactAll();
        Set<SSTableReader> sstables = cfs.getLiveSSTables();
        assertThat(sstables).hasSize(1);
        assertThat(sstables.iterator().next().getCompressionMetadata().parameters.getSstableCompressor()).isInstanceOf(expected);
    }

    private static Set<Class<? extends ICompressor>> compressorsOf(ColumnFamilyStore cfs)
    {
        Set<Class<? extends ICompressor>> classes = new HashSet<>();
        for (SSTableReader sstable : cfs.getLiveSSTables())
            classes.add(sstable.getCompressionMetadata().parameters.getSstableCompressor().getClass());
        return classes;
    }
}
