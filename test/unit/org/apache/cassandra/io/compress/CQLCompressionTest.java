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

import java.util.Set;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CQLCompressionTest extends CQLTester
{
    private static Config.FlushCompression defaultFlush;

    @BeforeClass
    public static void setUpClass()
    {
        CQLTester.setUpClass();

        defaultFlush = DatabaseDescriptor.getFlushCompression();
    }

    @Before
    public void resetDefaultFlush()
    {
        DatabaseDescriptor.setFlushCompression(defaultFlush);
    }

    @Test
    public void lz4ParamsTest()
    {
        createTable("create table %s (id int primary key, uh text) with compression = {'class':'LZ4Compressor', 'lz4_high_compressor_level':3}");
        assertEquals(((LZ4Compressor) getCurrentColumnFamilyStore().metadata().params.compression.getSstableCompressor()).compressorType, LZ4Compressor.LZ4_FAST_COMPRESSOR);
        createTable("create table %s (id int primary key, uh text) with compression = {'class':'LZ4Compressor', 'lz4_compressor_type':'high', 'lz4_high_compressor_level':13}");
        assertEquals(((LZ4Compressor)getCurrentColumnFamilyStore().metadata().params.compression.getSstableCompressor()).compressorType, LZ4Compressor.LZ4_HIGH_COMPRESSOR);
        assertEquals(((LZ4Compressor)getCurrentColumnFamilyStore().metadata().params.compression.getSstableCompressor()).compressionLevel, (Integer)13);
        createTable("create table %s (id int primary key, uh text) with compression = {'class':'LZ4Compressor'}");
        assertEquals(((LZ4Compressor)getCurrentColumnFamilyStore().metadata().params.compression.getSstableCompressor()).compressorType, LZ4Compressor.LZ4_FAST_COMPRESSOR);
        assertEquals(((LZ4Compressor)getCurrentColumnFamilyStore().metadata().params.compression.getSstableCompressor()).compressionLevel, (Integer)9);
    }

    @Test(expected = ConfigurationException.class)
    public void lz4BadParamsTest() throws Throwable
    {
        try
        {
            createTable("create table %s (id int primary key, uh text) with compression = {'class':'LZ4Compressor', 'lz4_compressor_type':'high', 'lz4_high_compressor_level':113}");
        }
        catch (RuntimeException e)
        {
            throw e.getCause();
        }
    }

    @Test
    public void zstdParamsTest()
    {
        createTable("create table %s (id int primary key, uh text) with compression = {'class':'ZstdCompressor', 'compression_level':-22}");
        assertTrue(((ZstdCompressor)getCurrentColumnFamilyStore().metadata().params.compression.getSstableCompressor()).getClass().equals(ZstdCompressor.class));
        assertEquals(((ZstdCompressor)getCurrentColumnFamilyStore().metadata().params.compression.getSstableCompressor()).getCompressionLevel(), -22);

        createTable("create table %s (id int primary key, uh text) with compression = {'class':'ZstdCompressor'}");
        assertTrue(((ZstdCompressor)getCurrentColumnFamilyStore().metadata().params.compression.getSstableCompressor()).getClass().equals(ZstdCompressor.class));
        assertEquals(((ZstdCompressor)getCurrentColumnFamilyStore().metadata().params.compression.getSstableCompressor()).getCompressionLevel(), ZstdCompressor.DEFAULT_COMPRESSION_LEVEL);
    }

    @Test(expected = ConfigurationException.class)
    public void zstdBadParamsTest() throws Throwable
    {
        try
        {
            createTable("create table %s (id int primary key, uh text) with compression = {'class':'ZstdCompressor', 'compression_level':'100'}");
        }
        catch (RuntimeException e)
        {
            throw e.getCause();
        }
    }

    @Test
    public void lz4FlushTest() throws Throwable
    {
        createTable("CREATE TABLE %s (k text PRIMARY KEY, v text) WITH compression = {'class': 'LZ4Compressor'};");
        ColumnFamilyStore store = flushTwice();

        // Should flush as LZ4 "fast"
        Set<SSTableReader> sstables = store.getLiveSSTables();
        sstables.forEach(sstable -> {
            LZ4Compressor compressor = (LZ4Compressor) sstable.getCompressionMetadata().parameters.getSstableCompressor();
            assertEquals(LZ4Compressor.LZ4_FAST_COMPRESSOR, compressor.compressorType);
        });

        // Should compact to LZ4 "fast"
        forceCompactAll();

        sstables = store.getLiveSSTables();
        assertEquals(sstables.size(), 1);
        store.getLiveSSTables().forEach(sstable -> {
            LZ4Compressor compressor = (LZ4Compressor) sstable.getCompressionMetadata().parameters.getSstableCompressor();
            assertEquals(LZ4Compressor.LZ4_FAST_COMPRESSOR, compressor.compressorType);
        });
    }

    @Test
    public void lz4hcFlushTest() throws Throwable
    {
        createTable("CREATE TABLE %s (k text PRIMARY KEY, v text) WITH compression = " +
                    "{'class': 'LZ4Compressor', 'lz4_compressor_type': 'high'};");
        ColumnFamilyStore store = flushTwice();

        // Should flush as LZ4 "fast" mode
        Set<SSTableReader> sstables = store.getLiveSSTables();
        sstables.forEach(sstable -> {
            LZ4Compressor compressor = (LZ4Compressor) sstable.getCompressionMetadata().parameters.getSstableCompressor();
            assertEquals(LZ4Compressor.LZ4_FAST_COMPRESSOR, compressor.compressorType);
        });

        // Should compact to LZ4 "high" mode
        forceCompactAll();

        sstables = store.getLiveSSTables();
        assertEquals(sstables.size(), 1);
        store.getLiveSSTables().forEach(sstable -> {
            LZ4Compressor compressor = (LZ4Compressor) sstable.getCompressionMetadata().parameters.getSstableCompressor();
            assertEquals(LZ4Compressor.LZ4_HIGH_COMPRESSOR, compressor.compressorType);
        });
    }

    @Test
    public void zstdFlushTest() throws Throwable
    {
        createTable("CREATE TABLE %s (k text PRIMARY KEY, v text) WITH compression = {'class': 'ZstdCompressor'};");
        ColumnFamilyStore store = flushTwice();

        // Should flush as LZ4
        Set<SSTableReader> sstables = store.getLiveSSTables();
        sstables.forEach(sstable -> {
            assertTrue(sstable.getCompressionMetadata().parameters.getSstableCompressor() instanceof LZ4Compressor);
        });

        // Should compact to Zstd
        forceCompactAll();

        sstables = store.getLiveSSTables();
        assertEquals(sstables.size(), 1);
        store.getLiveSSTables().forEach(sstable -> {
            assertTrue(sstable.getCompressionMetadata().parameters.getSstableCompressor() instanceof ZstdCompressor);
        });
    }

    @Test
    public void deflateFlushTest() throws Throwable
    {
        createTable("CREATE TABLE %s (k text PRIMARY KEY, v text) WITH compression = {'class': 'DeflateCompressor'};");
        ColumnFamilyStore store = flushTwice();

        // Should flush as LZ4
        Set<SSTableReader> sstables = store.getLiveSSTables();
        sstables.forEach(sstable -> {
            assertTrue(sstable.getCompressionMetadata().parameters.getSstableCompressor() instanceof LZ4Compressor);
        });

        // Should compact to Deflate
        forceCompactAll();

        sstables = store.getLiveSSTables();
        assertEquals(sstables.size(), 1);
        store.getLiveSSTables().forEach(sstable -> {
            assertTrue(sstable.getCompressionMetadata().parameters.getSstableCompressor() instanceof DeflateCompressor);
        });
    }

    @Test
    public void useNoCompressorOnFlushTest() throws Throwable
    {
        DatabaseDescriptor.setFlushCompression(Config.FlushCompression.none);
        createTable("CREATE TABLE %s (k text PRIMARY KEY, v text) WITH compression = {'class': 'LZ4Compressor'};");
        ColumnFamilyStore store = flushTwice();

        // Should flush as Noop compressor
        Set<SSTableReader> sstables = store.getLiveSSTables();
        sstables.forEach(sstable -> {
            assertTrue(sstable.getCompressionMetadata().parameters.getSstableCompressor() instanceof NoopCompressor);
        });

        // Should compact to LZ4
        forceCompactAll();

        sstables = store.getLiveSSTables();
        assertEquals(sstables.size(), 1);
        store.getLiveSSTables().forEach(sstable -> {
            assertTrue(sstable.getCompressionMetadata().parameters.getSstableCompressor() instanceof LZ4Compressor);
        });
    }

    @Test
    public void useTableCompressorOnFlushTest() throws Throwable
    {
        DatabaseDescriptor.setFlushCompression(Config.FlushCompression.table);

        createTable("CREATE TABLE %s (k text PRIMARY KEY, v text) WITH compression = {'class': 'ZstdCompressor'};");
        ColumnFamilyStore store = flushTwice();

        // Should flush as Zstd
        Set<SSTableReader> sstables = store.getLiveSSTables();
        sstables.forEach(sstable -> {
            assertTrue(sstable.getCompressionMetadata().parameters.getSstableCompressor() instanceof ZstdCompressor);
        });
    }

    @Test
    public void zstdTableFlushTest() throws Throwable
    {
        createTable("CREATE TABLE %s (k text PRIMARY KEY, v text) WITH compression = {'class': 'ZstdCompressor'};");
        ColumnFamilyStore store = flushTwice();

        // Should flush as LZ4
        Set<SSTableReader> sstables = store.getLiveSSTables();
        sstables.forEach(sstable -> {
            assertTrue(sstable.getCompressionMetadata().parameters.getSstableCompressor() instanceof LZ4Compressor);
        });

        // Should compact to Zstd
        forceCompactAll();

        sstables = store.getLiveSSTables();
        assertEquals(sstables.size(), 1);
        store.getLiveSSTables().forEach(sstable -> {
            assertTrue(sstable.getCompressionMetadata().parameters.getSstableCompressor() instanceof ZstdCompressor);
        });
    }

    private ColumnFamilyStore flushTwice() throws Throwable
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
}
