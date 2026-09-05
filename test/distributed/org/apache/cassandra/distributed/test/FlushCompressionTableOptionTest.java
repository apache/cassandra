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

package org.apache.cassandra.distributed.test;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.compress.LZ4Compressor;
import org.apache.cassandra.io.compress.NoopCompressor;
import org.apache.cassandra.io.compress.ZstdCompressor;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableId;
import org.apache.cassandra.io.sstable.SSTableIdFactory;
import org.apache.cassandra.io.sstable.format.CompressionInfoComponent;
import org.apache.cassandra.io.sstable.format.SSTableFormat.Components;
import org.apache.cassandra.io.util.File;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code flush_compression} table option on a running node: the compressor recorded in the
 * CompressionInfo component of flushed SSTables is read from disk, in SSTable id order, and compared
 * against the table option and the {@code flush_compression} yaml setting. Automatic compaction is
 * disabled on every node start so flushed SSTables are never rewritten.
 */
public class FlushCompressionTableOptionTest extends TestBaseImpl
{
    private static final String ZSTD = "{'class': 'ZstdCompressor'}";

    @Test
    public void tableOptionOverridesYamlTable() throws Throwable
    {
        try (Cluster cluster = start("table"))
        {
            createTables(cluster, "auto", "fast", "none", "table");
            flushAll(cluster, "auto", "fast", "none", "table");

            assertFlushedWith(cluster.get(1), tableFor("auto"), ZstdCompressor.class);
            assertFlushedWith(cluster.get(1), tableFor("fast"), LZ4Compressor.class);
            assertFlushedWith(cluster.get(1), tableFor("none"), NoopCompressor.class);
            assertFlushedWith(cluster.get(1), tableFor("table"), ZstdCompressor.class);
        }
    }

    @Test
    public void tableOptionOverridesYamlNone() throws Throwable
    {
        try (Cluster cluster = start("none"))
        {
            createTables(cluster, "auto", "fast", "none", "table");
            flushAll(cluster, "auto", "fast", "none", "table");

            assertFlushedWith(cluster.get(1), tableFor("auto"), NoopCompressor.class);
            assertFlushedWith(cluster.get(1), tableFor("fast"), LZ4Compressor.class);
            assertFlushedWith(cluster.get(1), tableFor("none"), NoopCompressor.class);
            assertFlushedWith(cluster.get(1), tableFor("table"), ZstdCompressor.class);
        }
    }

    @Test
    public void tableOptionOverridesYamlFast() throws Throwable
    {
        try (Cluster cluster = start("fast"))
        {
            createTables(cluster, "auto", "fast", "none", "table");
            flushAll(cluster, "auto", "fast", "none", "table");

            assertFlushedWith(cluster.get(1), tableFor("auto"), LZ4Compressor.class);
            assertFlushedWith(cluster.get(1), tableFor("fast"), LZ4Compressor.class);
            assertFlushedWith(cluster.get(1), tableFor("none"), NoopCompressor.class);
            assertFlushedWith(cluster.get(1), tableFor("table"), ZstdCompressor.class);
        }
    }

    @Test
    public void alterAppliesToNextFlushAndSurvivesRestart() throws Throwable
    {
        try (Cluster cluster = start("fast"))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (k int PRIMARY KEY, v text) WITH compression = " + ZSTD));
            insertAndFlush(cluster, "tbl", 1);
            assertFlushedWith(cluster.get(1), "tbl", LZ4Compressor.class);

            cluster.schemaChange(withKeyspace("ALTER TABLE %s.tbl WITH flush_compression = 'table'"));
            insertAndFlush(cluster, "tbl", 2);
            assertFlushedWith(cluster.get(1), "tbl", LZ4Compressor.class, ZstdCompressor.class);

            cluster.get(1).shutdown().get();
            cluster.get(1).startup();
            disableAutoCompaction(cluster);

            Object[][] rows = cluster.coordinator(1).execute(withKeyspace("SELECT flush_compression FROM system_schema.tables WHERE keyspace_name = '%s' AND table_name = 'tbl'"), ConsistencyLevel.ONE);
            assertThat(rows[0][0]).isEqualTo("table");

            insertAndFlush(cluster, "tbl", 3);
            assertFlushedWith(cluster.get(1), "tbl", LZ4Compressor.class, ZstdCompressor.class, ZstdCompressor.class);
        }
    }

    private static Cluster start(String yamlFlushCompression) throws Throwable
    {
        Cluster cluster = init(Cluster.build(1).withConfig(c -> c.set("flush_compression", yamlFlushCompression)).start());
        disableAutoCompaction(cluster);
        return cluster;
    }

    private static void disableAutoCompaction(Cluster cluster)
    {
        cluster.get(1).nodetoolResult("disableautocompaction").asserts().success();
    }

    /** One table per option, named {@code opt_<option>}, all compressed with Zstd. */
    private static void createTables(Cluster cluster, String... options)
    {
        for (String option : options)
            cluster.schemaChange(withKeyspace("CREATE TABLE %s." + tableFor(option) + " (k int PRIMARY KEY, v text) WITH compression = " + ZSTD + " AND flush_compression = '" + option + '\''));
    }

    private static void flushAll(Cluster cluster, String... options)
    {
        for (String option : options)
            insertAndFlush(cluster, tableFor(option), 1);
    }

    private static String tableFor(String option)
    {
        return "opt_" + option;
    }

    private static void insertAndFlush(Cluster cluster, String table, int key)
    {
        cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s." + table + " (k, v) VALUES (?, ?)"), ConsistencyLevel.ONE, key, "v" + key);
        cluster.get(1).nodetool("flush", KEYSPACE, table);
    }

    @SafeVarargs
    private static void assertFlushedWith(IInvokableInstance instance, String table, Class<?>... expected)
    {
        List<String> expectedNames = new ArrayList<>();
        for (Class<?> c : expected)
            expectedNames.add(c.getName());

        List<String> actual = instance.callOnInstance(() -> compressorsOnDisk(KEYSPACE, table));
        assertThat(actual).describedAs("compressors recorded in CompressionInfo components of %s, in SSTable id order", table)
                          .containsExactlyElementsOf(expectedNames);
    }

    /**
     * Compressor class names read from the CompressionInfo component of every SSTable in the table directories,
     * ordered by SSTable id (flush order), independent of the SSTableReader instances held by the ColumnFamilyStore.
     */
    private static List<String> compressorsOnDisk(String keyspace, String table)
    {
        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        TreeMap<SSTableId, String> compressors = new TreeMap<>(SSTableIdFactory.COMPARATOR);
        for (File dir : cfs.getDirectories().getCFDirectories())
        {
            for (File file : dir.tryList())
            {
                if (!file.name().endsWith(Components.COMPRESSION_INFO.name))
                    continue;

                Descriptor descriptor = Descriptor.fromFile(file);
                try (CompressionMetadata metadata = CompressionInfoComponent.load(descriptor, null))
                {
                    compressors.put(descriptor.id, metadata.compressor().getClass().getName());
                }
            }
        }
        return new ArrayList<>(compressors.values());
    }
}
