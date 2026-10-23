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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compression.CompressionDictionary;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.tools.BulkLoader;
import org.apache.cassandra.tools.ToolRunner;
import org.apache.cassandra.utils.CompressionDictionaryHelper;

import static java.util.stream.Collectors.toList;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Streaming a part of an sstable sends its compressed chunks as they are, so the receiver has to decompress them with
 * the dictionary they were compressed with. The stream carries that dictionary, so the receiver does not need to know
 * it already.
 */
public class CompressionDictionaryStreamingTest extends TestBaseImpl
{
    private static final String TABLE = "tbl";
    private static final String RESTORED = "restored";
    private static final int ROWS = 2000;

    /**
     * Repair streams an sstable to a node which does not know its dictionary. Entire-sstable streaming is disabled, so
     * the chunks are streamed and decompressed by the receiver.
     */
    @Test
    public void partialSSTable() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(2)
                                           .withConfig(c -> c.with(NETWORK, GOSSIP)
                                                             .set("flush_compression", "table")
                                                             .set("stream_entire_sstables", false)
                                                             .set("compression_dictionary_refresh_initial_delay", "1h"))
                                           .start()))
        {
            createTable(cluster, TABLE);
            long dictId = writeWithDictionary(cluster);
            assertThat(sstableDictionaries(cluster)).containsOnly(dictId);

            cluster.get(2).nodetoolResult("repair", "--full", KEYSPACE, TABLE).asserts().success();

            assertThat(count(cluster, 2, TABLE)).isEqualTo(ROWS);
            assertThat(cluster.get(2).executeInternal(withKeyspace("SELECT v FROM %s." + TABLE + " WHERE pk = ?"), 7)[0][0]).isEqualTo(value(7));
        }
    }

    /**
     * sstableloader streams to each node only the part of an sstable in the ranges the node owns. The nodes never knew
     * the dictionary, as when a backup is restored into another cluster.
     */
    @Test
    public void bulkLoad() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(2)
                                           .withDataDirCount(1)
                                           .withConfig(c -> c.with(NETWORK, GOSSIP, NATIVE_PROTOCOL)
                                                             .set("flush_compression", "table")
                                                             .set("compression_dictionary_refresh_initial_delay", "1h"))
                                           .start(), 1))
        {
            createTable(cluster, TABLE);
            createTable(cluster, RESTORED);
            long dictId = writeWithDictionary(cluster);
            // a single sstable with all rows, each node owns only a part of it
            assertThat(sstableDictionaries(cluster)).containsExactly(dictId);

            Path backup = Files.createTempDirectory("backup");
            try
            {
                ToolRunner.invokeClass(BulkLoader.class,
                                       "--nodes", cluster.get(1).config().broadcastAddress().getHostString(),
                                       "--port", Integer.toString(cluster.get(1).callOnInstance(DatabaseDescriptor::getNativeTransportPort)),
                                       "--storage-port", Integer.toString(cluster.get(1).callOnInstance(DatabaseDescriptor::getStoragePort)),
                                       copySSTables(cluster, backup, RESTORED).absolutePath())
                          .assertOnCleanExit();
            }
            finally
            {
                new File(backup).deleteRecursive();
            }

            long first = count(cluster, 1, RESTORED);
            assertThat(first).isPositive().isLessThan(ROWS);
            assertThat(first + count(cluster, 2, RESTORED)).isEqualTo(ROWS);
            assertThat(cluster.coordinator(1).execute(withKeyspace("SELECT v FROM %s." + RESTORED + " WHERE pk = ?"), ConsistencyLevel.ONE, 7)[0][0]).isEqualTo(value(7));
        }
    }

    private static void createTable(Cluster cluster, String table)
    {
        cluster.schemaChange(withKeyspace("CREATE TABLE %s." + table + " (pk int PRIMARY KEY, v text) " +
                                          "WITH compression = {'class': 'ZstdDictionaryCompressor', 'chunk_length_in_kb': '4'}"));
    }

    /**
     * Trains a dictionary which only node 1 knows, then writes all rows on node 1 and flushes them with it.
     *
     * @return id of the dictionary
     */
    private static long writeWithDictionary(Cluster cluster)
    {
        long dictId = cluster.get(1).callOnInstance(() -> {
            CompressionDictionary dictionary = CompressionDictionaryHelper.INSTANCE.trainDictionary(KEYSPACE, TABLE);
            Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE).compressionDictionaryManager().add(dictionary);
            return dictionary.dictId().id;
        });

        String insert = withKeyspace("INSERT INTO %s." + TABLE + " (pk, v) VALUES (?, ?)");
        for (int pk = 0; pk < ROWS; pk++)
            cluster.get(1).executeInternal(insert, pk, value(pk));
        cluster.get(1).flush(KEYSPACE);
        return dictId;
    }

    private static String value(int pk)
    {
        return "2025-10-20 11:00:01 INFO com.example.UserService request " + pk + " processed in 12ms";
    }

    private static long count(Cluster cluster, int node, String table)
    {
        return (long) cluster.get(node).executeInternal(withKeyspace("SELECT count(*) FROM %s." + table))[0][0];
    }

    /** ids of the dictionaries the live sstables of node 1 were written with, one per sstable, 0 for none */
    private static List<Long> sstableDictionaries(Cluster cluster)
    {
        return cluster.get(1).callOnInstance(() -> {
            List<Long> result = new ArrayList<>();
            for (SSTableReader sstable : Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE).getLiveSSTables())
            {
                CompressionDictionary dictionary = sstable.getCompressionMetadata().compressionDictionary();
                result.add(dictionary == null ? 0L : dictionary.dictId().id);
            }
            return result;
        });
    }

    /**
     * Copies the sstable files of node 1 into a directory laid out as keyspace/table, which sstableloader streams
     * into the given table.
     */
    private static File copySSTables(Cluster cluster, Path dir, String table) throws IOException
    {
        File tableDir = new File(dir.resolve(KEYSPACE).resolve(table));
        tableDir.tryCreateDirectories();
        List<String> sstableDirs = cluster.get(1).callOnInstance(() -> Keyspace.open(KEYSPACE)
                                                                               .getColumnFamilyStore(TABLE)
                                                                               .getDirectories()
                                                                               .getCFDirectories()
                                                                               .stream()
                                                                               .map(File::absolutePath)
                                                                               .collect(toList()));
        for (String sstableDir : sstableDirs)
        {
            for (File file : new File(sstableDir).tryList(File::isFile))
                Files.copy(file.toPath(), tableDir.toPath().resolve(file.name()));
        }
        return tableDir;
    }
}
