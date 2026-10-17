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
import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compression.CompressionDictionary;
import org.apache.cassandra.db.compression.CompressionDictionaryManager;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;

import static org.apache.cassandra.Util.spinUntilTrue;
import static org.apache.cassandra.io.compress.IDictionaryCompressor.TRAINING_MAX_DICTIONARY_SIZE_PARAMETER_NAME;
import static org.apache.cassandra.io.compress.IDictionaryCompressor.TRAINING_MAX_TOTAL_SAMPLE_SIZE_PARAMETER_NAME;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Restores a dropped table from a snapshot. Without the fix:
 * <ol>
 * <li>nodetool compact writes the sstable with dictionary object D1; the compressor cache keeps compressor C1 using
 * D1.</li>
 * <li>DROP TABLE releases every reference to D1 except the one of C1.</li>
 * <li>nodetool import reads the dictionary again, as object D2 with the same id.</li>
 * <li>The restored sstable gets C1 for D2, as the cache compares dictionaries by id.</li>
 * <li>nodetool compact of another table evicts C1, as the cache holds one compressor here. That releases the last
 * reference to D1, so the next read with C1 fails with "Dictionary is released".</li>
 * </ol>
 */
public class CompressionDictionaryRestoreTest extends TestBaseImpl
{
    private static final String TABLE = "tbl";
    private static final String OTHER = "other";
    private static final int ROWS = 5000;

    @Test
    public void restoreDroppedTable() throws Throwable
    {
        // The cache of compressors is shared by all tables of a node and holds compression_dictionary_cache_size
        // compressors, 10 by default. Holding one stands for a node with more tables compressed with a dictionary than
        // the cache holds.
        try (Cluster cluster = init(Cluster.build(1)
                                           .withDataDirCount(1)
                                           .withConfig(c -> c.set("compression_dictionary_cache_size", 1))
                                           .start()))
        {
            // trained dictionaries are stored and read at QUORUM, which a single node reaches only with one replica
            cluster.schemaChange("ALTER KEYSPACE system_distributed WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
            createTable(cluster, TABLE);
            long dictId = writeAndCompactWithDictionary(cluster, TABLE);
            cluster.get(1).nodetoolResult("snapshot", "-t", "backup", "-kt", KEYSPACE + '.' + TABLE).asserts().success();
            String backup = snapshotDirectory(cluster, TABLE, "backup");

            cluster.schemaChange(withKeyspace("DROP TABLE %s." + TABLE));
            createTable(cluster, TABLE);
            cluster.get(1).nodetoolResult("import", "--copy-data", KEYSPACE, TABLE, backup).asserts().success();
            assertThat(sstableDictionaries(cluster, TABLE)).containsExactly(dictId);
            assertRows(cluster, TABLE);

            createTable(cluster, OTHER);
            writeAndCompactWithDictionary(cluster, OTHER);

            assertRows(cluster, TABLE);
        }
    }

    private static void createTable(Cluster cluster, String table)
    {
        cluster.schemaChange(withKeyspace("CREATE TABLE %s." + table + " (pk int PRIMARY KEY, v text) " +
                                          "WITH compression = {'class': 'ZstdDictionaryCompressor', 'chunk_length_in_kb': '4'}"));
    }

    /**
     * Writes and flushes rows, trains a dictionary on the flushed sstable, as nodetool compressiondictionary train
     * does, and compacts the sstable. A flush does not compress with the dictionary (flush_compression is fast), the
     * compaction does.
     *
     * @return id of the dictionary
     */
    private static long writeAndCompactWithDictionary(Cluster cluster, String table)
    {
        String insert = withKeyspace("INSERT INTO %s." + table + " (pk, v) VALUES (?, ?)");
        for (int pk = 0; pk < ROWS; pk++)
            cluster.get(1).executeInternal(insert, pk, value(pk));
        cluster.get(1).flush(KEYSPACE);

        long dictId = cluster.get(1).callOnInstance(() -> {
            CompressionDictionaryManager manager = Keyspace.open(KEYSPACE).getColumnFamilyStore(table).compressionDictionaryManager();
            // samples are chunks of the flushed sstable, 16KiB each, up to 1MiB of them; training needs at least 11
            manager.train(true, Map.of(TRAINING_MAX_DICTIONARY_SIZE_PARAMETER_NAME, "10KiB",
                                       TRAINING_MAX_TOTAL_SAMPLE_SIZE_PARAMETER_NAME, "1MiB"));
            spinUntilTrue(() -> manager.getCurrent() != null, 30);
            return manager.getCurrent().dictId().id;
        });

        cluster.get(1).nodetoolResult("compact", KEYSPACE, table).asserts().success();
        return dictId;
    }

    private static String value(int pk)
    {
        return "2025-10-20 11:00:01 INFO com.example.UserService request " + pk + " processed in 12ms";
    }

    private static void assertRows(Cluster cluster, String table)
    {
        assertThat(cluster.get(1).executeInternal(withKeyspace("SELECT count(*) FROM %s." + table))[0][0]).isEqualTo((long) ROWS);
        assertThat(cluster.get(1).executeInternal(withKeyspace("SELECT v FROM %s." + table + " WHERE pk = ?"), 7)[0][0]).isEqualTo(value(7));
    }

    /** ids of the dictionaries the live sstables of the table were written with, one per sstable, 0 for none */
    private static List<Long> sstableDictionaries(Cluster cluster, String table)
    {
        return cluster.get(1).callOnInstance(() -> {
            List<Long> result = new ArrayList<>();
            for (SSTableReader sstable : Keyspace.open(KEYSPACE).getColumnFamilyStore(table).getLiveSSTables())
            {
                CompressionDictionary dictionary = sstable.getCompressionMetadata().compressionDictionary();
                result.add(dictionary == null ? 0L : dictionary.dictId().id);
            }
            return result;
        });
    }

    private static String snapshotDirectory(Cluster cluster, String table, String tag)
    {
        return cluster.get(1).callOnInstance(() -> {
            File tableDirectory = Keyspace.open(KEYSPACE).getColumnFamilyStore(table).getDirectories().getCFDirectories().get(0);
            return Directories.getSnapshotDirectoryWithoutCreation(tableDirectory, tag).absolutePath();
        });
    }
}
