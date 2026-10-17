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

import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compression.CompressionDictionary;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.CompressionDictionaryHelper;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * A stream of a part of an sstable carries its compressed chunks and the dictionary they were compressed with,
 * a receiver which does not know the dictionary decompresses them with it.
 */
public class CompressionDictionaryStreamingTest extends TestBaseImpl
{
    private static final String TABLE = "tbl";
    private static final int ROWS = 2000;

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
            cluster.schemaChange(withKeyspace("CREATE TABLE %s." + TABLE + " (pk int PRIMARY KEY, v text) " +
                                              "WITH compression = {'class': 'ZstdDictionaryCompressor', 'chunk_length_in_kb': '4'}"));

            long dictId = cluster.get(1).callOnInstance(() -> {
                CompressionDictionary dictionary = CompressionDictionaryHelper.INSTANCE.trainDictionary(KEYSPACE, TABLE);
                Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE).compressionDictionaryManager().add(dictionary);
                return dictionary.dictId().id;
            });

            String insert = withKeyspace("INSERT INTO %s." + TABLE + " (pk, v) VALUES (?, ?)");
            for (int pk = 0; pk < ROWS; pk++)
                cluster.get(1).executeInternal(insert, pk, value(pk));
            cluster.get(1).flush(KEYSPACE);
            assertThat(sstableDictionaries(cluster, 1)).containsOnly(dictId);

            cluster.get(2).nodetoolResult("repair", "--full", KEYSPACE, TABLE).asserts().success();

            assertThat(cluster.get(2).executeInternal(withKeyspace("SELECT count(*) FROM %s." + TABLE))[0][0]).isEqualTo((long) ROWS);
            assertThat(cluster.get(2).executeInternal(withKeyspace("SELECT v FROM %s." + TABLE + " WHERE pk = ?"), 7)[0][0]).isEqualTo(value(7));
        }
    }

    private static String value(int pk)
    {
        return "2025-10-20 11:00:01 INFO com.example.UserService request " + pk + " processed in 12ms";
    }

    /** ids of the dictionaries the live sstables were written with, 0 for none */
    private static Set<Long> sstableDictionaries(Cluster cluster, int node)
    {
        return cluster.get(node).callOnInstance(() -> {
            Set<Long> result = new HashSet<>();
            for (SSTableReader sstable : Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE).getLiveSSTables())
            {
                CompressionDictionary dictionary = sstable.getCompressionMetadata().compressionDictionary();
                result.add(dictionary == null ? 0L : dictionary.dictId().id);
            }
            return result;
        });
    }
}
