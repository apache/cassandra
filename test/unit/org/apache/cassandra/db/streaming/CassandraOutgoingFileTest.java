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

package org.apache.cassandra.db.streaming;

import java.util.Arrays;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.CachingParams;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertTrue;

public class CassandraOutgoingFileTest
{
    public static final String KEYSPACE = "CassandraOutgoingFileTest";
    public static final String CF_STANDARD = "Standard1";
    public static final String CF_INDEXED = "Indexed1";
    public static final String CF_STANDARDLOWINDEXINTERVAL = "StandardLowIndexInterval";

    private static SSTableReader sstable;

    @BeforeClass
    public static void defineSchemaAndPrepareSSTable()
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE, CF_STANDARD),
                                    SchemaLoader.compositeIndexCFMD(KEYSPACE, CF_INDEXED, true),
                                    SchemaLoader.standardCFMD(KEYSPACE, CF_STANDARDLOWINDEXINTERVAL)
                                                .minIndexInterval(8)
                                                .maxIndexInterval(256)
                                                .caching(CachingParams.CACHE_NOTHING));

        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore("Standard1");

        // insert data and compact to a single sstable
        CompactionManager.instance.disableAutoCompaction();
        for (int j = 0; j < 10; j++)
        {
            new RowUpdateBuilder(store.metadata(), j, String.valueOf(j))
            .clustering("0")
            .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
            .build()
            .applyUnsafe();
        }
        store.forceBlockingFlush();
        CompactionManager.instance.performMaximal(store, false);

        sstable = store.getLiveSSTables().iterator().next();
    }

    @Test
    public void testCompleteRangeTriggersFullStreaming()
    {
        Token minToken = sstable.first.getToken();
        Token maxToken = sstable.last.getToken();

        Range<Token> requestedRange = new Range<>(minToken, maxToken);

        Range.normalize(Arrays.asList(requestedRange));

        List<SSTableReader.PartitionPositionBounds> sections = sstable.getPositionsForRanges(Arrays.asList(requestedRange));

        CassandraOutgoingFile cof = new CassandraOutgoingFile(StreamOperation.BOOTSTRAP, sstable.ref(), sections,
                                                              Arrays.asList(requestedRange), sstable.estimatedKeys());

        assertTrue(cof.shouldStreamFullSSTable());
    }
}