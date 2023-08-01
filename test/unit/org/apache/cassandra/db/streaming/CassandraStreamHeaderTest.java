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

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.streaming.CassandraStreamHeader.CassandraStreamHeaderSerializer;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.format.SSTableFormat.Components;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.serializers.SerializationUtils;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class CassandraStreamHeaderTest
{
    public static final String KEYSPACE = "CassandraStreamHeaderTest";
    public static final String CF_COMPRESSED = "compressed";

    private static SSTableReader sstable;
    private static ColumnFamilyStore store;

    @BeforeClass
    public static void defineSchemaAndPrepareSSTable()
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE, CF_COMPRESSED).compression(CompressionParams.DEFAULT));

        Keyspace keyspace = Keyspace.open(KEYSPACE);
        store = keyspace.getColumnFamilyStore(CF_COMPRESSED);

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
        Util.flush(store);
        CompactionManager.instance.performMaximal(store, false);

        sstable = store.getLiveSSTables().iterator().next();
    }

    @Test
    public void transferedSizeWithCompressionTest()
    {
        // compression info is lazily initialized to reduce GC, compute size based on compressionMetadata
        CassandraStreamHeader header = header(false, true);
        long transferedSize = header.size();
        assertEquals(transferedSize, header.calculateSize());

        // computing file chunks before sending over network, and verify size is the same
        header.compressionInfo.chunks();
        assertEquals(transferedSize, header.calculateSize());

        SerializationUtils.assertSerializationCycle(header, CassandraStreamHeader.serializer);
    }

    @Test
    public void transferedSizeWithZeroCopyStreamingTest()
    {
        // verify all component on-disk length is used for ZCS
        CassandraStreamHeader header = header(true, true);
        long transferedSize = header.size();
        assertEquals(ComponentManifest.create(sstable).totalSize(), transferedSize);
        assertEquals(transferedSize, header.calculateSize());

        // verify that computing file chunks doesn't change transferred size for ZCS
        header.compressionInfo.chunks();
        assertEquals(transferedSize, header.calculateSize());

        SerializationUtils.assertSerializationCycle(header, CassandraStreamHeader.serializer);
    }

    @Test
    public void transferedSizeWithoutCompressionTest()
    {
        // verify section size is used as transferred size
        CassandraStreamHeader header = header(false, false);
        long transferedSize = header.size();
        assertNull(header.compressionInfo);
        assertEquals(sstable.uncompressedLength(), transferedSize);
        assertEquals(transferedSize, header.calculateSize());

        SerializationUtils.assertSerializationCycle(header, CassandraStreamHeader.serializer);
    }

    private CassandraStreamHeader header(boolean entireSSTable, boolean compressed)
    {
        List<Range<Token>> requestedRanges = Collections.singletonList(new Range<>(store.getPartitioner().getMinimumToken(), sstable.getLast().getToken()));
        requestedRanges = Range.normalize(requestedRanges);

        List<SSTableReader.PartitionPositionBounds> sections = sstable.getPositionsForRanges(requestedRanges);
        CompressionInfo compressionInfo = compressed ? CompressionInfo.newLazyInstance(sstable.getCompressionMetadata(), sections)
                                                     : null;

        TableMetadata metadata = store.metadata();
        SerializationHeader.Component serializationHeader = SerializationHeader.makeWithoutStats(metadata).toComponent();
        ComponentManifest componentManifest = entireSSTable ? ComponentManifest.create(sstable) : null;
        DecoratedKey firstKey = entireSSTable ? sstable.getFirst() : null;

        return CassandraStreamHeader.builder()
                                    .withSSTableVersion(sstable.descriptor.version)
                                    .withSSTableLevel(0)
                                    .withEstimatedKeys(10)
                                    .withCompressionInfo(compressionInfo)
                                    .withSections(sections)
                                    .isEntireSSTable(entireSSTable)
                                    .withComponentManifest(componentManifest)
                                    .withFirstKey(firstKey)
                                    .withSerializationHeader(serializationHeader)
                                    .withTableId(metadata.id)
                                    .build();
    }

    @Test
    public void serializerTest()
    {
        String ddl = "CREATE TABLE tbl (k INT PRIMARY KEY, v INT)";
        TableMetadata metadata = CreateTableStatement.parse(ddl, "ks").build();
        CassandraStreamHeader header =
            CassandraStreamHeader.builder()
                                 .withSSTableVersion(DatabaseDescriptor.getSelectedSSTableFormat().getLatestVersion())
                                 .withSSTableLevel(0)
                                 .withEstimatedKeys(0)
                                 .withSections(Collections.emptyList())
                                 .withSerializationHeader(SerializationHeader.makeWithoutStats(metadata).toComponent())
                                 .withTableId(metadata.id)
                                 .build();

        SerializationUtils.assertSerializationCycle(header, CassandraStreamHeader.serializer);
    }

    @Test
    public void serializerTest_EntireSSTableTransfer()
    {
        String ddl = "CREATE TABLE tbl (k INT PRIMARY KEY, v INT)";
        TableMetadata metadata = CreateTableStatement.parse(ddl, "ks").build();

        ComponentManifest manifest = new ComponentManifest(new LinkedHashMap<Component, Long>() {{ put(Components.DATA, 100L); }});

        CassandraStreamHeader header =
            CassandraStreamHeader.builder()
                                 .withSSTableVersion(DatabaseDescriptor.getSelectedSSTableFormat().getLatestVersion())
                                 .withSSTableLevel(0)
                                 .withEstimatedKeys(0)
                                 .withSections(Collections.emptyList())
                                 .withSerializationHeader(SerializationHeader.makeWithoutStats(metadata).toComponent())
                                 .withComponentManifest(manifest)
                                 .isEntireSSTable(true)
                                 .withFirstKey(Murmur3Partitioner.instance.decorateKey(ByteBufferUtil.EMPTY_BYTE_BUFFER))
                                 .withTableId(metadata.id)
                                 .build();

        SerializationUtils.assertSerializationCycle(header, new TestableCassandraStreamHeaderSerializer());
    }

    private static class TestableCassandraStreamHeaderSerializer extends CassandraStreamHeaderSerializer
    {
        @Override
        public CassandraStreamHeader deserialize(DataInputPlus in, int version) throws IOException
        {
            return deserialize(in, version, tableId -> Murmur3Partitioner.instance);
        }
    }
}
