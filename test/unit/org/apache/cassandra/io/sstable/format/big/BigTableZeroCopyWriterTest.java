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

package org.apache.cassandra.io.sstable.format.big;

import java.io.ByteArrayInputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Set;
import java.util.function.Function;

import com.google.common.collect.ImmutableSet;
import org.apache.cassandra.io.util.File;
import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReadsListener;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.net.AsyncStreamingInputPlus;
import org.apache.cassandra.schema.CachingParams;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.io.util.DataInputPlus.DataInputStreamPlus;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class BigTableZeroCopyWriterTest
{
    public static final String KEYSPACE1 = "BigTableBlockWriterTest";
    public static final String CF_STANDARD = "Standard1";
    public static final String CF_STANDARD2 = "Standard2";
    public static final String CF_INDEXED = "Indexed1";
    public static final String CF_STANDARDLOWINDEXINTERVAL = "StandardLowIndexInterval";

    public static SSTableReader sstable;
    public static ColumnFamilyStore store;
    private static int expectedRowCount;

    @BeforeClass
    public static void defineSchema() throws Exception
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD2),
                                    SchemaLoader.compositeIndexCFMD(KEYSPACE1, CF_INDEXED, true),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARDLOWINDEXINTERVAL)
                                                .minIndexInterval(8)
                                                .maxIndexInterval(256)
                                                .caching(CachingParams.CACHE_NOTHING));

        String ks = KEYSPACE1;
        String cf = "Standard1";

        // clear and create just one sstable for this test
        Keyspace keyspace = Keyspace.open(ks);
        store = keyspace.getColumnFamilyStore(cf);
        store.clearUnsafe();
        store.disableAutoCompaction();

        DecoratedKey firstKey = null, lastKey = null;
        long timestamp = System.currentTimeMillis();
        for (int i = 0; i < store.metadata().params.minIndexInterval; i++)
        {
            DecoratedKey key = Util.dk(String.valueOf(i));
            if (firstKey == null)
                firstKey = key;
            if (lastKey == null)
                lastKey = key;
            if (store.metadata().partitionKeyType.compare(lastKey.getKey(), key.getKey()) < 0)
                lastKey = key;

            new RowUpdateBuilder(store.metadata(), timestamp, key.getKey())
            .clustering("col")
            .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
            .build()
            .applyUnsafe();
            expectedRowCount++;
        }
        Util.flush(store);

        sstable = store.getLiveSSTables().iterator().next();
    }

    @Test
    public void writeDataFile_DataInputPlus()
    {
        writeDataTestCycle(buffer -> new DataInputStreamPlus(new ByteArrayInputStream(buffer.array())));
    }

    @Test
    public void writeDataFile_RebufferingByteBufDataInputPlus()
    {
        try (AsyncStreamingInputPlus input = new AsyncStreamingInputPlus(new EmbeddedChannel()))
        {
            writeDataTestCycle(buffer ->
            {
                input.append(Unpooled.wrappedBuffer(buffer));
                return input;
            });

            input.requestClosure();
        }
    }


    private void writeDataTestCycle(Function<ByteBuffer, DataInputPlus> bufferMapper)
    {
        File dir = store.getDirectories().getDirectoryForNewSSTables();
        Descriptor desc = store.newSSTableDescriptor(dir);
        TableMetadataRef metadata = Schema.instance.getTableMetadataRef(desc);

        LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.STREAM);
        Set<Component> componentsToWrite = ImmutableSet.of(Component.DATA, Component.PRIMARY_INDEX,
                                                           Component.STATS);

        BigTableZeroCopyWriter btzcw = new BigTableZeroCopyWriter(desc, metadata, txn, componentsToWrite);

        for (Component component : componentsToWrite)
        {
            if (Files.exists(Paths.get(desc.filenameFor(component))))
            {
                Pair<DataInputPlus, Long> pair = getSSTableComponentData(sstable, component, bufferMapper);

                try
                {
                    btzcw.writeComponent(component.type, pair.left, pair.right);
                }
                catch (ClosedChannelException e)
                {
                    throw new UncheckedIOException(e);
                }
            }
        }

        Collection<SSTableReader> readers = btzcw.finish(true);

        SSTableReader reader = readers.toArray(new SSTableReader[0])[0];

        assertNotEquals(sstable.getFilename(), reader.getFilename());
        assertEquals(sstable.estimatedKeys(), reader.estimatedKeys());
        assertEquals(sstable.isPendingRepair(), reader.isPendingRepair());

        assertRowCount(expectedRowCount);
    }

    private void assertRowCount(int expected)
    {
        int count = 0;
        for (int i = 0; i < store.metadata().params.minIndexInterval; i++)
        {
            DecoratedKey dk = Util.dk(String.valueOf(i));
            UnfilteredRowIterator rowIter = sstable.rowIterator(dk,
                                                                Slices.ALL,
                                                                ColumnFilter.all(store.metadata()),
                                                                false,
                                                                SSTableReadsListener.NOOP_LISTENER);
            while (rowIter.hasNext())
            {
                rowIter.next();
                count++;
            }
        }
        assertEquals(expected, count);
    }

    private Pair<DataInputPlus, Long> getSSTableComponentData(SSTableReader sstable, Component component,
                                                              Function<ByteBuffer, DataInputPlus> bufferMapper)
    {
        FileHandle componentFile = new FileHandle.Builder(sstable.descriptor.filenameFor(component))
                                   .bufferSize(1024).complete();
        ByteBuffer buffer = ByteBuffer.allocate((int) componentFile.channel.size());
        componentFile.channel.read(buffer, 0);
        buffer.flip();

        DataInputPlus inputPlus = bufferMapper.apply(buffer);

        return Pair.create(inputPlus, componentFile.channel.size());
    }
}
