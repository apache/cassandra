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
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import javax.xml.crypto.Data;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.UpdateBuilder;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.lifecycle.Tracker;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.schema.CachingParams;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.io.util.DataInputPlus.*;
import static org.junit.Assert.*;

public class BigTableBlockWriterTest
{
    public static final String KEYSPACE1 = "BigTableBlockWriterTest";
    public static final String CF_STANDARD = "Standard1";
    public static final String CF_STANDARD2 = "Standard2";
    public static final String CF_INDEXED = "Indexed1";
    public static final String CF_STANDARDLOWINDEXINTERVAL = "StandardLowIndexInterval";

    private static ColumnFamilyStore cfs;

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
    }

    @Test
    public void writeDataFile()
    {
        String ks = KEYSPACE1;
        String cf = "Standard1";

        // clear and create just one sstable for this test
        Keyspace keyspace = Keyspace.open(ks);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore(cf);
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
        }
        store.forceBlockingFlush();

        SSTableReader sstable = store.getLiveSSTables().iterator().next();
        File dir = store.getDirectories().getDirectoryForNewSSTables();
        Descriptor desc = store.newSSTableDescriptor(dir);
        TableMetadataRef metadata = Schema.instance.getTableMetadataRef(desc);

        LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.STREAM);
        Set<Component> componentsToWrite = ImmutableSet.of(Component.DATA, Component.PRIMARY_INDEX,
                                                           Component.STATS);

        BigTableBlockWriter btbw = new BigTableBlockWriter(desc, metadata, txn, componentsToWrite);

        for (Component component : componentsToWrite)
        {
            if (Files.exists(Paths.get(desc.filenameFor(component))))
            {
                Pair<DataInputStreamPlus, Long> pair = getSSTableComponentData(sstable, component);
                btbw.writeComponent(component.type, pair.left, pair.right);
            }
        }

        Collection<SSTableReader> readers = btbw.finish(true);

        for (SSTableReader reader : readers)
        {
            System.out.printf("File: %s Generation: %s\n", reader.descriptor.filenameFor(Component.DATA),
                              reader.descriptor.generation);
        }

        SSTableReader reader = readers.toArray(new SSTableReader[0])[0];

        assertNotEquals(sstable.getFilename(), reader.getFilename());
        assertEquals(sstable.estimatedKeys(), reader.estimatedKeys());
        assertEquals(sstable.isPendingRepair(), reader.isPendingRepair());
    }

    private Pair<DataInputStreamPlus, Long> getSSTableComponentData(SSTableReader sstable, Component component)
    {
        FileHandle componentFile = new FileHandle.Builder(sstable.descriptor.filenameFor(component))
                               .bufferSize(1024).complete();

        ByteBuffer buffer = ByteBuffer.allocate((int) componentFile.channel.size());
        componentFile.channel.read(buffer, 0);
        DataInputStreamPlus is = new DataInputStreamPlus(new ByteArrayInputStream(buffer.array()));

        return Pair.create(is, componentFile.channel.size());
    }

    public static ByteBuffer random(int i, int size)
    {
        byte[] bytes = new byte[size + 4];
        ThreadLocalRandom.current().nextBytes(bytes);
        ByteBuffer r = ByteBuffer.wrap(bytes);
        r.putInt(0, i);
        return r;
    }

    public static void truncate(ColumnFamilyStore cfs)
    {
        cfs.truncateBlocking();
        LifecycleTransaction.waitForDeletions();
        Uninterruptibles.sleepUninterruptibly(10L, TimeUnit.MILLISECONDS);
        assertEquals(0, cfs.metric.liveDiskSpaceUsed.getCount());
        assertEquals(0, cfs.metric.totalDiskSpaceUsed.getCount());
    }
}