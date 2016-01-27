/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.db;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

import org.junit.BeforeClass;
import org.junit.Test;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class PartitionTest
{
    private static final String KEYSPACE1 = "Keyspace1";
    private static final String CF_STANDARD1 = "Standard1";
    private static final String CF_TENCOL = "TenColumns";
    private static final String CF_COUNTER1 = "Counter1";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_TENCOL, 10, AsciiType.instance),
                                    SchemaLoader.denseCFMD(KEYSPACE1, CF_COUNTER1, BytesType.instance));
    }

    @Test
    public void testSingleColumn() throws IOException
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD1);
        PartitionUpdate update = new RowUpdateBuilder(cfs.metadata, 5, "key1")
                                 .clustering("c")
                                 .add("val", "val1")
                                 .buildUpdate();

        CachedBTreePartition partition = CachedBTreePartition.create(update.unfilteredIterator(), FBUtilities.nowInSeconds());

        DataOutputBuffer bufOut = new DataOutputBuffer();
        CachedPartition.cacheSerializer.serialize(partition, bufOut);

        CachedPartition deserialized = CachedPartition.cacheSerializer.deserialize(new DataInputBuffer(bufOut.getData()));

        assert deserialized != null;
        assert deserialized.metadata().cfName.equals(CF_STANDARD1);
        assert deserialized.partitionKey().equals(partition.partitionKey());
    }

    @Test
    public void testManyColumns() throws IOException
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_TENCOL);
        RowUpdateBuilder builder = new RowUpdateBuilder(cfs.metadata, 5, "key1")
                                   .clustering("c")
                                   .add("val", "val1");

        for (int i = 0; i < 10; i++)
            builder.add("val" + i, "val" + i);

        PartitionUpdate update = builder.buildUpdate();

        CachedBTreePartition partition = CachedBTreePartition.create(update.unfilteredIterator(), FBUtilities.nowInSeconds());

        DataOutputBuffer bufOut = new DataOutputBuffer();
        CachedPartition.cacheSerializer.serialize(partition, bufOut);

        CachedPartition deserialized = CachedPartition.cacheSerializer.deserialize(new DataInputBuffer(bufOut.getData()));

        assertEquals(partition.columns().regulars.size(), deserialized.columns().regulars.size());
        assertTrue(deserialized.columns().regulars.getSimple(1).equals(partition.columns().regulars.getSimple(1)));
        assertTrue(deserialized.columns().regulars.getSimple(5).equals(partition.columns().regulars.getSimple(5)));

        ColumnDefinition cDef = cfs.metadata.getColumnDefinition(ByteBufferUtil.bytes("val8"));
        assertTrue(partition.lastRow().getCell(cDef).value().equals(deserialized.lastRow().getCell(cDef).value()));
        assert deserialized.partitionKey().equals(partition.partitionKey());
    }

    @Test
    public void testDigest() throws NoSuchAlgorithmException
    {
        testDigest(MessagingService.current_version);
    }

    @Test
    public void testLegacyDigest() throws NoSuchAlgorithmException
    {
        testDigest(MessagingService.VERSION_22);
    }

    public void testDigest(int version) throws NoSuchAlgorithmException
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_TENCOL);

        try
        {
            RowUpdateBuilder builder = new RowUpdateBuilder(cfs.metadata, 5, "key1").clustering("c").add("val", "val1");
            for (int i = 0; i < 10; i++)
                builder.add("val" + i, "val" + i);
            builder.build().applyUnsafe();

            new RowUpdateBuilder(cfs.metadata, 5, "key2").clustering("c").add("val", "val2").build().applyUnsafe();

            ReadCommand cmd1 = Util.cmd(cfs, "key1").build();
            ReadCommand cmd2 = Util.cmd(cfs, "key2").build();
            ImmutableBTreePartition p1 = Util.getOnlyPartitionUnfiltered(cmd1);
            ImmutableBTreePartition p2 = Util.getOnlyPartitionUnfiltered(cmd2);

            MessageDigest digest1 = MessageDigest.getInstance("MD5");
            MessageDigest digest2 = MessageDigest.getInstance("MD5");
            UnfilteredRowIterators.digest(cmd1, p1.unfilteredIterator(), digest1, version);
            UnfilteredRowIterators.digest(cmd2, p2.unfilteredIterator(), digest2, version);
            assertFalse(Arrays.equals(digest1.digest(), digest2.digest()));

            p1 = Util.getOnlyPartitionUnfiltered(Util.cmd(cfs, "key2").build());
            p2 = Util.getOnlyPartitionUnfiltered(Util.cmd(cfs, "key2").build());
            digest1 = MessageDigest.getInstance("MD5");
            digest2 = MessageDigest.getInstance("MD5");
            UnfilteredRowIterators.digest(cmd1, p1.unfilteredIterator(), digest1, version);
            UnfilteredRowIterators.digest(cmd2, p2.unfilteredIterator(), digest2, version);
            assertTrue(Arrays.equals(digest1.digest(), digest2.digest()));

            p1 = Util.getOnlyPartitionUnfiltered(Util.cmd(cfs, "key2").build());
            RowUpdateBuilder.deleteRow(cfs.metadata, 6, "key2", "c").applyUnsafe();
            p2 = Util.getOnlyPartitionUnfiltered(Util.cmd(cfs, "key2").build());
            digest1 = MessageDigest.getInstance("MD5");
            digest2 = MessageDigest.getInstance("MD5");
            UnfilteredRowIterators.digest(cmd1, p1.unfilteredIterator(), digest1, version);
            UnfilteredRowIterators.digest(cmd2, p2.unfilteredIterator(), digest2, version);
            assertFalse(Arrays.equals(digest1.digest(), digest2.digest()));
        }
        finally
        {
            cfs.truncateBlocking();
        }
    }

    @Test
    public void testColumnStatsRecordsRowDeletesCorrectly()
    {
        long timestamp = System.currentTimeMillis();
        int localDeletionTime = (int) (timestamp / 1000);

        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_TENCOL);
        RowUpdateBuilder builder = new RowUpdateBuilder(cfs.metadata, 5, "key1").clustering("c").add("val", "val1");
        for (int i = 0; i < 10; i++)
            builder.add("val" + i, "val" + i);
        builder.build().applyUnsafe();

        RowUpdateBuilder.deleteRowAt(cfs.metadata, 10L, localDeletionTime, "key1", "c").applyUnsafe();
        ImmutableBTreePartition partition = Util.getOnlyPartitionUnfiltered(Util.cmd(cfs, "key1").build());
        EncodingStats stats = partition.stats();
        assertEquals(localDeletionTime, stats.minLocalDeletionTime);
    }
}
