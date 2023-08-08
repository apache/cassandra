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

package org.apache.cassandra.index.sai.disk.v1.keystore;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.io.IndexOutputWriter;
import org.apache.cassandra.index.sai.disk.v1.MetadataSource;
import org.apache.cassandra.index.sai.disk.v1.MetadataWriter;
import org.apache.cassandra.index.sai.disk.v1.SAICodecUtils;
import org.apache.cassandra.index.sai.disk.v1.bitpack.NumericValuesMeta;
import org.apache.cassandra.index.sai.disk.v1.bitpack.NumericValuesWriter;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.SAIRandomizedTester;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;
import org.apache.lucene.store.IndexInput;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class KeyLookupTest extends SAIRandomizedTester
{
    protected IndexDescriptor indexDescriptor;

    @Before
    public void setup() throws Exception
    {
        indexDescriptor = newIndexDescriptor();
    }

    @Test
    public void testFileValidation() throws Exception
    {
        List<PrimaryKey> primaryKeys = new ArrayList<>();

        for (int x = 0; x < 11; x++)
        {
            ByteBuffer buffer = UTF8Type.instance.decompose(Integer.toString(x));
            DecoratedKey partitionKey = Murmur3Partitioner.instance.decorateKey(buffer);
            PrimaryKey primaryKey = SAITester.TEST_FACTORY.create(partitionKey);
            primaryKeys.add(primaryKey);
        }

        primaryKeys.sort(PrimaryKey::compareTo);

        try (MetadataWriter metadataWriter = new MetadataWriter(indexDescriptor.openPerSSTableOutput(IndexComponent.GROUP_META)))
        {
            IndexOutputWriter bytesWriter = indexDescriptor.openPerSSTableOutput(IndexComponent.PARTITION_KEY_BLOCKS);
            NumericValuesWriter blockFPWriter = new NumericValuesWriter(indexDescriptor, IndexComponent.PARTITION_KEY_BLOCK_OFFSETS, metadataWriter, true);
            try (KeyStoreWriter writer = new KeyStoreWriter(indexDescriptor.componentName(IndexComponent.PARTITION_KEY_BLOCKS),
                                                            metadataWriter,
                                                            bytesWriter,
                                                            blockFPWriter,
                                                            4,
                                                            false))
            {
                primaryKeys.forEach(primaryKey -> {
                    try
                    {
                        writer.add(primaryKey);
                    }
                    catch (IOException e)
                    {
                        e.printStackTrace();
                    }
                });
            }
        }
        assertTrue(validateComponent(IndexComponent.PARTITION_KEY_BLOCKS, true));
        assertTrue(validateComponent(IndexComponent.PARTITION_KEY_BLOCKS, false));
        assertTrue(validateComponent(IndexComponent.PARTITION_KEY_BLOCK_OFFSETS, true));
        assertTrue(validateComponent(IndexComponent.PARTITION_KEY_BLOCK_OFFSETS, false));
    }

    @Test
    public void testLongPrefixesAndSuffixes() throws Exception
    {
        List<byte[]> keys = new ArrayList<>();
        writeKeys(writer ->  {
            // The following writes a set of keys that cover the following conditions:

            // Start value 0
            byte[] bytes = new byte[20];
            keys.add(bytes);
            writer.add(ByteComparable.fixedLength(bytes));
            // prefix > 15
            bytes = new byte[20];
            Arrays.fill(bytes, 16, 20, (byte)1);
            keys.add(bytes);
            writer.add(ByteComparable.fixedLength(bytes));
            // prefix == 15
            bytes = new byte[20];
            Arrays.fill(bytes, 15, 20, (byte)1);
            keys.add(bytes);
            writer.add(ByteComparable.fixedLength(bytes));
            // prefix < 15
            bytes = new byte[20];
            Arrays.fill(bytes, 14, 20, (byte)1);
            keys.add(bytes);
            writer.add(ByteComparable.fixedLength(bytes));
            // suffix > 16
            bytes = new byte[20];
            Arrays.fill(bytes, 0, 4, (byte)1);
            keys.add(bytes);
            writer.add(ByteComparable.fixedLength(bytes));
            // suffix == 16
            bytes = new byte[20];
            Arrays.fill(bytes, 0, 5, (byte)1);
            keys.add(bytes);
            writer.add(ByteComparable.fixedLength(bytes));
            // suffix < 16
            bytes = new byte[20];
            Arrays.fill(bytes, 0, 6, (byte)1);
            keys.add(bytes);
            writer.add(ByteComparable.fixedLength(bytes));

            bytes = new byte[32];
            Arrays.fill(bytes, 0, 16, (byte)1);
            keys.add(bytes);
            writer.add(ByteComparable.fixedLength(bytes));
            // prefix >= 15 && suffix >= 16
            bytes = new byte[32];
            Arrays.fill(bytes, 0, 32, (byte)1);
            keys.add(bytes);
            writer.add(ByteComparable.fixedLength(bytes));
        }, false);

        doTestKeyLookup(keys);
    }

    @Test
    public void testNonUniqueKeys() throws Exception
    {
        List<byte[]> keys = new ArrayList<>();

        writeKeys(writer ->  {
            for (int x = 0; x < 4000; x++)
            {
                ByteBuffer buffer = Int32Type.instance.decompose(5000);
                ByteSource byteSource = Int32Type.instance.asComparableBytes(buffer, ByteComparable.Version.OSS50);
                byte[] bytes = ByteSourceInverse.readBytes(byteSource);
                keys.add(bytes);

                writer.add(ByteComparable.fixedLength(bytes));
            }
        }, false);

        doTestKeyLookup(keys);
    }

    @Test
    public void testSeekToPointId() throws Exception
    {
        List<byte[]> keys = new ArrayList<>();

        writeKeys(writer -> {
            for (int x = 0; x < 4000; x++)
            {
                ByteBuffer buffer = Int32Type.instance.decompose(x);
                ByteSource byteSource = Int32Type.instance.asComparableBytes(buffer, ByteComparable.Version.OSS50);
                byte[] bytes = ByteSourceInverse.readBytes(byteSource);
                keys.add(bytes);

                writer.add(ByteComparable.fixedLength(bytes));
            }
        }, false);

        doTestKeyLookup(keys);
    }

    @Test
    public void testSeekToPointIdOutOfRange() throws Exception
    {
        writeKeys(writer -> {
            for (int x = 0; x < 4000; x++)
            {
                ByteBuffer buffer = Int32Type.instance.decompose(x);
                ByteSource byteSource = Int32Type.instance.asComparableBytes(buffer, ByteComparable.Version.OSS50);
                byte[] bytes = ByteSourceInverse.readBytes(byteSource);

                writer.add(ByteComparable.fixedLength(bytes));
            }
        }, false);

        withKeyLookupCursor(cursor -> {
            assertThatThrownBy(() -> cursor.seekToPointId(-2)).isInstanceOf(IndexOutOfBoundsException.class)
                                                              .hasMessage(String.format(KeyLookup.INDEX_OUT_OF_BOUNDS, -2, 4000));
            assertThatThrownBy(() -> cursor.seekToPointId(Long.MAX_VALUE)).isInstanceOf(IndexOutOfBoundsException.class)
                                                                          .hasMessage(String.format(KeyLookup.INDEX_OUT_OF_BOUNDS, Long.MAX_VALUE, 4000));
            assertThatThrownBy(() -> cursor.seekToPointId(4000)).isInstanceOf(IndexOutOfBoundsException.class)
                                                                .hasMessage(String.format(KeyLookup.INDEX_OUT_OF_BOUNDS, 4000, 4000));
        });
    }

    @Test
    public void testSeekToKey() throws Exception
    {
        Map<Long, byte[]> keys = new HashMap<>();

        writeKeys(writer -> {
            long pointId = 0;
            for (int x = 0; x < 4000; x += 4)
            {
                byte[] key = makeKey(x);
                keys.put(pointId++, key);

                writer.add(ByteComparable.fixedLength(key));
            }
        }, true);

        withKeyLookupCursor(cursor -> {
            assertEquals(0L, cursor.clusteredSeekToKey(ByteComparable.fixedLength(keys.get(0L)), 0L, 10L));
            cursor.reset();
            assertEquals(160L, cursor.clusteredSeekToKey(ByteComparable.fixedLength(keys.get(160L)), 160L, 170L));
            cursor.reset();
            assertEquals(165L, cursor.clusteredSeekToKey(ByteComparable.fixedLength(keys.get(165L)), 160L, 170L));
            cursor.reset();
            assertEquals(175L, cursor.clusteredSeekToKey(ByteComparable.fixedLength(keys.get(175L)), 160L, 176L));
            cursor.reset();
            assertEquals(176L, cursor.clusteredSeekToKey(ByteComparable.fixedLength(keys.get(176L)), 160L, 177L));
            cursor.reset();
            assertEquals(176L, cursor.clusteredSeekToKey(ByteComparable.fixedLength(keys.get(176L)), 175L, 177L));
            cursor.reset();
            assertEquals(176L, cursor.clusteredSeekToKey(ByteComparable.fixedLength(makeKey(701)), 160L, 177L));
            cursor.reset();
            assertEquals(504L, cursor.clusteredSeekToKey(ByteComparable.fixedLength(keys.get(504L)), 200L, 600L));
            cursor.reset();
            assertEquals(-1L, cursor.clusteredSeekToKey(ByteComparable.fixedLength(makeKey(4000)), 0L, 1000L));
            cursor.reset();
            assertEquals(-1L, cursor.clusteredSeekToKey(ByteComparable.fixedLength(makeKey(4000)), 999L, 1000L));
            cursor.reset();
            assertEquals(999L, cursor.clusteredSeekToKey(ByteComparable.fixedLength(keys.get(999L)), 0L, 1000L));
        });
    }

    @Test
    public void seekToKeyOnNonPartitionedTest() throws Throwable
    {
        Map<Long, byte[]> keys = new HashMap<>();

        writeKeys(writer -> {
            long pointId = 0;
            for (int x = 0; x < 16; x += 4)
            {
                byte[] key = makeKey(x);
                keys.put(pointId++, key);

                writer.add(ByteComparable.fixedLength(key));
            }
        }, false);

        withKeyLookupCursor(cursor -> assertThatThrownBy(() -> cursor.clusteredSeekToKey(ByteComparable.fixedLength(keys.get(0L)), 0L, 10L))
                                        .isInstanceOf(AssertionError.class));
    }

    @Test
    public void partitionedKeysMustBeInOrderInPartitions() throws Throwable
    {
        writeKeys(writer -> {
            writer.startPartition();
            writer.add(ByteComparable.fixedLength(makeKey(0)));
            writer.add(ByteComparable.fixedLength(makeKey(10)));
            assertThatThrownBy(() -> writer.add(ByteComparable.fixedLength(makeKey(9)))).isInstanceOf(IllegalArgumentException.class);
            writer.startPartition();
            writer.add(ByteComparable.fixedLength(makeKey(9)));
        }, true);
    }

    private byte[] makeKey(int value)
    {
        ByteBuffer buffer = Int32Type.instance.decompose(value);
        ByteSource byteSource = Int32Type.instance.asComparableBytes(buffer, ByteComparable.Version.OSS50);
        return ByteSourceInverse.readBytes(byteSource);
    }

    private void doTestKeyLookup(List<byte[]> keys) throws Exception
    {
        // iterate ascending
        withKeyLookupCursor(cursor -> {
            for (int x = 0; x < keys.size(); x++)
                assertArrayEquals(keys.get(x), ByteSourceInverse.readBytes(cursor.seekToPointId(x)));
        });

        // iterate ascending skipping blocks
        withKeyLookupCursor(cursor -> {
            for (int x = 0; x < keys.size(); x += 17)
                assertArrayEquals(keys.get(x), ByteSourceInverse.readBytes(cursor.seekToPointId(x)));
        });

        withKeyLookupCursor(cursor -> {
            assertArrayEquals(keys.get(7), ByteSourceInverse.readBytes(cursor.seekToPointId(7)));
            assertArrayEquals(keys.get(7), ByteSourceInverse.readBytes(cursor.seekToPointId(7)));
        });
    }

    protected void writeKeys(ThrowingConsumer<KeyStoreWriter> testCode, boolean clustering) throws IOException
    {
        try (MetadataWriter metadataWriter = new MetadataWriter(indexDescriptor.openPerSSTableOutput(IndexComponent.GROUP_META)))
        {
            IndexOutputWriter bytesWriter = indexDescriptor.openPerSSTableOutput(IndexComponent.PARTITION_KEY_BLOCKS);
            NumericValuesWriter blockFPWriter = new NumericValuesWriter(indexDescriptor, IndexComponent.PARTITION_KEY_BLOCK_OFFSETS, metadataWriter, true);
            try (KeyStoreWriter writer = new KeyStoreWriter(indexDescriptor.componentName(IndexComponent.PARTITION_KEY_BLOCKS),
                                                            metadataWriter,
                                                            bytesWriter,
                                                            blockFPWriter,
                                                            4,
                                                            clustering))
            {
                testCode.accept(writer);
            }
        }
    }

    @FunctionalInterface
    public interface ThrowingConsumer<T>
    {
        void accept(T t) throws IOException;
    }

    private void withKeyLookup(ThrowingConsumer<KeyLookup> testCode) throws IOException
    {
        MetadataSource metadataSource = MetadataSource.loadGroupMetadata(indexDescriptor);
        NumericValuesMeta blockPointersMeta = new NumericValuesMeta(metadataSource.get(indexDescriptor.componentName(IndexComponent.PARTITION_KEY_BLOCK_OFFSETS)));
        KeyLookupMeta keyLookupMeta = new KeyLookupMeta(metadataSource.get(indexDescriptor.componentName(IndexComponent.PARTITION_KEY_BLOCKS)));
        try (FileHandle keysData = indexDescriptor.createPerSSTableFileHandle(IndexComponent.PARTITION_KEY_BLOCKS, null);
             FileHandle blockOffsets = indexDescriptor.createPerSSTableFileHandle(IndexComponent.PARTITION_KEY_BLOCK_OFFSETS, null))
        {
            KeyLookup reader = new KeyLookup(keysData, blockOffsets, keyLookupMeta, blockPointersMeta);
            testCode.accept(reader);
        }
    }

    private void withKeyLookupCursor(ThrowingConsumer<KeyLookup.Cursor> testCode) throws IOException
    {
        withKeyLookup(reader -> {
            try (KeyLookup.Cursor cursor = reader.openCursor())
            {
                testCode.accept(cursor);
            }
        });
    }

    private boolean validateComponent(IndexComponent indexComponent, boolean checksum)
    {
        try (IndexInput input = indexDescriptor.openPerSSTableInput(indexComponent))
        {
            if (checksum)
                SAICodecUtils.validateChecksum(input);
            else
                SAICodecUtils.validate(input);
            return true;
        }
        catch (Throwable ignore)
        {
            return false;
        }
    }
}
