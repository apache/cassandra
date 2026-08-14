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

package org.apache.cassandra.index.sai.disk.v9.keystore;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.disk.format.IndexComponents;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.MetadataSource;
import org.apache.cassandra.index.sai.disk.v1.MetadataWriter;
import org.apache.cassandra.index.sai.disk.v1.bitpack.NumericValuesMeta;
import org.apache.cassandra.index.sai.disk.v1.bitpack.NumericValuesWriter;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.SAICodecUtils;
import org.apache.cassandra.index.sai.utils.SaiRandomizedTest;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;
import org.apache.lucene.store.IndexInput;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class KeyLookupTest extends SaiRandomizedTest
{
    public static final ByteComparable.Version VERSION = TypeUtil.BYTE_COMPARABLE_VERSION;
    private static final int BLOCK_SIZE = 4;
    protected IndexDescriptor indexDescriptor;

    @Before
    public void setup() throws Exception
    {
        indexDescriptor = newIndexDescriptor();
    }

    @Test
    public void testLexicographicException() throws Exception
    {
        IndexComponents.ForWrite components = indexDescriptor.newPerSSTableComponentsForWrite();
        try (MetadataWriter metadataWriter = new MetadataWriter(components))
        {
            NumericValuesWriter blockFPWriter = new NumericValuesWriter(components.addOrGet(IndexComponentType.PARTITION_KEY_BLOCK_OFFSETS),
                                                                        metadataWriter, true);
            try (KeyStoreWriter writer = new KeyStoreWriter(components.addOrGet(IndexComponentType.PARTITION_KEY_BLOCKS),
                                                            metadataWriter,
                                                            blockFPWriter,
                                                            4,
                                                            true))
            {
                // Start the first partition
                writer.startPartition();
                ByteBuffer buffer = Int32Type.instance.decompose(99999);
                ByteSource byteSource = Int32Type.instance.asComparableBytes(buffer, VERSION);
                byte[] bytes1 = ByteSourceInverse.readBytes(byteSource);

                writer.add(ByteComparable.preencoded(VERSION, bytes1));

                buffer = Int32Type.instance.decompose(444);
                byteSource = Int32Type.instance.asComparableBytes(buffer, VERSION);
                byte[] bytes2 = ByteSourceInverse.readBytes(byteSource);

                // Within the same partition, keys must be in ascending lexicographic order
                assertThrows(IllegalArgumentException.class, () -> writer.add(ByteComparable.preencoded(VERSION, bytes2)));

                // Start a new partition - now we can add a smaller key because it's a different partition
                writer.startPartition();
                writer.add(ByteComparable.preencoded(VERSION, bytes2));
            }
        }
    }

    @Test
    public void testFileValidation() throws Exception
    {
        List<PrimaryKey> primaryKeys = new ArrayList<>();

        for (int x = 0; x < 11; x++)
        {
            ByteBuffer buffer = UTF8Type.instance.decompose(Integer.toString(x));
            DecoratedKey partitionKey = Murmur3Partitioner.instance.decorateKey(buffer);
            PrimaryKey primaryKey = SAITester.TEST_FACTORY.create(partitionKey, Clustering.EMPTY);
            primaryKeys.add(primaryKey);
        }

        primaryKeys.sort(PrimaryKey::compareTo);
        IndexComponents.ForWrite components = indexDescriptor.newPerSSTableComponentsForWrite();

        try (MetadataWriter metadataWriter = new MetadataWriter(components))
        {
            NumericValuesWriter blockFPWriter = new NumericValuesWriter(components.addOrGet(IndexComponentType.PARTITION_KEY_BLOCK_OFFSETS),
                                                                        metadataWriter, true);
            try (KeyStoreWriter writer = new KeyStoreWriter(components.addOrGet(IndexComponentType.PARTITION_KEY_BLOCKS),
                                                            metadataWriter,
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
        assertTrue(validateComponent(components, IndexComponentType.PARTITION_KEY_BLOCKS, true));
        assertTrue(validateComponent(components, IndexComponentType.PARTITION_KEY_BLOCKS, false));
        assertTrue(validateComponent(components, IndexComponentType.PARTITION_KEY_BLOCK_OFFSETS, true));
        assertTrue(validateComponent(components, IndexComponentType.PARTITION_KEY_BLOCK_OFFSETS, false));
    }

    @Test
    public void testSeekToTerm() throws Exception
    {
        List<byte[]> keys = new ArrayList<>();
        writeTerms(keys);

        // iterate on keys ascending
        withKeyLookup(reader ->
                      {
                          for (int x = 0; x < keys.size(); x++)
                          {
                              try (KeyLookup.Cursor cursor = reader.openCursor())
                              {
                                  ByteComparable key = cursor.seekToPointId(x);

                                  byte[] bytes = ByteSourceInverse.readBytes(key.asComparableBytes(VERSION));

                                  assertArrayEquals(keys.get(x), bytes);
                              }
                          }
                      });

        // iterate on keys descending
        withKeyLookup(reader ->
                      {
                          for (int x = keys.size() - 1; x >= 0; x--)
                          {
                              try (KeyLookup.Cursor cursor = reader.openCursor())
                              {
                                  ByteComparable key = cursor.seekToPointId(x);

                                  byte[] bytes = ByteSourceInverse.readBytes(key.asComparableBytes(VERSION));

                                  assertArrayEquals(keys.get(x), bytes);
                              }
                          }
                      });

        // iterate randomly
        withKeyLookup(reader ->
                      {
                          for (int x = 0; x < keys.size(); x++)
                          {
                              int target = nextInt(0, keys.size());

                              try (KeyLookup.Cursor cursor = reader.openCursor())
                              {
                                  ByteComparable key = cursor.seekToPointId(target);

                                  byte[] bytes = ByteSourceInverse.readBytes(key.asComparableBytes(VERSION));

                                  assertArrayEquals(keys.get(target), bytes);
                              }
                          }
                      });
    }

    @Test
    public void testLongPrefixesAndSuffixes() throws Exception
    {
        List<byte[]> keys = new ArrayList<>();
        writeKeys(writer -> {
            // The following writes a set of keys that cover the following conditions:

            // Start value 0
            byte[] bytes = new byte[20];
            keys.add(bytes);
            writer.add(ByteComparable.preencoded(VERSION, bytes));
            // prefix > 15
            bytes = new byte[20];
            Arrays.fill(bytes, 16, 20, (byte) 1);
            keys.add(bytes);
            writer.add(ByteComparable.preencoded(VERSION, bytes));
            // prefix == 15
            bytes = new byte[20];
            Arrays.fill(bytes, 15, 20, (byte) 1);
            keys.add(bytes);
            writer.add(ByteComparable.preencoded(VERSION, bytes));
            // prefix < 15
            bytes = new byte[20];
            Arrays.fill(bytes, 14, 20, (byte) 1);
            keys.add(bytes);
            writer.add(ByteComparable.preencoded(VERSION, bytes));
            // suffix > 16
            bytes = new byte[20];
            Arrays.fill(bytes, 0, 4, (byte) 1);
            keys.add(bytes);
            writer.add(ByteComparable.preencoded(VERSION, bytes));
            // suffix == 16
            bytes = new byte[20];
            Arrays.fill(bytes, 0, 5, (byte) 1);
            keys.add(bytes);
            writer.add(ByteComparable.preencoded(VERSION, bytes));
            // suffix < 16
            bytes = new byte[20];
            Arrays.fill(bytes, 0, 6, (byte) 1);
            keys.add(bytes);
            writer.add(ByteComparable.preencoded(VERSION, bytes));

            bytes = new byte[32];
            Arrays.fill(bytes, 0, 16, (byte) 1);
            keys.add(bytes);
            writer.add(ByteComparable.preencoded(VERSION, bytes));
            // prefix >= 15 && suffix >= 16
            bytes = new byte[32];
            Arrays.fill(bytes, 0, 32, (byte) 1);
            keys.add(bytes);
            writer.add(ByteComparable.preencoded(VERSION, bytes));
        }, false);

        doTestKeyLookup(keys);
    }

    @Test
    public void testNonUniqueKeys() throws Exception
    {
        List<byte[]> keys = new ArrayList<>();

        writeKeys(writer -> {
            for (int x = 0; x < 4000; x++)
            {
                byte[] bytes = ByteSourceInverse.readBytes(intByteSource(5000));
                keys.add(bytes);
                writer.add(ByteComparable.preencoded(VERSION, bytes));
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
                byte[] bytes = ByteSourceInverse.readBytes(intByteSource(x));
                keys.add(bytes);
                writer.add(ByteComparable.preencoded(VERSION, bytes));
            }
        }, false);

        doTestKeyLookup(keys);
    }

    @Test
    public void testSeekToPointIdCC() throws Exception
    {
        List<byte[]> terms = new ArrayList<>();
        writeTerms(terms);

        // iterate ascending
        withKeyLookupCursor(cursor ->
                            {
                                for (int x = 0; x < terms.size(); x++)
                                {
                                    ByteComparable term = cursor.seekToPointId(x);

                                    byte[] bytes = ByteSourceInverse.readBytes(term.asComparableBytes(VERSION));
                                    assertArrayEquals(terms.get(x), bytes);
                                }
                            });

        // iterate descending
        withKeyLookupCursor(cursor ->
                            {
                                for (int x = terms.size() - 1; x >= 0; x--)
                                {
                                    ByteComparable term = cursor.seekToPointId(x);

                                    byte[] bytes = ByteSourceInverse.readBytes(term.asComparableBytes(VERSION));
                                    assertArrayEquals(terms.get(x), bytes);
                                }
                            });

        // iterate randomly
        withKeyLookupCursor(cursor ->
                            {
                                for (int x = 0; x < terms.size(); x++)
                                {
                                    int target = nextInt(0, terms.size());
                                    ByteComparable term = cursor.seekToPointId(target);

                                    byte[] bytes = ByteSourceInverse.readBytes(term.asComparableBytes(VERSION));
                                    assertArrayEquals(terms.get(target), bytes);
                                }
                            });
    }

    @Test
    public void testSeekToPointIdOutOfRange() throws Exception
    {
        writeKeys(writer -> {
            for (int x = 0; x < 4000; x++)
            {
                byte[] bytes = ByteSourceInverse.readBytes(intByteSource(x));
                writer.add(ByteComparable.preencoded(VERSION, bytes));
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

                writer.add(ByteComparable.preencoded(VERSION, key));
            }
        }, true);

        withKeyLookupCursor(cursor -> {
            assertEquals(0L, cursor.clusteredSeekToKey(ByteComparable.preencoded(VERSION, keys.get(0L)), 0L, 10L));
            assertEquals(160L, cursor.clusteredSeekToKey(ByteComparable.preencoded(VERSION, keys.get(160L)), 160L, 170L));
            assertEquals(165L, cursor.clusteredSeekToKey(ByteComparable.preencoded(VERSION, keys.get(165L)), 160L, 170L));
            assertEquals(175L, cursor.clusteredSeekToKey(ByteComparable.preencoded(VERSION, keys.get(175L)), 160L, 176L));
            assertEquals(176L, cursor.clusteredSeekToKey(ByteComparable.preencoded(VERSION, keys.get(176L)), 160L, 177L));
            assertEquals(176L, cursor.clusteredSeekToKey(ByteComparable.preencoded(VERSION, keys.get(176L)), 175L, 177L));
            assertEquals(176L, cursor.clusteredSeekToKey(ByteComparable.preencoded(VERSION, makeKey(701)), 160L, 177L));
            assertEquals(504L, cursor.clusteredSeekToKey(ByteComparable.preencoded(VERSION, keys.get(504L)), 200L, 600L));
            assertEquals(-1L, cursor.clusteredSeekToKey(ByteComparable.preencoded(VERSION, makeKey(4000)), 0L, 1000L));
            assertEquals(-1L, cursor.clusteredSeekToKey(ByteComparable.preencoded(VERSION, makeKey(4000)), 999L, 1000L));
            assertEquals(999L, cursor.clusteredSeekToKey(ByteComparable.preencoded(VERSION, keys.get(999L)), 0L, 1000L));
        });
    }

    @Test
    public void testSeekToKeyOnNonPartitioned() throws Throwable
    {
        Map<Long, byte[]> keys = new HashMap<>();

        writeKeys(writer -> {
            long pointId = 0;
            for (int x = 0; x < 16; x += 4)
            {
                byte[] key = makeKey(x);
                keys.put(pointId++, key);

                writer.add(ByteComparable.preencoded(VERSION, key));
            }
        }, false);

        withKeyLookupCursor(cursor -> assertThatThrownBy(() -> cursor.clusteredSeekToKey(ByteComparable.preencoded(VERSION, keys.get(0L)),
                                                                                         0L, 10L))
                                      .isInstanceOf(AssertionError.class));
    }

    @Test
    public void partitionedKeysMustBeInOrderInPartitions() throws Throwable
    {
        writeKeys(writer -> {
            writer.startPartition();
            writer.add(ByteComparable.preencoded(VERSION, makeKey(0)));
            writer.add(ByteComparable.preencoded(VERSION, makeKey(10)));
            assertThatThrownBy(() -> writer.add(ByteComparable.preencoded(VERSION, makeKey(9)))).isInstanceOf(IllegalArgumentException.class);
            writer.startPartition();
            writer.add(ByteComparable.preencoded(VERSION, makeKey(9)));
        }, true);
    }

    @Test
    public void testEmptyCursor() throws Exception
    {
        // Write an empty key store (keyCount = 0)
        writeKeys(writer -> {
        }, false);

        withKeyLookupCursor(cursor -> {
            assertEquals(-1L, cursor.clusteredSeekToKey(ByteComparable.preencoded(VERSION, makeKey(0)), 0L, 10L));

            assertThatThrownBy(() -> cursor.seekToPointId(0))
            .isInstanceOf(IndexOutOfBoundsException.class)
            .hasMessage(String.format(KeyLookup.INDEX_OUT_OF_BOUNDS, 0, 0));
            assertThatThrownBy(() -> cursor.seekToPointId(-1))
            .isInstanceOf(IndexOutOfBoundsException.class)
            .hasMessage(String.format(KeyLookup.INDEX_OUT_OF_BOUNDS, -1, 0));

            // Test close should not throw
            cursor.close();
        });
    }

    @Test
    public void testSeekToSamePointId() throws Exception
    {
        List<byte[]> keys = new ArrayList<>();
        writeKeys(writer -> {
            for (int x = 0; x < 100; x++)
            {
                byte[] bytes = ByteSourceInverse.readBytes(intByteSource(x));
                keys.add(bytes);
                writer.add(ByteComparable.preencoded(VERSION, bytes));
            }
        }, false);

        withKeyLookupCursor(cursor -> {
            // Seek to point 50
            ByteComparable key1 = cursor.seekToPointId(50);
            byte[] bytes1 = ByteSourceInverse.readBytes(key1.asComparableBytes(VERSION));
            assertArrayEquals(keys.get(50), bytes1);

            // Seek to the same point again (target == currentPointId)
            ByteComparable key2 = cursor.seekToPointId(50);
            byte[] bytes2 = ByteSourceInverse.readBytes(key2.asComparableBytes(VERSION));
            assertArrayEquals(keys.get(50), bytes2);
        });
    }

    @Test
    public void testSeekBackwardsInSameBlock() throws Exception
    {
        List<byte[]> keys = new ArrayList<>();
        writeKeys(writer -> {
            for (int x = 0; x < 20; x++)
            {
                byte[] bytes = ByteSourceInverse.readBytes(intByteSource(x));
                keys.add(bytes);
                writer.add(ByteComparable.preencoded(VERSION, bytes));
            }
        }, false);

        withKeyLookupCursor(cursor -> {
            int pointInBlock1 = BLOCK_SIZE + 2;
            ByteComparable key1 = cursor.seekToPointId(pointInBlock1);
            byte[] bytes1 = ByteSourceInverse.readBytes(key1.asComparableBytes(VERSION));
            assertArrayEquals(keys.get(pointInBlock1), bytes1);

            int earlierPointInBlock1 = BLOCK_SIZE + 1;
            ByteComparable key2 = cursor.seekToPointId(earlierPointInBlock1);
            byte[] bytes2 = ByteSourceInverse.readBytes(key2.asComparableBytes(VERSION));
            assertArrayEquals(keys.get(earlierPointInBlock1), bytes2);
        });
    }

    @Test
    public void testSeekForwardWithinBlock() throws Exception
    {
        List<byte[]> keys = new ArrayList<>();
        writeKeys(writer -> {
            for (int x = 0; x < 20; x++)
            {
                byte[] bytes = ByteSourceInverse.readBytes(intByteSource(x));
                keys.add(bytes);
                writer.add(ByteComparable.preencoded(VERSION, bytes));
            }
        }, false);

        withKeyLookupCursor(cursor -> {
            // Seek to start of block 1
            int blockStart = BLOCK_SIZE;
            cursor.seekToPointId(blockStart);

            // Seek forward within the same block (no block reset needed)
            int pointInSameBlock = blockStart + 2;
            ByteComparable key = cursor.seekToPointId(pointInSameBlock);
            byte[] bytes = ByteSourceInverse.readBytes(key.asComparableBytes(VERSION));
            assertArrayEquals(keys.get(pointInSameBlock), bytes);
        });
    }

    @Test
    public void testClusteredSeekWithMatchAtCurrentPosition() throws Exception
    {
        Map<Long, byte[]> keys = new HashMap<>();
        writeKeys(writer -> {
            writer.startPartition();
            for (long pointId = 0; pointId < 10; pointId++)
            {
                byte[] key = makeKey((int) pointId * 4);
                keys.put(pointId, key);
                writer.add(ByteComparable.preencoded(VERSION, key));
            }

            writer.startPartition();
            for (long pointId = 10; pointId < 20; pointId++)
            {
                byte[] key = makeKey((int) pointId * 4);
                keys.put(pointId, key);
                writer.add(ByteComparable.preencoded(VERSION, key));
            }

            writer.startPartition();
            for (long pointId = 20; pointId < 30; pointId++)
            {
                byte[] key = makeKey((int) pointId * 4);
                keys.put(pointId, key);
                writer.add(ByteComparable.preencoded(VERSION, key));
            }
        }, true);

        withKeyLookupCursor(cursor -> {
            // Position cursor at the start of partition 2 (point id 10)
            cursor.seekToPointId(10);

            // Now do a clustered seek within partition 2 for the key at position 10
            // Since cursor is already at the correct position and the key matches,
            // this should hit the early return.
            long result = cursor.clusteredSeekToKey(ByteComparable.preencoded(VERSION, keys.get(10L)), 10L, 20L);
            assertEquals(10L, result);
        });
    }

    @Test
    public void testClusteredSeekAtEndOfKeyCount() throws Exception
    {
        int partitionSize = 25;
        writeKeys(writer -> {
            writer.startPartition();
            for (long pointId = 0; pointId < partitionSize; pointId++)
            {
                byte[] key = makeKey((int) pointId * 4);
                writer.add(ByteComparable.preencoded(VERSION, key));
            }
        }, true);

        withKeyLookupCursor(cursor -> {
            // Search for a key that's beyond all keys in the partition, with endingPointId == keyCount
            long result = cursor.clusteredSeekToKey(ByteComparable.preencoded(VERSION, makeKey(10000)), 0L, partitionSize);
            assertEquals(-1L, result);
        });
    }

    @Test
    public void testClusteredSeekInLastBlock() throws Exception
    {
        Map<Long, byte[]> keys = new HashMap<>();
        int numBlocks = 3;
        int totalKeys = numBlocks * BLOCK_SIZE;
        writeKeys(writer -> {
            // Create a single partition with exactly totalKeys clustering keys (numBlocks blocks)
            // This ensures all point ids are in the same partition
            writer.startPartition();
            for (long pointId = 0; pointId < totalKeys; pointId++)
            {
                byte[] key = makeKey((int) pointId * 4);
                keys.put(pointId, key);
                writer.add(ByteComparable.preencoded(VERSION, key));
            }
        }, true);

        withKeyLookupCursor(cursor -> {
            // Search in the last block within the partition
            // This tests the last block logic in moveToBlockAndCompareTo (lines 335-336)
            long lastBlockStart = (numBlocks - 1) * BLOCK_SIZE;
            long result = cursor.clusteredSeekToKey(ByteComparable.preencoded(VERSION, keys.get(lastBlockStart)),
                                                    lastBlockStart, totalKeys);
            assertEquals(lastBlockStart, result);
        });
    }

    @Test
    public void testCursorReset() throws Exception
    {
        List<byte[]> keys = new ArrayList<>();
        writeKeys(writer -> {
            for (int x = 0; x < 50; x++)
            {
                byte[] bytes = ByteSourceInverse.readBytes(intByteSource(x));
                keys.add(bytes);
                writer.add(ByteComparable.preencoded(VERSION, bytes));
            }
        }, false);

        withKeyLookupCursor(cursor -> {
            cursor.seekToPointId(25);

            ByteComparable key = cursor.seekToPointId(0);
            byte[] bytes = ByteSourceInverse.readBytes(key.asComparableBytes(VERSION));
            assertArrayEquals(keys.get(0), bytes);
        });
    }

    @Test
    public void testCursorResetFromSecondPartition() throws Exception
    {
        writeKeys(writer -> {
            writer.startPartition();
            for (int pointId = 0; pointId < 8; pointId++)
            {
                writer.add(ByteComparable.preencoded(VERSION, ByteSourceInverse.readBytes(intByteSource(pointId))));
            }
            writer.startPartition();
            for (int pointId = 8; pointId < 16; pointId++)
            {
                writer.add(ByteComparable.preencoded(VERSION, ByteSourceInverse.readBytes(intByteSource(pointId))));
            }
        }, true);

        withKeyLookupCursor(cursor -> {
            ByteComparable key = cursor.seekToPointId(10);
            byte[] bytes = ByteSourceInverse.readBytes(key.asComparableBytes(TypeUtil.BYTE_COMPARABLE_VERSION));
            assertArrayEquals(ByteSourceInverse.readBytes(intByteSource(10)), bytes);

            key = cursor.seekToPointId(0);
            bytes = ByteSourceInverse.readBytes(key.asComparableBytes(TypeUtil.BYTE_COMPARABLE_VERSION));
            assertArrayEquals(ByteSourceInverse.readBytes(intByteSource(0)), bytes);

            key = cursor.seekToPointId(12);
            bytes = ByteSourceInverse.readBytes(key.asComparableBytes(TypeUtil.BYTE_COMPARABLE_VERSION));
            assertArrayEquals(ByteSourceInverse.readBytes(intByteSource(12)), bytes);
        });
    }

    @Test
    public void testMultipleCursorInstances() throws Exception
    {
        List<byte[]> keys = new ArrayList<>();
        writeKeys(writer -> {
            for (int x = 0; x < 50; x++)
            {
                byte[] bytes = ByteSourceInverse.readBytes(intByteSource(x));
                keys.add(bytes);
                writer.add(ByteComparable.preencoded(VERSION, bytes));
            }
        }, false);

        withKeyLookup(reader -> {
            // Open multiple cursors and verify they work independently
            try (KeyLookup.Cursor cursor1 = reader.openCursor();
                 KeyLookup.Cursor cursor2 = reader.openCursor())
            {
                ByteComparable key1 = cursor1.seekToPointId(10);
                ByteComparable key2 = cursor2.seekToPointId(20);

                byte[] bytes1 = ByteSourceInverse.readBytes(key1.asComparableBytes(VERSION));
                byte[] bytes2 = ByteSourceInverse.readBytes(key2.asComparableBytes(VERSION));

                assertArrayEquals(keys.get(10), bytes1);
                assertArrayEquals(keys.get(20), bytes2);
            }
        });
    }

    @Test
    public void testClusteredSeekBinarySearchPath() throws Exception
    {
        Map<Long, byte[]> keys = new HashMap<>();
        writeKeys(writer -> {
            // Create enough keys to trigger binary search (multiple blocks)
            for (int x = 0; x < 200; x += 2)
            {
                byte[] key = makeKey(x);
                keys.put((long) x / 2, key);
                writer.add(ByteComparable.preencoded(VERSION, key));
            }
        }, true);

        withKeyLookupCursor(cursor -> {
            // Search for a key in the middle, forcing binary search
            // pointId 50 corresponds to value 100 (since we increment by 2)
            long result = cursor.clusteredSeekToKey(ByteComparable.preencoded(VERSION, keys.get(50L)), 0L, 100L);
            assertEquals(50L, result);

            // Search for a key that doesn't exist but falls between existing keys
            // Key value 51 doesn't exist, next highest is 52 at position 26
            long result2 = cursor.clusteredSeekToKey(ByteComparable.preencoded(VERSION, makeKey(51)), 0L, 100L);
            assertEquals(26L, result2);
        });
    }

    private byte[] makeKey(int value)
    {
        return ByteSourceInverse.readBytes(intByteSource(value));
    }

    private void doTestKeyLookup(List<byte[]> keys) throws Exception
    {
        // iterate ascending
        withKeyLookupCursor(cursor -> {
            for (int x = 0; x < keys.size(); x++)
            {
                ByteComparable key = cursor.seekToPointId(x);

                byte[] bytes = ByteSourceInverse.readBytes(key.asComparableBytes(TypeUtil.BYTE_COMPARABLE_VERSION));

                assertArrayEquals(keys.get(x), bytes);
            }
        });

        // iterate ascending skipping blocks
        withKeyLookupCursor(cursor -> {
            for (int x = 0; x < keys.size(); x += 17)
            {
                ByteComparable key = cursor.seekToPointId(x);

                byte[] bytes = ByteSourceInverse.readBytes(key.asComparableBytes(TypeUtil.BYTE_COMPARABLE_VERSION));

                assertArrayEquals(keys.get(x), bytes);
            }
        });

        withKeyLookupCursor(cursor -> {
            ByteComparable key = cursor.seekToPointId(7);
            byte[] bytes = ByteSourceInverse.readBytes(key.asComparableBytes(TypeUtil.BYTE_COMPARABLE_VERSION));
            assertArrayEquals(keys.get(7), bytes);

            key = cursor.seekToPointId(7);
            bytes = ByteSourceInverse.readBytes(key.asComparableBytes(TypeUtil.BYTE_COMPARABLE_VERSION));
            assertArrayEquals(keys.get(7), bytes);
        });
    }

    private void writeTerms(List<byte[]> terms) throws Exception
    {
        writeKeys(writer -> {

            for (int x = 0; x < 1000 * 4; x++)
            {
                byte[] bytes = ByteSourceInverse.readBytes(intByteSource(x));
                terms.add(bytes);

                writer.add(ByteComparable.preencoded(VERSION, bytes));
            }
        }, false);
    }

    @Test
    public void testClusteredSeekKeyBeforePartition() throws Exception
    {
        writeKeys(writer -> {
            writer.startPartition();
            for (int x = 10; x < 26; x += 2)
            {
                writer.add(ByteComparable.preencoded(VERSION, ByteSourceInverse.readBytes(intByteSource(x))));
            }
        }, true);

        withKeyLookupCursor(cursor -> {
            long result = cursor.clusteredSeekToKey(ByteComparable.preencoded(VERSION, makeKey(5)), 0L, 8L);
            assertEquals(0L, result);

            result = cursor.clusteredSeekToKey(ByteComparable.preencoded(VERSION, makeKey(11)), 0L, 8L);
            assertEquals(1L, result);
        });
    }

    @Test
    public void testClusteredSeekKeyOutsidePartition() throws Exception
    {
        writeKeys(writer -> {
            writer.startPartition();
            for (int pointId = 0; pointId < 8; pointId++)
            {
                writer.add(ByteComparable.preencoded(VERSION, ByteSourceInverse.readBytes(intByteSource(pointId * 2))));
            }
            writer.startPartition();
            for (int pointId = 8; pointId < 16; pointId++)
            {
                writer.add(ByteComparable.preencoded(VERSION, ByteSourceInverse.readBytes(intByteSource(pointId * 2))));
            }
            writer.startPartition();
            for (int pointId = 16; pointId < 24; pointId++)
            {
                writer.add(ByteComparable.preencoded(VERSION, ByteSourceInverse.readBytes(intByteSource(pointId * 2))));
            }
        }, true);

        withKeyLookupCursor(cursor -> {
            // Search for a key (value 4) that exists in partition 0, but we're searching in partition 1 (pointIds 8-15)
            // Should return the startingPointId (8) since the search key is before all keys in the search range
            long result = cursor.clusteredSeekToKey(ByteComparable.preencoded(VERSION, makeKey(4)), 8L, 16L);
            assertEquals(8L, result);

            // Search for a key (value 10) from partition 0, searching in partition 1
            // Should also return startingPointId (8)
            result = cursor.clusteredSeekToKey(ByteComparable.preencoded(VERSION, makeKey(10)), 8L, 16L);
            assertEquals(8L, result);

            // Search for a key from partition 1 while searching in partition 0
            // Should return startingPointId (8) of next partition, i.e., partition 1
            result = cursor.clusteredSeekToKey(ByteComparable.preencoded(VERSION, makeKey(20)), 0L, 8L);
            assertEquals(8L, result);

            // Search for a key from partition 2 while searching in partition 0
            // Should return startingPointId (8) of next partition, i.e., partition 1
            result = cursor.clusteredSeekToKey(ByteComparable.preencoded(VERSION, makeKey(34)), 0L, 8L);
            assertEquals(8L, result);
        });
    }

    private ByteSource intByteSource(int value)
    {
        ByteBuffer buffer = Int32Type.instance.decompose(value);
        return Int32Type.instance.asComparableBytes(buffer, VERSION);
    }

    protected void writeKeys(ThrowingConsumer<KeyStoreWriter> testCode, boolean clustering) throws Exception
    {
        IndexComponents.ForWrite components = indexDescriptor.newPerSSTableComponentsForWrite();
        try (MetadataWriter metadataWriter = new MetadataWriter(components))
        {
            NumericValuesWriter blockFPWriter = new NumericValuesWriter(components.addOrGet(IndexComponentType.PARTITION_KEY_BLOCK_OFFSETS),
                                                                        metadataWriter, true);
            try (KeyStoreWriter writer = new KeyStoreWriter(components.addOrGet(IndexComponentType.PARTITION_KEY_BLOCKS),
                                                            metadataWriter,
                                                            blockFPWriter,
                                                            BLOCK_SIZE,
                                                            clustering))
            {
                testCode.accept(writer);
            }
        }
        components.markComplete();
    }

    private void withKeyLookup(ThrowingConsumer<KeyLookup> testCode) throws Exception
    {
        IndexComponents.ForRead components = indexDescriptor.perSSTableComponents();
        MetadataSource metadataSource = MetadataSource.loadMetadata(components);
        NumericValuesMeta blockPointersMeta = new NumericValuesMeta(metadataSource.get(components.get(IndexComponentType.PARTITION_KEY_BLOCK_OFFSETS)));
        KeyLookupMeta keyLookupMeta = new KeyLookupMeta(metadataSource.get(components.get(IndexComponentType.PARTITION_KEY_BLOCKS)));
        try (FileHandle keysData = components.get(IndexComponentType.PARTITION_KEY_BLOCKS).createFileHandle();
             FileHandle blockOffsets = components.get(IndexComponentType.PARTITION_KEY_BLOCK_OFFSETS).createFileHandle())
        {
            KeyLookup reader = new KeyLookup(keysData, blockOffsets, keyLookupMeta, blockPointersMeta);
            testCode.accept(reader);
        }
    }

    private void withKeyLookupCursor(ThrowingConsumer<KeyLookup.Cursor> testCode) throws Exception
    {
        withKeyLookup(reader -> {
            try (KeyLookup.Cursor cursor = reader.openCursor())
            {
                testCode.accept(cursor);
            }
        });
    }

    private boolean validateComponent(IndexComponents.ForRead components, IndexComponentType indexComponentType, boolean checksum)
    {
        try (IndexInput input = components.get(indexComponentType).openInput())
        {
            if (checksum)
                SAICodecUtils.validateChecksum(input);
            else
                SAICodecUtils.validate(input);
            return true;
        }
        catch (Throwable ignored)
        {
            return false;
        }
    }

    @FunctionalInterface
    public interface ThrowingConsumer<T>
    {
        void accept(T t) throws Exception;
    }
}
