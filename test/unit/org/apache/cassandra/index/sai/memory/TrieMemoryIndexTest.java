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
package org.apache.cassandra.index.sai.memory;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.IntFunction;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.index.sai.ColumnContext;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.utils.PrimaryKeys;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.schema.CachingParams;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static org.junit.Assert.assertEquals;

//TODO Rework this test
public class TrieMemoryIndexTest
{
    private static final String KEYSPACE = "test_keyspace";
    private static final String TABLE = "test_table";
    private static final String PART_KEY_COL = "key";
    private static final String REG_COL = "col";

    private static DecoratedKey key = Murmur3Partitioner.instance.decorateKey(ByteBufferUtil.bytes("key"));

    @Before
    public void setup()
    {
        DatabaseDescriptor.daemonInitialization();
    }

//    @Test
//    public void shouldEncodeAndDecodeStrings()
//    {
//        for (int i = 0; i < 31; ++i)
//        {
//            final UTF8Type type = UTF8Type.instance;
//            String input = String.valueOf(i);
//
//            final TrieMemoryIndex.Encoder encoder = TrieMemoryIndex.encoderFor(type);
//            final ByteComparable encodedAndTerminated = encoder.encode(type.decompose(input));
//            // Note: For string, we unescape the prefix-free encoding, so the comparable contains exactly the bytes of the source.
//            final ByteBuffer decoded = ByteBuffer.wrap(ByteSourceUtil.readBytes(encoder.decodeEntry(new AbstractMap.SimpleEntry<>(encodedAndTerminated, null)).left.asComparableBytes(ByteComparable.Version.DSE68)));
//            final String output = type.compose(decoded);
//
//            assertEquals(input, output);
//        }
//    }
//
//    @Test
//    public void shouldEncodeAndDecodeLongs()
//    {
//        for (int i = -31; i < 31; ++i)
//        {
//            final LongType type = LongType.instance;
//            long input = i;
//
//            final TrieMemoryIndex.Encoder encoder = TrieMemoryIndex.encoderFor(type);
//            final ByteComparable encodedAndTerminated = encoder.encode(type.decompose(input));
//            // Note: For long, we return the prefix-free encoding which must be converted to the type.
//            final ByteBuffer decoded = type.fromComparableBytes(encoder.decodeEntry(new AbstractMap.SimpleEntry<>(encodedAndTerminated, null)).left.asPeekableBytes(ByteComparable.Version.DSE68),
//                                                                ByteComparable.Version.DSE68);
//            final long output = type.compose(decoded);
//
//            assertEquals(input, output);
//        }
//    }

    @Test
    public void shouldAcceptPrefixValues()
    {
        shouldAcceptPrefixValuesForType(UTF8Type.instance, i -> UTF8Type.instance.decompose(String.format("%03d", i)));
        shouldAcceptPrefixValuesForType(Int32Type.instance, Int32Type.instance::decompose);
    }

    private void shouldAcceptPrefixValuesForType(AbstractType<?> type, IntFunction<ByteBuffer> decompose)
    {
        final TrieMemoryIndex index = newTrieMemoryIndex(type);
        for (int i = 0; i < 99; ++i)
        {
            index.add(key, Clustering.EMPTY, decompose.apply(i));
        }

        final Iterator<Pair<ByteComparable, PrimaryKeys>> iterator = index.iterator();
        int i = 0;
        while (iterator.hasNext())
        {
            Pair<ByteComparable, PrimaryKeys> pair = iterator.next();
            assertEquals(1, pair.right.size());

            final int rowId = i;
            final ByteComparable expectedByteComparable = TypeUtil.isLiteral(type)
                                                          ? ByteComparable.fixedLength(decompose.apply(rowId))
                                                          : version -> type.asComparableBytes(decompose.apply(rowId), version);
            final ByteComparable actualByteComparable = pair.left;
            assertEquals("Mismatch at: " + i, 0, ByteComparable.compare(expectedByteComparable, actualByteComparable, ByteComparable.Version.OSS41));

            i++;
        }
        assertEquals(99, i);
    }

    private TrieMemoryIndex newTrieMemoryIndex(AbstractType<?> columnType)
    {
        ColumnMetadata column = ColumnMetadata.regularColumn(KEYSPACE, TABLE, REG_COL, columnType);
        TableMetadata metadata = TableMetadata.builder(KEYSPACE, TABLE)
                                              .addPartitionKeyColumn(PART_KEY_COL, UTF8Type.instance)
                                              .addRegularColumn(REG_COL, columnType)
                                              .partitioner(Murmur3Partitioner.instance)
                                              .caching(CachingParams.CACHE_NOTHING)
                                              .build();

        Map<String, String> options = new HashMap<>();
        options.put(IndexTarget.CUSTOM_INDEX_OPTION_NAME, StorageAttachedIndex.class.getCanonicalName());
        options.put("target", REG_COL);

        IndexMetadata indexMetadata = IndexMetadata.fromSchemaMetadata("col_index", IndexMetadata.Kind.CUSTOM, options);
        ColumnContext ci = new ColumnContext(metadata, indexMetadata);
        return new TrieMemoryIndex(ci);
    }
}
