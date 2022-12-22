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
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.IntFunction;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.index.TargetParser;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.PrimaryKeys;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.schema.CachingParams;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.MockSchema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;

import static org.junit.Assert.assertEquals;

public class TrieMemoryIndexTest
{
    private static final String KEYSPACE = "test_keyspace";
    private static final String TABLE = "test_table";
    private static final String PART_KEY_COL = "key";
    private static final String REG_COL = "col";

    private static DecoratedKey key = Murmur3Partitioner.instance.decorateKey(ByteBufferUtil.bytes("key"));

    private TableMetadata table;

    @Before
    public void setup()
    {
        SchemaLoader.prepareServer();
    }

    @Test
    public void iteratorShouldReturnAllValuesNumeric()
    {
        TrieMemoryIndex index = newTrieMemoryIndex(Int32Type.instance, Int32Type.instance);

        for (int row = 0; row < 100; row++)
        {
            index.add(makeKey(table, Integer.toString(row)), Clustering.EMPTY, Int32Type.instance.decompose(row / 10), allocatedBytes -> {}, allocatesBytes -> {});
        }

        Iterator<Pair<ByteComparable, PrimaryKeys>> iterator = index.iterator();
        int valueCount = 0;
        while(iterator.hasNext())
        {
            Pair<ByteComparable, PrimaryKeys> pair = iterator.next();
            int value = ByteSourceInverse.getSignedInt(pair.left.asComparableBytes(ByteComparable.Version.OSS41));
            int idCount = 0;
            Iterator<PrimaryKey> primaryKeyIterator = pair.right.iterator();
            while (primaryKeyIterator.hasNext())
            {
                PrimaryKey primaryKey = primaryKeyIterator.next();
                int id = Int32Type.instance.compose(primaryKey.partitionKey().getKey());
                assertEquals(id/10, value);
                idCount++;
            }
            assertEquals(10, idCount);
            valueCount++;
        }
        assertEquals(10, valueCount);
    }

    @Test
    public void iteratorShouldReturnAllValuesString()
    {
        TrieMemoryIndex index = newTrieMemoryIndex(UTF8Type.instance, UTF8Type.instance);

        for (int row = 0; row < 100; row++)
        {
            index.add(makeKey(table, Integer.toString(row)), Clustering.EMPTY, UTF8Type.instance.decompose(Integer.toString(row / 10)), allocatedBytes -> {}, allocatesBytes -> {});
        }

        Iterator<Pair<ByteComparable, PrimaryKeys>> iterator = index.iterator();
        int valueCount = 0;
        while(iterator.hasNext())
        {
            Pair<ByteComparable, PrimaryKeys> pair = iterator.next();
            String value = new String(ByteSourceInverse.readBytes(pair.left.asPeekableBytes(ByteComparable.Version.OSS41)), StandardCharsets.UTF_8);
            int idCount = 0;
            Iterator<PrimaryKey> primaryKeyIterator = pair.right.iterator();
            while (primaryKeyIterator.hasNext())
            {
                PrimaryKey primaryKey = primaryKeyIterator.next();
                String id = UTF8Type.instance.compose(primaryKey.partitionKey().getKey());
                assertEquals(Integer.toString(Integer.parseInt(id) / 10), value);
                idCount++;
            }
            assertEquals(10, idCount);
            valueCount++;
        }
        assertEquals(10, valueCount);
    }

    @Test
    public void shouldAcceptPrefixValues()
    {
        shouldAcceptPrefixValuesForType(UTF8Type.instance, i -> UTF8Type.instance.decompose(String.format("%03d", i)));
        shouldAcceptPrefixValuesForType(Int32Type.instance, Int32Type.instance::decompose);
    }

    private void shouldAcceptPrefixValuesForType(AbstractType<?> type, IntFunction<ByteBuffer> decompose)
    {
        final TrieMemoryIndex index = newTrieMemoryIndex(UTF8Type.instance, type);
        for (int i = 0; i < 99; ++i)
        {
            index.add(key, Clustering.EMPTY, decompose.apply(i), allocatedBytes -> {}, allocatesBytes -> {});
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

    private TrieMemoryIndex newTrieMemoryIndex(AbstractType<?> partitionKeyType, AbstractType<?> columnType)
    {
        table = TableMetadata.builder(KEYSPACE, TABLE)
                             .addPartitionKeyColumn(PART_KEY_COL, partitionKeyType)
                             .addRegularColumn(REG_COL, columnType)
                             .partitioner(Murmur3Partitioner.instance)
                             .caching(CachingParams.CACHE_NOTHING)
                             .build();

        Map<String, String> options = new HashMap<>();
        options.put(IndexTarget.CUSTOM_INDEX_OPTION_NAME, StorageAttachedIndex.class.getCanonicalName());
        options.put("target", REG_COL);

        IndexMetadata indexMetadata = IndexMetadata.fromSchemaMetadata("col_index", IndexMetadata.Kind.CUSTOM, options);
        Pair<ColumnMetadata, IndexTarget.Type> target = TargetParser.parse(table, indexMetadata);
        IndexContext indexContext = new IndexContext(table.keyspace,
                                                     table.name,
                                                     table.partitionKeyType,
                                                     table.comparator,
                                                     target.left,
                                                     target.right,
                                                     indexMetadata,
                                                     MockSchema.newCFS(table));

        return new TrieMemoryIndex(indexContext);
    }

    DecoratedKey makeKey(TableMetadata table, Object...partitionKeys)
    {
        ByteBuffer key;
        if (TypeUtil.isComposite(table.partitionKeyType))
            key = ((CompositeType)table.partitionKeyType).decompose(partitionKeys);
        else
            key = table.partitionKeyType.fromString((String)partitionKeys[0]);
        return table.partitioner.decorateKey(key);
    }
}
