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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.IntFunction;
import java.util.stream.Collectors;

import org.junit.Test;

import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.ExcludingBounds;
import org.apache.cassandra.dht.IncludingExcludingBounds;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.iterators.KeyRangeIterator;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.PrimaryKeys;
import org.apache.cassandra.index.sai.utils.SAIRandomizedTester;
import org.apache.cassandra.schema.CachingParams;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.MockSchema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TrieMemoryIndexTest extends SAIRandomizedTester
{
    private static final String KEYSPACE = "test_keyspace";
    private static final String TABLE = "test_table";
    private static final String PART_KEY_COL = "key";
    private static final String REG_COL = "col";

    private static final DecoratedKey key = Murmur3Partitioner.instance.decorateKey(ByteBufferUtil.bytes("key"));

    private StorageAttachedIndex index;

    @Test
    public void heapGrowsAsDataIsAddedTest()
    {
        TrieMemoryIndex index = newTrieMemoryIndex(Int32Type.instance);
        for (int i = 0; i < 99; i++)
        {
            assertTrue(index.add(key, Clustering.EMPTY, Int32Type.instance.decompose(i)) > 0);
        }
    }

    @Test
    public void randomQueryTest() throws Exception
    {
        TrieMemoryIndex index = newTrieMemoryIndex(Int32Type.instance);

        Map<DecoratedKey, Integer> keyMap = new TreeMap<>();
        Map<Integer, Integer> rowMap = new HashMap<>();

        for (int row = 0; row < getRandom().nextIntBetween(1000, 5000); row++)
        {
            int pk = getRandom().nextIntBetween(0, 10000);
            while (rowMap.containsKey(pk))
                pk = getRandom().nextIntBetween(0, 10000);
            int value = getRandom().nextIntBetween(0, 100);
            rowMap.put(pk, value);
            DecoratedKey key = Murmur3Partitioner.instance.decorateKey(Int32Type.instance.decompose(pk));
            index.add(key, Clustering.EMPTY, Int32Type.instance.decompose(value));
            keyMap.put(key, pk);
        }

        List<DecoratedKey> keys = new ArrayList<>(keyMap.keySet());

        for (int executionCount = 0; executionCount < 1000; executionCount++)
        {
            Expression expression = generateRandomExpression();

            AbstractBounds<PartitionPosition> keyRange = generateRandomBounds(keys);

            Set<Integer> expectedKeys = keyMap.keySet()
                                              .stream()
                                              .filter(keyRange::contains)
                                              .map(keyMap::get)
                                              .filter(pk -> expression.isSatisfiedBy(Int32Type.instance.decompose(rowMap.get(pk))))
                                              .collect(Collectors.toSet());

            Set<Integer> foundKeys = new HashSet<>();

            try (KeyRangeIterator iterator = index.search(null, expression, keyRange))
            {
                while (iterator.hasNext())
                {
                    int key = Int32Type.instance.compose(iterator.next().partitionKey().getKey());
                    assertFalse(foundKeys.contains(key));
                    foundKeys.add(key);
                }
            }

            assertEquals(expectedKeys, foundKeys);
        }
    }

    private AbstractBounds<PartitionPosition> generateRandomBounds(List<DecoratedKey> keys)
    {
        PartitionPosition leftBound = getRandom().nextBoolean() ? Murmur3Partitioner.instance.getMinimumToken().minKeyBound()
                                                                : keys.get(getRandom().nextIntBetween(0, keys.size() - 1)).getToken().minKeyBound();

        PartitionPosition rightBound = getRandom().nextBoolean() ? Murmur3Partitioner.instance.getMinimumToken().minKeyBound()
                                                                 : keys.get(getRandom().nextIntBetween(0, keys.size() - 1)).getToken().maxKeyBound();

        AbstractBounds<PartitionPosition> keyRange;

        if (leftBound.isMinimum() && rightBound.isMinimum())
            keyRange = new Range<>(leftBound, rightBound);
        else
        {
            if (AbstractBounds.strictlyWrapsAround(leftBound, rightBound))
            {
                PartitionPosition temp = leftBound;
                leftBound = rightBound;
                rightBound = temp;
            }
            if (getRandom().nextBoolean())
                keyRange = new Bounds<>(leftBound, rightBound);
            else if (getRandom().nextBoolean())
                keyRange = new ExcludingBounds<>(leftBound, rightBound);
            else
                keyRange = new IncludingExcludingBounds<>(leftBound, rightBound);
        }
        return keyRange;
    }

    private Expression generateRandomExpression()
    {
        Expression expression = Expression.create(index);

        int equality = getRandom().nextIntBetween(0, 100);
        int lower = getRandom().nextIntBetween(0, 75);
        int upper = getRandom().nextIntBetween(25, 100);
        while (upper <= lower)
            upper = getRandom().nextIntBetween(0, 100);

        if (getRandom().nextBoolean())
            expression.add(Operator.EQ, Int32Type.instance.decompose(equality));
        else
        {
            boolean useLower = getRandom().nextBoolean();
            boolean useUpper = getRandom().nextBoolean();
            if (!useLower && !useUpper)
                useLower = useUpper = true;
            if (useLower)
                expression.add(getRandom().nextBoolean() ? Operator.GT : Operator.GTE, Int32Type.instance.decompose(lower));
            if (useUpper)
                expression.add(getRandom().nextBoolean() ? Operator.LT : Operator.LTE, Int32Type.instance.decompose(upper));
        }
        return expression;
    }

    @Test
    public void shouldAcceptPrefixValuesTest()
    {
        shouldAcceptPrefixValuesForType(UTF8Type.instance, i -> UTF8Type.instance.decompose(String.format("%03d", i)));
        shouldAcceptPrefixValuesForType(Int32Type.instance, Int32Type.instance::decompose);
    }

    private void shouldAcceptPrefixValuesForType(AbstractType<?> type, IntFunction<ByteBuffer> decompose)
    {
        TrieMemoryIndex index = newTrieMemoryIndex(type);
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
            final ByteComparable expectedByteComparable = index.index.termType().isLiteral()
                                                          ? ByteComparable.fixedLength(decompose.apply(rowId))
                                                          : version -> type.asComparableBytes(decompose.apply(rowId), version);
            final ByteComparable actualByteComparable = pair.left;
            assertEquals("Mismatch at: " + i, 0, ByteComparable.compare(expectedByteComparable, actualByteComparable, ByteComparable.Version.OSS50));

            i++;
        }
        assertEquals(99, i);
    }

    private TrieMemoryIndex newTrieMemoryIndex(AbstractType<?> columnType)
    {
        TableMetadata table = TableMetadata.builder(KEYSPACE, TABLE)
                                           .addPartitionKeyColumn(PART_KEY_COL, UTF8Type.instance)
                                           .addRegularColumn(REG_COL, columnType)
                                           .partitioner(Murmur3Partitioner.instance)
                                           .caching(CachingParams.CACHE_NOTHING)
                                           .build();

        Map<String, String> options = new HashMap<>();
        options.put(IndexTarget.CUSTOM_INDEX_OPTION_NAME, StorageAttachedIndex.class.getCanonicalName());
        options.put("target", REG_COL);

        IndexMetadata indexMetadata = IndexMetadata.fromSchemaMetadata("col_index", IndexMetadata.Kind.CUSTOM, options);

        ColumnFamilyStore cfs = MockSchema.newCFS(table);

        index = new StorageAttachedIndex(cfs, indexMetadata);
        return new TrieMemoryIndex(index);
    }
}
