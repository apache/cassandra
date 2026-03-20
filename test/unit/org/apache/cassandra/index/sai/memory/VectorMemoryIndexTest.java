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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.BootStrapper;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.ExcludingBounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.IncludingExcludingBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.disk.v1.vector.PrimaryKeyWithScore;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.inject.InvokePointBuilder;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.config.CassandraRelevantProperties.MEMTABLE_SHARD_COUNT;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class VectorMemoryIndexTest extends SAITester
{
    private static final Injections.Counter indexSearchCounter = Injections.newCounter("IndexSearchCounter")
                                                                           .add(InvokePointBuilder.newInvokePoint()
                                                                                                  .onClass(TrieMemoryIndex.class)
                                                                                                  .onMethod("search"))
                                                                           .build();

    private ColumnFamilyStore cfs;
    private StorageAttachedIndex index;
    private VectorMemoryIndex memtableIndex;
    private IPartitioner partitioner;
    private Map<DecoratedKey, Integer> keyMap;
    private Map<Integer, ByteBuffer> rowMap;
    private int dimensionCount;

    @BeforeClass
    public static void setShardCount()
    {
        MEMTABLE_SHARD_COUNT.setInt(8);
    }

    @Before
    public void setup() throws Throwable
    {
        TokenMetadata metadata = StorageService.instance.getTokenMetadata();
        metadata.updateNormalTokens(BootStrapper.getRandomTokens(metadata, 10), FBUtilities.getBroadcastAddressAndPort());

        dimensionCount = getRandom().nextIntBetween(2, 2048);
        index = SAITester.createMockIndex(VectorType.getInstance(FloatType.instance, dimensionCount));
        cfs = index.baseCfs();
        partitioner = cfs.getPartitioner();
        indexSearchCounter.reset();
        keyMap = new TreeMap<>();
        rowMap = new HashMap<>();

        Injections.inject(indexSearchCounter);
    }

    @Test
    public void randomQueryTest() throws Exception
    {
        // A non-null memtable tells it to track the mapping from primary key to vector, needed for brute force search
        Memtable memtable = Mockito.mock(Memtable.class);
        memtableIndex = new VectorMemoryIndex(index, memtable);

        for (int row = 0; row < getRandom().nextIntBetween(1000, 5000); row++)
        {
            int pk = getRandom().nextIntBetween(0, 10000);
            while (rowMap.containsKey(pk))
                pk = getRandom().nextIntBetween(0, 10000);
            var value = randomVector();
            rowMap.put(pk, value);
            addRow(pk, value);
        }

        List<DecoratedKey> keys = new ArrayList<>(keyMap.keySet());
        long actualVectorsReturned = 0;
        long expectedVectorsReturned = 0;
        double expectedRecall = 0.9;

        for (int executionCount = 0; executionCount < 1000; executionCount++)
        {
            Expression expression = generateRandomExpression();
            AbstractBounds<PartitionPosition> keyRange = generateRandomBounds(keys);
            Set<Integer> keysInRange = keys.stream().filter(keyRange::contains)
                                           .map(k -> Int32Type.instance.compose(k.getKey()))
                                           .collect(Collectors.toSet());

            Set<Integer> foundKeys = new HashSet<>();
            int limit = getRandom().nextIntBetween(1, 100);

            ReadCommand command = PartitionRangeReadCommand.create(cfs.metadata(),
                                                                   FBUtilities.nowInSeconds(),
                                                                   ColumnFilter.all(cfs.metadata()),
                                                                   RowFilter.none(),
                                                                   DataLimits.cqlLimits(limit),
                                                                   DataRange.allData(cfs.metadata().partitioner));

            long expectedResults = Math.min(limit, keysInRange.size());

            try (CloseableIterator<PrimaryKeyWithScore> iterator = memtableIndex.orderBy(new QueryContext(command,
                                                                                                          DatabaseDescriptor.getRangeRpcTimeout(TimeUnit.MILLISECONDS)),
                                                                                         expression, keyRange))
            {
                PrimaryKeyWithScore lastKey = null;
                while (iterator.hasNext() && foundKeys.size() < expectedResults)
                {
                    PrimaryKeyWithScore primaryKeyWithScore = iterator.next();
                    if (lastKey != null)
                        // This assertion only holds true as long as we query at most the expectedNumResults.
                        // Once we query deeper, we might get a key with a higher score than the last key.
                        // This is a direct consequence of the approximate part of ANN.
                        // Note that PrimaryKeyWithScore is flipped to descending order, so we use >= here.
                        assertTrue("Returned keys are not ordered by score", primaryKeyWithScore.compareTo(lastKey) >= 0);
                    lastKey = primaryKeyWithScore;
                    int key = Int32Type.instance.compose(primaryKeyWithScore.primaryKey().partitionKey().getKey());
                    assertFalse(foundKeys.contains(key));

                    assertTrue(keyRange.contains(primaryKeyWithScore.primaryKey().partitionKey()));
                    assertTrue(rowMap.containsKey(key));
                    foundKeys.add(key);
                }
                // Note that we weight each result evenly instead of each query evenly.
                actualVectorsReturned += foundKeys.size();
                expectedVectorsReturned += expectedResults;
                if (foundKeys.size() < expectedResults)
                    assertTrue("Expected at least " + expectedResults + " results but got " + foundKeys.size(),
                               foundKeys.size() >= expectedResults * expectedRecall);
            }
        }

        assertTrue("Expected at least " + expectedVectorsReturned + " results but got " + actualVectorsReturned,
                   actualVectorsReturned >= expectedVectorsReturned * expectedRecall);
    }

    @Test
    public void indexIteratorTest()
    {
        // VSTODO
    }

    private Expression generateRandomExpression()
    {
        Expression expression = Expression.create(index);
        expression.add(Operator.ANN, randomVector());
        return expression;
    }

    private ByteBuffer randomVector() {
        List<Float> rawVector = new ArrayList<>(dimensionCount);
        for (int i = 0; i < dimensionCount; i++) {
            rawVector.add(getRandom().nextFloat());
        }
        return VectorType.getInstance(FloatType.instance, dimensionCount).getSerializer().serialize(rawVector);
    }

    private AbstractBounds<PartitionPosition> generateRandomBounds(List<DecoratedKey> keys)
    {
        PartitionPosition leftBound = getRandom().nextBoolean() ? partitioner.getMinimumToken().minKeyBound()
                                                                : keys.get(getRandom().nextIntBetween(0, keys.size() - 1)).getToken().minKeyBound();

        PartitionPosition rightBound = getRandom().nextBoolean() ? partitioner.getMinimumToken().minKeyBound()
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

    private void addRow(int pk, ByteBuffer value)
    {
        DecoratedKey key = makeKey(cfs.metadata(), pk);
        memtableIndex.add(key, Clustering.EMPTY, value);
        keyMap.put(key, pk);
    }

    private DecoratedKey makeKey(TableMetadata table, Integer partitionKey)
    {
        ByteBuffer key = table.partitionKeyType.fromString(partitionKey.toString());
        return table.partitioner.decorateKey(key);
    }
}
