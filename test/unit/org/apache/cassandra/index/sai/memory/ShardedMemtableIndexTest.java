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
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.iterators.KeyRangeIterator;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.SAIRandomizedTester;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

import static org.apache.cassandra.config.CassandraRelevantProperties.MEMTABLE_SHARD_COUNT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ShardedMemtableIndexTest extends SAIRandomizedTester
{
    private ColumnFamilyStore cfs;
    private IPartitioner partitioner;
    private StorageAttachedIndex index;
    private ShardedMemtableIndex memtableIndex;
    private Map<DecoratedKey, Integer> keyMap;
    private Map<Integer, Integer> rowMap;

    @BeforeClass
    public static void setShardCount() {
        System.setProperty(MEMTABLE_SHARD_COUNT.getKey(), "8");
    }

    @Before
    public void setup() throws Throwable
    {
        // CQLTester @BeforeClass already sets up server.
        // Set up the keyspace and the table.
        String keyspace = createKeyspace("CREATE KEYSPACE %s with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } and durable_writes = false");
        String table = createTable(keyspace,
                                   "CREATE TABLE %s (pk int PRIMARY KEY, val int)",
                            "memtable_index");
        execute("use " + keyspace + ";");

        setupCfsAndIndex(keyspace, table);

        partitioner = cfs.getPartitioner();
        keyMap = new TreeMap<>();
        rowMap = new HashMap<>();
    }

    public void setupCfsAndIndex(String keyspace, String table)
    {
        cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        cfs.disableAutoCompaction();

        Map<String, String> options = new HashMap<>();
        options.put(IndexTarget.CUSTOM_INDEX_OPTION_NAME,
                    StorageAttachedIndex.class.getCanonicalName());
        options.put("target", "val");

        IndexMetadata indexMetadata = IndexMetadata.fromSchemaMetadata("val_idx", IndexMetadata.Kind.CUSTOM, options);

        index = new StorageAttachedIndex(cfs, indexMetadata);
    }

    @Test
    public void onHeapAllocationTest()
    {
        // Should take the system variable-based shard count here
        memtableIndex = new ShardedMemtableIndex(index, cfs, null, cfs.getCurrentMemtable());
        assertEquals(8, memtableIndex.shardCount());

        assertEquals(0L, memtableIndex.writeCount());

        for (int row = 0; row < 100; row++)
        {
            addRow(row, row);
        }

        assertTrue(memtableIndex.writeCount() > 0);
    }

    @Test
    public void randomQueryTest() throws Exception
    {
        // Should take the system variable-based shard count here
        memtableIndex = new ShardedMemtableIndex(index, cfs, null, cfs.getCurrentMemtable());
        assertEquals(8, memtableIndex.shardCount());

        for (int row = 0; row < getRandom().nextIntBetween(1000, 5000); row++)
        {
            int pk = getRandom().nextIntBetween(0, 10000);
            while (rowMap.containsKey(pk))
                pk = getRandom().nextIntBetween(0, 10000);
            int value = getRandom().nextIntBetween(0, 100);
            rowMap.put(pk, value);
            addRow(pk, value);
        }

        List<DecoratedKey> keys = new ArrayList<>(keyMap.keySet());

        for (int executionCount = 0; executionCount < 1000; executionCount++)
        {
            Expression expression = generateRandomExpression(index);

            AbstractBounds<PartitionPosition> keyRange = generateRandomBounds(keys, partitioner);

            Set<Integer> expectedKeys = keyMap.keySet()
                                              .stream()
                                              .filter(keyRange::contains)
                                              .map(keyMap::get)
                                              .filter(pk -> expression.isSatisfiedBy(Int32Type.instance.decompose(rowMap.get(pk))))
                                              .collect(Collectors.toSet());

            Set<Integer> foundKeys = new HashSet<>();

            try (KeyRangeIterator iterator = memtableIndex.search(null, expression, keyRange))
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

    @Test
    public void indexIteratorTest()
    {
        // Should take the system variable-based shard count here
        memtableIndex = new ShardedMemtableIndex(index, cfs, null, cfs.getCurrentMemtable());
        assertEquals(8, memtableIndex.shardCount());

        Map<Integer, Set<DecoratedKey>> terms = buildTermMap();

        terms.entrySet()
             .stream()
             .forEach(entry -> entry.getValue()
                                    .forEach(pk -> addRow(Int32Type.instance.compose(pk.getKey()), entry.getKey())));

        for (int executionCount = 0; executionCount < 1000; executionCount++)
        {
            // These keys have midrange tokens that select 3 of the 8 shards
            DecoratedKey minimum = makeKey(cfs.metadata(), getRandom().nextIntBetween(0, 20000));
            DecoratedKey temp = makeKey(cfs.metadata(), getRandom().nextIntBetween(0, 20000));
            while (temp.compareTo(minimum) <= 0)
                temp = makeKey(cfs.metadata(), getRandom().nextIntBetween(0, 20000));
            DecoratedKey maximum = temp;

            Iterator<Pair<ByteComparable, Iterator<PrimaryKey>>> iterator = memtableIndex.iterator(minimum, maximum);

            while (iterator.hasNext())
            {
                Pair<ByteComparable, Iterator<PrimaryKey>> termPair = iterator.next();
                int term = termFromComparable(termPair.left);

                // The iterator will return keys outside the range of min/max so we need to filter here to
                // get the correct keys
                List<DecoratedKey> expectedPks = terms.get(term)
                                                      .stream()
                                                      .filter(pk -> pk.compareTo(minimum) >= 0 && pk.compareTo(maximum) <= 0)
                                                      .sorted()
                                                      .collect(Collectors.toList());

                List<DecoratedKey> termPks = new ArrayList<>();

                while (termPair.right.hasNext())
                {
                    DecoratedKey pk = termPair.right.next().partitionKey();
                    if (pk.compareTo(minimum) >= 0 && pk.compareTo(maximum) <= 0)
                        termPks.add(pk);
                }

                assertEquals(expectedPks, termPks);
            }
        }
    }

    private int termFromComparable(ByteComparable comparable)
    {
        ByteSource.Peekable peekable = ByteSource.peekable(comparable.asComparableBytes(ByteComparable.Version.OSS50));
        return Int32Type.instance.compose(Int32Type.instance.fromComparableBytes(peekable, ByteComparable.Version.OSS50));
    }

    private Map<Integer, Set<DecoratedKey>> buildTermMap()
    {
        Map<Integer, Set<DecoratedKey>> terms = new HashMap<>();

        for (int count = 0; count < 10000; count++)
        {
            int term = getRandom().nextIntBetween(0, 100);
            Set<DecoratedKey> pks;
            if (terms.containsKey(term))
                pks = terms.get(term);
            else
            {
                pks = new HashSet<>();
                terms.put(term, pks);
            }
            DecoratedKey key = makeKey(cfs.metadata(), getRandom().nextIntBetween(0, 20000));
            while (pks.contains(key))
                key = makeKey(cfs.metadata(), getRandom().nextIntBetween(0, 20000));
            pks.add(key);
        }

        return terms;
    }

    private void addRow(int pk, int value)
    {
        DecoratedKey key = makeKey(cfs.metadata(), pk);
        memtableIndex.index(key, Clustering.EMPTY, Int32Type.instance.decompose(value));
        keyMap.put(key, pk);
    }

    private DecoratedKey makeKey(TableMetadata table, Integer partitionKey)
    {
        ByteBuffer key = table.partitionKeyType.fromString(partitionKey.toString());
        return table.partitioner.decorateKey(key);
    }
}
