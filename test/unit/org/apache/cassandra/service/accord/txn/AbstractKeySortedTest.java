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

package org.apache.cassandra.service.accord.txn;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import com.google.common.collect.Lists;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import accord.primitives.Seekable;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.TokenRange;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.utils.ByteBufferUtil;

import static accord.utils.Property.qt;
import static org.junit.Assert.assertEquals;

public class AbstractKeySortedTest
{
    private static final TableId TABLE1 = TableId.fromString("00000000-0000-0000-0000-000000000001");

    @BeforeClass
    public static void beforeClass() throws Exception
    {
        SchemaLoader.prepareServer();
    }

    static class Item<K extends Seekable>
    {
        final K key;
        final int value;

        public Item(K key, int value)
        {
            this.key = key;
            this.value = value;
        }
        
        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Item<?> item = (Item<?>) o;
            return value == ((Item<?>) o).value && key.equals(item.key);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(key, value);
        }

        @Override
        public String toString()
        {
            return "Item{" +
                   "key=" + key +
                   ", value=" + value +
                   '}';
        }
    }

    static class SortedItems<K extends Seekable> extends AbstractKeySorted<Item<K>>
    {
        @SafeVarargs
        public SortedItems(Item<K>... items)
        {
            super(items, items[0].key.domain());
        }

        public SortedItems(List<Item<K>> items)
        {
            super(items, items.get(0).key.domain());
        }

        @Override
        int compareNonKeyFields(Item<K> left, Item<K> right)
        {
            return Integer.compare(left.value, right.value);
        }

        @Override
        Seekable getKey(Item<K> item)
        {
            return item.key;
        }

        @Override
        Item[] newArray(int size)
        {
            return new Item[size];
        }
    }

    private static PartitionKey key(int k)
    {
        DecoratedKey dk = ByteOrderedPartitioner.instance.decorateKey(ByteBufferUtil.bytes(k));
        return new PartitionKey(TABLE1, dk);
    }

    private static TokenRange range(int start, int end)
    {
        Token startToken = ByteOrderedPartitioner.instance.decorateKey(ByteBufferUtil.bytes(start)).getToken();
        Token endToken = ByteOrderedPartitioner.instance.decorateKey(ByteBufferUtil.bytes(end)).getToken();
        return TokenRange.create(TABLE1, startToken, endToken);
    }

    private static Item<PartitionKey> item(int k, int v)
    {
        return new Item<>(key(k), v);
    }

    private static Item<TokenRange> item(int s, int e, int v)
    {
        return new Item<>(range(s, e), v);
    }

    @SafeVarargs
    private static List<Item<PartitionKey>> itemList(Item<PartitionKey>... items)
    {
        return Lists.newArrayList(items);
    }

    @Test
    public void checkInitialSorting()
    {
        List<Item<PartitionKey>> initial = itemList(item(5, 4), item(3, 3), item(3, 1), item(6, 5));
        SortedItems<PartitionKey> expected = new SortedItems<>(item(3, 1), item(3, 3), item(5, 4), item(6, 5));
        expected.validateOrder();
        SortedItems<PartitionKey> actual = new SortedItems<>(initial);
        actual.validateOrder();
        assertEquals(expected, actual);
    }

    @Test
    public void checkIterationForKey()
    {
        SortedItems<PartitionKey> source = new SortedItems<>(item(1, 5), item(3, 1), item(3, 3), item(5, 4), item(6, 5));
        source.validateOrder();

        List<Item<?>> actual = new ArrayList<>();
        source.forEachWithKey(key(0), actual::add);
        assertEquals(List.of(), actual);

        actual.clear();
        source.forEachWithKey(key(1), actual::add);
        assertEquals(List.of(item(1, 5)), actual);

        actual.clear();
        source.forEachWithKey(key(2), actual::add);
        assertEquals(List.of(), actual);

        actual.clear();
        source.forEachWithKey(key(3), actual::add);
        assertEquals(itemList(item(3, 1), item(3, 3)), actual);

        actual.clear();
        source.forEachWithKey(key(4), i -> Assert.fail());
        assertEquals(List.of(), actual);

        actual.clear();
        source.forEachWithKey(key(5), actual::add);
        assertEquals(itemList(item(5, 4)), actual);

        actual.clear();
        source.forEachWithKey(key(6), actual::add);
        assertEquals(itemList(item(6, 5)), actual);

        actual.clear();
        source.forEachWithKey(key(7), i -> Assert.fail());
        assertEquals(List.of(), actual);
    }

    @Test
    public void forEachWithKeyRegressionTest()
    {
        SortedItems<TokenRange> source = new SortedItems<>(item(1, 2, 3), item(2, 6,3));
        source.validateOrder();

        List<Item<?>> actual = new ArrayList<>();
        source.forEachWithKey(range(2, 6), actual::add);
        assertEquals(List.of(item(2, 6, 3)), actual);
    }

    @Test
    public void forEachWithKeyTest()
    {
        qt().check(rs -> {
            int numberOfSortedItems = rs.nextInt(1, 20);

            int[] starts = new int[numberOfSortedItems];
            int[] ends = new int[numberOfSortedItems];
            List<Item<TokenRange>> items = new ArrayList<>();
            int minimum = rs.nextInt(0, 10);

            for (int i = 0; i < numberOfSortedItems; i++)
            {
                starts[i] = minimum + rs.nextInt(1, 20);
                ends[i] = starts[i] + rs.nextInt(10, 25);
                items.add(item(starts[i], ends[i], rs.nextInt()));
                minimum = ends[i];
            }

            SortedItems<TokenRange> source = new SortedItems<>(items);
            source.validateOrder();

            // 1 - no match
            // 2 - overlaps on the left end
            // 3 - overlaps on the right end
            // 4 - subset of range
            // 5 - superset of range
            // 6 - exact match
            // 7 - spans several ranges
            for (int kind = 1; kind < 8; kind++)
            {
                int idx = rs.nextInt(numberOfSortedItems);
                int start = starts[idx];
                int end = ends[idx];
                TokenRange query;

                switch (kind)
                {
                    case 1:
                        query = range(minimum, minimum + rs.nextInt(1, 10));
                        break;
                    case 2:
                        query = range(start - 1, start);
                        break;
                    case 3:
                        query = range(end - 1, end + 1);
                        break;
                    case 4:
                        query = range(start + 1, end - 1);
                        break;
                    case 5:
                        query = range(start - 1, end + 1);
                        break;
                    case 6:
                        query = range(start, end);
                        break;
                    case 7:
                        query = range(start - 1, ends[rs.nextInt(idx, numberOfSortedItems)] + 1);
                        break;
                    default:
                        throw new IllegalStateException("Unhandled kind " + kind);
                }

                List<Item<TokenRange>> expected = new ArrayList<>();
                for (Item<TokenRange> candidate : items)
                    if (query.compareIntersecting(candidate.key) == 0)
                        expected.add(candidate);

                List<Item<TokenRange>> actual = new ArrayList<>();
                source.forEachWithKey(query, actual::add);

                assertEquals(expected, actual);
            }
        });
    }
}
