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

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.utils.ByteBufferUtil;

public class AbstractKeySortedTest
{
    private static final TableId TABLE1 = TableId.fromString("00000000-0000-0000-0000-000000000001");

    @BeforeClass
    public static void beforeClass() throws Exception
    {
        SchemaLoader.prepareServer();
    }

    static class Item
    {
        final PartitionKey key;
        final int value;

        public Item(PartitionKey key, int value)
        {
            this.key = key;
            this.value = value;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Item item = (Item) o;
            return value == item.value && key.equals(item.key);
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

    static class SortedItems extends AbstractKeySorted<Item>
    {
        public SortedItems(Item... items)
        {
            super(items);
        }

        public SortedItems(List<Item> items)
        {
            super(items);
        }

        @Override
        int compareNonKeyFields(Item left, Item right)
        {
            return Integer.compare(left.value, right.value);
        }

        @Override
        PartitionKey getKey(Item item)
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

    private static Item item(int k, int v)
    {
        return new Item(key(k), v);
    }

    private static List<Item> itemList(Item... items)
    {
        return Lists.newArrayList(items);
    }

    @Test
    public void checkInitialSorting()
    {
        List<Item> initial = itemList(item(5, 4), item(3, 3), item(3, 1), item(6, 5));
        SortedItems expected = new SortedItems(item(3, 1), item(3, 3), item(5, 4), item(6, 5));
        expected.validateOrder();
        SortedItems actual = new SortedItems(initial);
        actual.validateOrder();
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void checkIterationForKey()
    {
        SortedItems source = new SortedItems(item(1, 5), item(3, 1), item(3, 3), item(5, 4), item(6, 5));
        source.validateOrder();

        source.forEachWithKey(key(0), i -> Assert.fail());
        source.forEachWithKey(key(1), i -> Assert.assertEquals(item(1, 5), i));
        source.forEachWithKey(key(2), i -> Assert.fail());
        List<Item> actual = new ArrayList<>();
        source.forEachWithKey(key(3), actual::add);
        Assert.assertEquals(itemList(item(3, 1), item(3, 3)), actual);
        source.forEachWithKey(key(4), i -> Assert.fail());
        source.forEachWithKey(key(5), i -> Assert.assertEquals(item(5, 4), i));
        source.forEachWithKey(key(6), i -> Assert.assertEquals(item(6, 5), i));
        source.forEachWithKey(key(7), i -> Assert.fail());
    }
}
