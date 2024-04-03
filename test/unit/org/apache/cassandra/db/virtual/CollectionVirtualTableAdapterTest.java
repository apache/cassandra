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

package org.apache.cassandra.db.virtual;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.virtual.model.CollectionEntry;
import org.apache.cassandra.db.virtual.model.CollectionEntryTestRow;
import org.apache.cassandra.db.virtual.model.PartitionEntryTestRow;
import org.apache.cassandra.db.virtual.walker.CollectionEntryTestRowWalker;
import org.apache.cassandra.db.virtual.walker.PartitionEntryTestRowWalker;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.METRIC_SCOPE_UNDEFINED;
import static org.junit.Assert.assertEquals;

/**
 * Test that verifies the functionality of the collection virtual table adapter, which is used to expose internal
 * collection data as a virtual table. This test is a bit more complex than the other virtual table tests because it
 * tests the functionality of the virtual table adapter itself, rather than just the virtual table.
 */
public class CollectionVirtualTableAdapterTest extends CQLTester
{
    private static final String KS_NAME = "vts";
    private static final String VT_NAME = "collection_virtual_table";
    private static final String VT_NAME_1 = "map_key_filter_virtual_table";
    private static final String VT_NAME_2 = "map_value_filter_virtual_table";
    private final List<VirtualTable> tables = new ArrayList<>();
    private final List<CollectionEntry> internalTestCollection = new ArrayList<>();
    private final Map<String, CollectionEntry> internalTestMap = new HashMap<>();

    private static void addSinglePartitionData(Collection<CollectionEntry> list)
    {
        list.add(new CollectionEntry("1984", "key", 3, "value",
                                     1, 1, 1, (short) 1, (byte) 1, true));
        list.add(new CollectionEntry("1984", "key", 2, "value",
                                     1, 1, 1, (short) 1, (byte) 1, true));
        list.add(new CollectionEntry("1984", "key", 1, "value",
                                     1, 1, 1, (short) 1, (byte) 1, true));
    }

    private static void addMultiPartitionData(Collection<CollectionEntry> list)
    {
        addSinglePartitionData(list);
        list.add(new CollectionEntry("1985", "key", 3, "value",
                                     1, 1, 1, (short) 1, (byte) 1, true));
        list.add(new CollectionEntry("1985", "key", 2, "value",
                                     1, 1, 1, (short) 1, (byte) 1, true));
        list.add(new CollectionEntry("1985", "key", 1, "value",
                                     1, 1, 1, (short) 1, (byte) 1, true));
    }

    @Before
    public void config() throws Exception
    {
        tables.add(CollectionVirtualTableAdapter.create(
            KS_NAME,
            VT_NAME,
            "The collection virtual table",
            new CollectionEntryTestRowWalker(),
            internalTestCollection,
            CollectionEntryTestRow::new));
        tables.add(CollectionVirtualTableAdapter.createSinglePartitionedKeyFiltered(
            KS_NAME,
            VT_NAME_1,
            "The partition key filtered virtual table",
            new PartitionEntryTestRowWalker(),
            internalTestMap,
            internalTestMap::containsKey,
            PartitionEntryTestRow::new));
        tables.add(CollectionVirtualTableAdapter.createSinglePartitionedValueFiltered(
            KS_NAME,
            VT_NAME_2,
            "The partition value filtered virtual table",
            new PartitionEntryTestRowWalker(),
            internalTestMap,
            value -> value instanceof CollectionEntryExt,
            PartitionEntryTestRow::new));
        VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace(KS_NAME, tables));
    }

    @After
    public void postCleanup()
    {
        internalTestCollection.clear();
        internalTestMap.clear();
    }

    @Test
    public void testSelectAll()
    {
        addSinglePartitionData(internalTestCollection);
        List<CollectionEntry> sortedClustering = new ArrayList<>(internalTestCollection);
        sortedClustering.sort(Comparator.comparingLong(CollectionEntry::getOrderedKey));
        ResultSet result = executeNet(String.format("SELECT * FROM %s.%s", KS_NAME, VT_NAME));

        int index = 0;
        for (Row row : result)
        {
            assertEquals(sortedClustering.get(index).getPrimaryKey(), row.getString("primary_key"));
            assertEquals(sortedClustering.get(index).getSecondaryKey(), row.getString("secondary_key"));
            assertEquals(sortedClustering.get(index).getOrderedKey(), row.getLong("ordered_key"));
            assertEquals(sortedClustering.get(index).getIntValue(), row.getInt("int_value"));
            assertEquals(sortedClustering.get(index).getLongValue(), row.getLong("long_value"));
            assertEquals(sortedClustering.get(index).getValue(), row.getString("value"));
            assertEquals(sortedClustering.get(index).getDoubleValue(), row.getDouble("double_value"), 0.0);
            assertEquals(sortedClustering.get(index).getShortValue(), row.getShort("short_value"));
            assertEquals(sortedClustering.get(index).getByteValue(), row.getByte("byte_value"));
            assertEquals(sortedClustering.get(index).getBooleanValue(), row.getBool("boolean_value"));
            index++;
        }
        assertEquals(sortedClustering.size(), index);
    }

    @Test
    public void testSelectPartition()
    {
        addMultiPartitionData(internalTestCollection);
        ResultSet result = executeNet(
            String.format("SELECT * FROM %s.%s WHERE primary_key = ? AND secondary_key = ?", KS_NAME, VT_NAME),
            "1984", "key");

        AtomicInteger size = new AtomicInteger(3);
        result.forEach(row -> {
            assertEquals("1984", row.getString("primary_key"));
            assertEquals("key", row.getString("secondary_key"));
            size.decrementAndGet();
        });
        assertEquals(0, size.get());
    }

    @Test
    public void testSelectPartitionMap()
    {
        internalTestMap.put("1984", new CollectionEntry("primary", "key", 3, "value",
                                                        1, 1, 1, (short) 1, (byte) 1, true));
        ResultSet result = executeNet(String.format("SELECT * FROM %s.%s WHERE key = ?", KS_NAME, VT_NAME_1),
                                      "1984");

        AtomicInteger size = new AtomicInteger(1);
        result.forEach(row -> {
            assertEquals("1984", row.getString("key"));
            assertEquals("primary", row.getString("primary_key"));
            assertEquals("key", row.getString("secondary_key"));
            size.decrementAndGet();
        });
        assertEquals(0, size.get());
    }

    @Test
    public void testSelectPartitionUnknownKey()
    {
        internalTestMap.put("1984", new CollectionEntry("primary", "key", 3, "value",
                                                        1, 1, 1, (short) 1, (byte) 1, true));
        ResultSet first = executeNet(String.format("SELECT * FROM %s.%s WHERE key = ?", KS_NAME, VT_NAME_1),
                                     METRIC_SCOPE_UNDEFINED);
        assertEquals(0, first.all().size());

        addSinglePartitionData(internalTestCollection);
        ResultSet second = executeNet(
            String.format("SELECT * FROM %s.%s WHERE primary_key = ? AND secondary_key = ?", KS_NAME, VT_NAME),
            METRIC_SCOPE_UNDEFINED, "key");
        assertEquals(0, second.all().size());
    }

    @Test
    public void testSelectPartitionValueFitered()
    {
        internalTestMap.put("1984", new CollectionEntry("primary", "key", 3, "value",
                                                        1, 1, 1, (short) 1, (byte) 1, true));
        internalTestMap.put("1985", new CollectionEntryExt("primary", "key", 3, "value",
                                                           1, 1, 1, (short) 1, (byte) 1, true, "extra"));

        ResultSet first = executeNet(String.format("SELECT * FROM %s.%s WHERE key = ?", KS_NAME, VT_NAME_2),
                                     "1984");
        assertEquals(0, first.all().size());

        ResultSet second = executeNet(String.format("SELECT * FROM %s.%s WHERE key = ?", KS_NAME, VT_NAME_2),
                                      "1985");
        assertEquals(1, second.all().size());
    }

    @Test
    public void testSelectEmptyPartition()
    {
        addSinglePartitionData(internalTestCollection);
        assertRowsNet(executeNet(String.format("SELECT * FROM %s.%s WHERE primary_key = 'EMPTY'", KS_NAME, VT_NAME)));
    }

    @Test
    public void testSelectEmptyCollection()
    {
        internalTestCollection.clear();
        assertRowsNet(executeNet(String.format("SELECT * FROM %s.%s", KS_NAME, VT_NAME)));
    }

    private static class CollectionEntryExt extends CollectionEntry
    {
        public CollectionEntryExt(String primaryKey,
                                  String secondaryKey,
                                  long orderedKey,
                                  String value,
                                  int intValue,
                                  long longValue,
                                  double doubleValue,
                                  short shortValue,
                                  byte byteValue,
                                  boolean booleanValue,
                                  String extraColumn)
        {
            super(primaryKey, secondaryKey, orderedKey, value, intValue, longValue, doubleValue, shortValue, byteValue,
                  booleanValue);
        }
    }
}
