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
package org.apache.cassandra.db.aggregation;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.Constants.Literal;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.VariableSpecifications;
import org.apache.cassandra.cql3.functions.ScalarFunction;
import org.apache.cassandra.cql3.functions.TimeFcts;
import org.apache.cassandra.cql3.selection.Selectable;
import org.apache.cassandra.cql3.selection.Selector;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class GroupMakerTest
{
    @BeforeClass
    public static void setupDD()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testIsNewGroupWithClusteringColumns()
    {
        ClusteringComparator comparator = newComparator(false, false, false);
        GroupMaker groupMaker = GroupMaker.newPkPrefixGroupMaker(comparator, 2);

        assertTrue(groupMaker.isNewGroup(partitionKey(1), clustering(1, 1, 1)));
        assertFalse(groupMaker.isNewGroup(partitionKey(1), clustering(1, 1, 2)));
        assertFalse(groupMaker.isNewGroup(partitionKey(1), clustering(1, 1, 3)));
        assertTrue(groupMaker.isNewGroup(partitionKey(1), clustering(1, 2, 1)));
        assertTrue(groupMaker.isNewGroup(partitionKey(1), clustering(1, 3, 1)));
        assertFalse(groupMaker.isNewGroup(partitionKey(1), clustering(1, 3, 2)));
        assertTrue(groupMaker.isNewGroup(partitionKey(1), clustering(2, 1, 1)));
        assertFalse(groupMaker.isNewGroup(partitionKey(1), clustering(2, 1, 2)));

        assertTrue(groupMaker.isNewGroup(partitionKey(2), clustering(2, 2, 1)));
    }

    @Test
    public void testIsNewGroupWithOneClusteringColumnsPrefix()
    {
        ClusteringComparator comparator = newComparator(false, false, false);
        GroupMaker groupMaker = GroupMaker.newPkPrefixGroupMaker(comparator, 1);

        assertTrue(groupMaker.isNewGroup(partitionKey(1), clustering(1, 1, 1)));
        assertFalse(groupMaker.isNewGroup(partitionKey(1), clustering(1, 1, 2)));
        assertFalse(groupMaker.isNewGroup(partitionKey(1), clustering(1, 1, 3)));
        assertFalse(groupMaker.isNewGroup(partitionKey(1), clustering(1, 2, 1)));
        assertFalse(groupMaker.isNewGroup(partitionKey(1), clustering(1, 3, 1)));
        assertFalse(groupMaker.isNewGroup(partitionKey(1), clustering(1, 3, 2)));
        assertTrue(groupMaker.isNewGroup(partitionKey(1), clustering(2, 1, 1)));
        assertFalse(groupMaker.isNewGroup(partitionKey(1), clustering(2, 1, 2)));

        assertTrue(groupMaker.isNewGroup(partitionKey(2), clustering(2, 2, 1)));
    }

    @Test
    public void testIsNewGroupWithReversedClusteringColumns()
    {
        ClusteringComparator comparator = newComparator(true, true, true);

        GroupMaker groupMaker = GroupMaker.newPkPrefixGroupMaker(comparator, 2);

        assertTrue(groupMaker.isNewGroup(partitionKey(1), clustering(1, 3, 2)));
        assertFalse(groupMaker.isNewGroup(partitionKey(1), clustering(1, 3, 1)));
        assertTrue(groupMaker.isNewGroup(partitionKey(1), clustering(1, 2, 1)));
        assertTrue(groupMaker.isNewGroup(partitionKey(1), clustering(1, 1, 3)));
        assertFalse(groupMaker.isNewGroup(partitionKey(1), clustering(1, 1, 2)));
        assertFalse(groupMaker.isNewGroup(partitionKey(1), clustering(1, 1, 1)));

        assertTrue(groupMaker.isNewGroup(partitionKey(1), clustering(2, 1, 2)));
        assertFalse(groupMaker.isNewGroup(partitionKey(1), clustering(2, 1, 1)));

        assertTrue(groupMaker.isNewGroup(partitionKey(2), clustering(2, 2, 1)));
    }

    @Test
    public void testIsNewGroupWithOneReversedClusteringColumns()
    {
        ClusteringComparator comparator = newComparator(true, false, false);

        GroupMaker groupMaker = GroupMaker.newPkPrefixGroupMaker(comparator, 2);

        assertTrue(groupMaker.isNewGroup(partitionKey(1), clustering(1, 3, 1)));
        assertFalse(groupMaker.isNewGroup(partitionKey(1), clustering(1, 3, 2)));
        assertTrue(groupMaker.isNewGroup(partitionKey(1), clustering(1, 2, 1)));
        assertTrue(groupMaker.isNewGroup(partitionKey(1), clustering(1, 1, 1)));
        assertFalse(groupMaker.isNewGroup(partitionKey(1), clustering(1, 1, 2)));
        assertFalse(groupMaker.isNewGroup(partitionKey(1), clustering(1, 1, 3)));

        assertTrue(groupMaker.isNewGroup(partitionKey(1), clustering(2, 1, 1)));
        assertFalse(groupMaker.isNewGroup(partitionKey(1), clustering(2, 1, 2)));

        assertTrue(groupMaker.isNewGroup(partitionKey(2), clustering(2, 2, 1)));
    }

    @Test
    public void testIsNewGroupWithStaticClusteringColumns()
    {
        ClusteringComparator comparator = newComparator(false, false, false);
        GroupMaker groupMaker = GroupMaker.newPkPrefixGroupMaker(comparator, 2);

        assertTrue(groupMaker.isNewGroup(partitionKey(1), clustering(1, 1, 1)));
        assertFalse(groupMaker.isNewGroup(partitionKey(1), clustering(1, 1, 2)));

        assertTrue(groupMaker.isNewGroup(partitionKey(2), Clustering.STATIC_CLUSTERING));
        assertTrue(groupMaker.isNewGroup(partitionKey(3), Clustering.STATIC_CLUSTERING));
        assertTrue(groupMaker.isNewGroup(partitionKey(4), clustering(1, 1, 2)));
    }

    @Test
    public void testIsNewGroupWithOnlyPartitionKeyComponents()
    {
        ClusteringComparator comparator = newComparator(false, false, false);
        GroupMaker goupMaker = GroupMaker.newPkPrefixGroupMaker(comparator, 2);

        assertTrue(goupMaker.isNewGroup(partitionKey(1, 1), clustering(1, 1, 1)));
        assertFalse(goupMaker.isNewGroup(partitionKey(1, 1), clustering(1, 1, 2)));

        assertTrue(goupMaker.isNewGroup(partitionKey(1, 2), clustering(1, 1, 2)));
        assertTrue(goupMaker.isNewGroup(partitionKey(1, 2), clustering(2, 2, 2)));

        assertTrue(goupMaker.isNewGroup(partitionKey(2, 2), clustering(1, 1, 2)));
    }

    @Test
    public void testIsNewGroupWithFunction()
    {
        GroupMaker groupMaker = newSelectorGroupMaker(false);

        assertTrue(groupMaker.isNewGroup(partitionKey(1), clustering("2016-09-27 16:10:00 UTC")));
        assertFalse(groupMaker.isNewGroup(partitionKey(1), clustering("2016-09-27 16:12:00 UTC")));
        assertFalse(groupMaker.isNewGroup(partitionKey(1), clustering("2016-09-27 16:14:00 UTC")));
        assertTrue(groupMaker.isNewGroup(partitionKey(1), clustering("2016-09-27 16:15:00 UTC")));
        assertTrue(groupMaker.isNewGroup(partitionKey(1), clustering("2016-09-27 16:21:00 UTC")));
        assertFalse(groupMaker.isNewGroup(partitionKey(1), clustering("2016-09-27 16:22:00 UTC")));
        assertTrue(groupMaker.isNewGroup(partitionKey(1), clustering("2016-09-27 16:26:00 UTC")));
        assertFalse(groupMaker.isNewGroup(partitionKey(1), clustering("2016-09-27 16:26:20 UTC")));

        assertTrue(groupMaker.isNewGroup(partitionKey(2), clustering("2016-09-27 16:26:20 UTC")));
        assertTrue(groupMaker.isNewGroup(partitionKey(2), clustering("2016-09-27 16:30:00 UTC")));
    }

    @Test
    public void testIsNewGroupWithFunctionAndReversedOrder()
    {
        GroupMaker groupMaker = newSelectorGroupMaker(true);

        assertTrue(groupMaker.isNewGroup(partitionKey(1), clustering("2016-09-27 16:26:20 UTC")));
        assertFalse(groupMaker.isNewGroup(partitionKey(1), clustering("2016-09-27 16:26:00 UTC")));
        assertTrue(groupMaker.isNewGroup(partitionKey(1), clustering("2016-09-27 16:22:00 UTC")));
        assertFalse(groupMaker.isNewGroup(partitionKey(1), clustering("2016-09-27 16:21:00 UTC")));
        assertTrue(groupMaker.isNewGroup(partitionKey(1), clustering("2016-09-27 16:15:00 UTC")));
        assertTrue(groupMaker.isNewGroup(partitionKey(1), clustering("2016-09-27 16:14:00 UTC")));
        assertFalse(groupMaker.isNewGroup(partitionKey(1), clustering("2016-09-27 16:12:00 UTC")));
        assertFalse(groupMaker.isNewGroup(partitionKey(1), clustering("2016-09-27 16:10:00 UTC")));

        assertTrue(groupMaker.isNewGroup(partitionKey(2), clustering("2016-09-27 16:30:00 UTC")));
        assertTrue(groupMaker.isNewGroup(partitionKey(2), clustering("2016-09-27 16:26:20 UTC")));
    }

    @Test
    public void testIsNewGroupWithFunctionWithStaticColumn()
    {
        GroupMaker groupMaker = newSelectorGroupMaker(false);

        assertTrue(groupMaker.isNewGroup(partitionKey(1), clustering("2016-09-27 16:10:00 UTC")));
        assertFalse(groupMaker.isNewGroup(partitionKey(1), clustering("2016-09-27 16:12:00 UTC")));
        assertFalse(groupMaker.isNewGroup(partitionKey(1), clustering("2016-09-27 16:14:00 UTC")));
        assertTrue(groupMaker.isNewGroup(partitionKey(1), clustering("2016-09-27 16:15:00 UTC")));
        assertTrue(groupMaker.isNewGroup(partitionKey(1), clustering("2016-09-27 16:21:00 UTC")));
        assertFalse(groupMaker.isNewGroup(partitionKey(1), clustering("2016-09-27 16:22:00 UTC")));
        assertTrue(groupMaker.isNewGroup(partitionKey(1), clustering("2016-09-27 16:26:00 UTC")));
        assertFalse(groupMaker.isNewGroup(partitionKey(1), clustering("2016-09-27 16:26:20 UTC")));

        assertTrue(groupMaker.isNewGroup(partitionKey(2), Clustering.STATIC_CLUSTERING));
        assertTrue(groupMaker.isNewGroup(partitionKey(3), Clustering.STATIC_CLUSTERING));
        assertTrue(groupMaker.isNewGroup(partitionKey(4), clustering("2016-09-27 16:26:20 UTC")));
        assertTrue(groupMaker.isNewGroup(partitionKey(4), clustering("2016-09-27 16:30:00 UTC")));
    }

    @Test
    public void testIsNewGroupWithFunctionAndReversedOrderWithStaticColumns()
    {
        GroupMaker groupMaker = newSelectorGroupMaker(true);

        assertTrue(groupMaker.isNewGroup(partitionKey(1), clustering("2016-09-27 16:26:20 UTC")));
        assertFalse(groupMaker.isNewGroup(partitionKey(1), clustering("2016-09-27 16:26:00 UTC")));
        assertTrue(groupMaker.isNewGroup(partitionKey(1), clustering("2016-09-27 16:22:00 UTC")));
        assertFalse(groupMaker.isNewGroup(partitionKey(1), clustering("2016-09-27 16:21:00 UTC")));
        assertTrue(groupMaker.isNewGroup(partitionKey(1), clustering("2016-09-27 16:15:00 UTC")));
        assertTrue(groupMaker.isNewGroup(partitionKey(1), clustering("2016-09-27 16:14:00 UTC")));
        assertFalse(groupMaker.isNewGroup(partitionKey(1), clustering("2016-09-27 16:12:00 UTC")));
        assertFalse(groupMaker.isNewGroup(partitionKey(1), clustering("2016-09-27 16:10:00 UTC")));

        assertTrue(groupMaker.isNewGroup(partitionKey(2), Clustering.STATIC_CLUSTERING));
        assertTrue(groupMaker.isNewGroup(partitionKey(3), Clustering.STATIC_CLUSTERING));
        assertTrue(groupMaker.isNewGroup(partitionKey(4), clustering("2016-09-27 16:30:00 UTC")));
        assertTrue(groupMaker.isNewGroup(partitionKey(4), clustering("2016-09-27 16:26:20 UTC")));
    }

    @Test
    public void testIsNewGroupWithPrefixAndFunction()
    {
        GroupMaker groupMaker = newSelectorGroupMaker(false, false);

        assertTrue(groupMaker.isNewGroup(partitionKey(1), clustering(1, "2016-09-27 16:10:00 UTC")));
        assertFalse(groupMaker.isNewGroup(partitionKey(1), clustering(1, "2016-09-27 16:12:00 UTC")));
        assertFalse(groupMaker.isNewGroup(partitionKey(1), clustering(1, "2016-09-27 16:14:00 UTC")));
        assertTrue(groupMaker.isNewGroup(partitionKey(1), clustering(1, "2016-09-27 16:15:00 UTC")));
        assertTrue(groupMaker.isNewGroup(partitionKey(1), clustering(2, "2016-09-27 16:16:00 UTC")));
        assertTrue(groupMaker.isNewGroup(partitionKey(1), clustering(2, "2016-09-27 16:22:00 UTC")));
        assertTrue(groupMaker.isNewGroup(partitionKey(1), clustering(2, "2016-09-27 16:26:00 UTC")));
        assertFalse(groupMaker.isNewGroup(partitionKey(1), clustering(2, "2016-09-27 16:26:20 UTC")));

        assertTrue(groupMaker.isNewGroup(partitionKey(2), clustering(1, "2016-09-27 16:26:20 UTC")));
        assertTrue(groupMaker.isNewGroup(partitionKey(2), clustering(1, "2016-09-27 16:30:00 UTC")));
    }

    private static DecoratedKey partitionKey(int... components)
    {
        ByteBuffer buffer = ByteBuffer.allocate(components.length * 4);
        for (int component : components)
        {
            buffer.putInt(component);
        }
        buffer.flip();
        return DatabaseDescriptor.getPartitioner().decorateKey(buffer);
    }

    private static Clustering<?> clustering(int... components)
    {
        return Clustering.make(toByteBufferArray(components));
    }

    private static Clustering<?> clustering(String timeComponent)
    {
        ByteBuffer buffer = TimestampType.instance.fromString(timeComponent);
        return Clustering.make(buffer);
    }

    private static Clustering<?> clustering(int component, String timeComponent)
    {
        ByteBuffer first = Int32Type.instance.decompose(component);
        ByteBuffer second = TimestampType.instance.fromString(timeComponent);
        return Clustering.make(first, second);
    }

    private static ByteBuffer[] toByteBufferArray(int[] values)
    {
        ByteBuffer[] buffers = new ByteBuffer[values.length];

        for (int i = 0; i < values.length; i++)
        {
            buffers[i] = Int32Type.instance.decompose(values[i]);
        }

        return buffers;
    }

    private static ClusteringComparator newComparator(boolean... reversed)
    {
        AbstractType<?>[] types = new AbstractType<?>[reversed.length];
        for (int i = 0, m = reversed.length; i < m; i++)
            types[i] = reversed[i] ? ReversedType.getInstance(Int32Type.instance) : Int32Type.instance;

        return new ClusteringComparator(types);
    }

    private GroupMaker newSelectorGroupMaker(boolean... reversed)
    {
        TableMetadata.Builder builder = TableMetadata.builder("keyspace", "test")
                                                     .addPartitionKeyColumn("partition_key", Int32Type.instance);

        int last = reversed.length - 1;
        for (int i = 0; i < reversed.length; i++)
        {
            AbstractType<?> type = i == last ? TimestampType.instance : Int32Type.instance;
            builder.addClusteringColumn("clustering" + i, reversed[i] ? ReversedType.getInstance(type) : type);
        }

        TableMetadata table = builder.build();

        ColumnMetadata column = table.getColumn(new ColumnIdentifier("clustering" + last, false));

        Selectable.WithTerm duration = new Selectable.WithTerm(Literal.duration("5m"));
        Selectable.WithTerm startTime = new Selectable.WithTerm(Literal.string("2016-09-27 16:00:00 UTC"));
        ScalarFunction function = TimeFcts.FloorTimestampFunction.newInstanceWithStartTimeArgument();

        Selectable.WithFunction selectable = new Selectable.WithFunction(function, Arrays.asList(column, duration, startTime));
        Selector.Factory factory = selectable.newSelectorFactory(table, null, new ArrayList<>(), VariableSpecifications.empty());
        Selector selector = factory.newInstance(QueryOptions.DEFAULT);

        return GroupMaker.newSelectorGroupMaker(table.comparator, reversed.length, selector, Collections.singletonList(column));
    }
}
