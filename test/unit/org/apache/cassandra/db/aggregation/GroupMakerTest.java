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

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.ReversedType;

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
        GroupMaker groupMaker = GroupMaker.newInstance(comparator, 2);

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
        GroupMaker groupMaker = GroupMaker.newInstance(comparator, 1);

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

        GroupMaker groupMaker = GroupMaker.newInstance(comparator, 2);

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

        GroupMaker groupMaker = GroupMaker.newInstance(comparator, 2);

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
        GroupMaker groupMaker = GroupMaker.newInstance(comparator, 2);

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
        GroupMaker goupMaker = GroupMaker.newInstance(comparator, 2);

        assertTrue(goupMaker.isNewGroup(partitionKey(1, 1), clustering(1, 1, 1)));
        assertFalse(goupMaker.isNewGroup(partitionKey(1, 1), clustering(1, 1, 2)));

        assertTrue(goupMaker.isNewGroup(partitionKey(1, 2), clustering(1, 1, 2)));
        assertTrue(goupMaker.isNewGroup(partitionKey(1, 2), clustering(2, 2, 2)));

        assertTrue(goupMaker.isNewGroup(partitionKey(2, 2), clustering(1, 1, 2)));
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

    private static Clustering clustering(int... components)
    {
        return Clustering.make(toByteBufferArray(components));
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
}
