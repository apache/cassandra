/*
 * * Licensed to the Apache Software Foundation (ASF) under one
 * * or more contributor license agreements.  See the NOTICE file
 * * distributed with this work for additional information
 * * regarding copyright ownership.  The ASF licenses this file
 * * to you under the Apache License, Version 2.0 (the
 * * "License"); you may not use this file except in compliance
 * * with the License.  You may obtain a copy of the License at
 * *
 * *    http://www.apache.org/licenses/LICENSE-2.0
 * *
 * * Unless required by applicable law or agreed to in writing,
 * * software distributed under the License is distributed on an
 * * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * * KIND, either express or implied.  See the License for the
 * * specific language governing permissions and limitations
 * * under the License.
 * */
package org.apache.cassandra.db.filter;


import java.nio.ByteBuffer;
import java.util.*;
import java.util.List;

import org.apache.cassandra.db.*;
import org.junit.Test;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.db.ClusteringPrefix.Kind.*;
import static org.junit.Assert.*;

// TODO refactor this test - clustering comparator should be tested independently from slices intersections
// TODO each intersection test should be done in both directions as "intersects" relation is symmetric
public class SliceTest
{
    @Test
    public void testIntersectsSingleSlice()
    {
        List<AbstractType<?>> types = new ArrayList<>();
        types.add(Int32Type.instance);
        types.add(Int32Type.instance);
        types.add(Int32Type.instance);
        ClusteringComparator cc = new ClusteringComparator(types);

        ClusteringPrefix.Kind sk = INCL_START_BOUND;
        ClusteringPrefix.Kind ek = INCL_END_BOUND;

        // filter falls entirely before sstable
        Slice slice = Slice.make(makeBound(sk, 0, 0, 0), makeBound(ek, 1, 0, 0));
        assertFalse(slice.intersects(cc, Slice.make(makeBound(sk, 2, 0, 0), makeBound(ek, 3, 0, 0))));

        // same case, but with empty start
        slice = Slice.make(makeBound(sk), makeBound(ek, 1, 0, 0));
        assertFalse(slice.intersects(cc, Slice.make(makeBound(sk, 2, 0, 0), makeBound(ek, 3, 0, 0))));

        // same case, but with missing components for start
        slice = Slice.make(makeBound(sk, 0), makeBound(ek, 1, 0, 0));
        assertFalse(slice.intersects(cc, Slice.make(makeBound(sk, 2, 0, 0), makeBound(ek, 3, 0, 0))));

        // same case, but with missing components for start and end
        slice = Slice.make(makeBound(sk, 0), makeBound(ek, 1, 0));
        assertFalse(slice.intersects(cc, Slice.make(makeBound(sk, 2, 0, 0), makeBound(ek, 3, 0, 0))));


        // end of slice matches start of sstable for the first component, but not the second component
        slice = Slice.make(makeBound(sk, 0, 0, 0), makeBound(ek, 1, 0, 0));
        assertFalse(slice.intersects(cc, Slice.make(makeBound(sk, 1, 1, 0), makeBound(ek, 3, 0, 0))));

        // same case, but with missing components for start
        slice = Slice.make(makeBound(sk, 0), makeBound(ek, 1, 0, 0));
        assertFalse(slice.intersects(cc, Slice.make(makeBound(sk, 1, 1, 0), makeBound(ek, 3, 0, 0))));

        // same case, but with missing components for start and end
        slice = Slice.make(makeBound(sk, 0), makeBound(ek, 1, 0));
        assertFalse(slice.intersects(cc, Slice.make(makeBound(sk, 1, 1, 0), makeBound(ek, 3, 0, 0))));

        // first two components match, but not the last
        slice = Slice.make(makeBound(sk, 0, 0, 0), makeBound(ek, 1, 1, 0));
        assertFalse(slice.intersects(cc, Slice.make(makeBound(sk, 1, 1, 1), makeBound(ek, 3, 1, 1))));

        // all three components in slice end match the start of the sstable
        slice = Slice.make(makeBound(sk, 0, 0, 0), makeBound(ek, 1, 1, 1));
        assertTrue(slice.intersects(cc, Slice.make(makeBound(sk, 1, 1, 1), makeBound(ek, 3, 1, 1))));


        // filter falls entirely after sstable
        slice = Slice.make(makeBound(sk, 4, 0, 0), makeBound(ek, 4, 0, 0));
        assertFalse(slice.intersects(cc, Slice.make(makeBound(sk, 2, 0, 0), makeBound(ek, 3, 0, 0))));

        // same case, but with empty end
        slice = Slice.make(makeBound(sk, 4, 0, 0), makeBound(ek));
        assertFalse(slice.intersects(cc, Slice.make(makeBound(sk, 2, 0, 0), makeBound(ek, 3, 0, 0))));

        // same case, but with missing components for end
        slice = Slice.make(makeBound(sk, 4, 0, 0), makeBound(ek, 1));
        assertFalse(slice.intersects(cc, Slice.make(makeBound(sk, 2, 0, 0), makeBound(ek, 3, 0, 0))));

        // same case, but with missing components for start and end
        slice = Slice.make(makeBound(sk, 4, 0), makeBound(ek, 1));
        assertFalse(slice.intersects(cc, Slice.make(makeBound(sk, 2, 0, 0), makeBound(ek, 3, 0, 0))));


        // start of slice matches end of sstable for the first component, but not the second component
        slice = Slice.make(makeBound(sk, 1, 1, 1), makeBound(ek, 2, 0, 0));
        assertFalse(slice.intersects(cc, Slice.make(makeBound(sk, 0, 0, 0), makeBound(ek, 1, 0, 0))));

        // start of slice matches end of sstable for the first two components, but not the last component
        slice = Slice.make(makeBound(sk, 1, 1, 1), makeBound(ek, 2, 0, 0));
        assertFalse(slice.intersects(cc, Slice.make(makeBound(sk, 0, 0, 0), makeBound(ek, 1, 1, 0))));

        // all three components in the slice start match the end of the sstable
        slice = Slice.make(makeBound(sk, 1, 1, 1), makeBound(ek, 2, 0, 0));
        assertTrue(slice.intersects(cc, Slice.make(makeBound(sk, 0, 0, 0), makeBound(ek, 1, 1, 1))));


        // slice covers entire sstable (with no matching edges)
        slice = Slice.make(makeBound(sk, 0, 0, 0), makeBound(ek, 2, 0, 0));
        assertTrue(slice.intersects(cc, Slice.make(makeBound(sk, 1, 0, 0), makeBound(ek, 1, 1, 1))));

        // same case, but with empty ends
        slice = Slice.make(makeBound(sk), makeBound(ek));
        assertTrue(slice.intersects(cc, Slice.make(makeBound(sk, 1, 0, 0), makeBound(ek, 1, 1, 1))));

        // same case, but with missing components
        slice = Slice.make(makeBound(sk, 0), makeBound(ek, 2, 0));
        assertTrue(slice.intersects(cc, Slice.make(makeBound(sk, 1, 0, 0), makeBound(ek, 1, 1, 1))));

        // slice covers entire sstable (with matching start)
        slice = Slice.make(makeBound(sk, 1, 0, 0), makeBound(ek, 2, 0, 0));
        assertTrue(slice.intersects(cc, Slice.make(makeBound(sk, 1, 0, 0), makeBound(ek, 1, 1, 1))));

        // slice covers entire sstable (with matching end)
        slice = Slice.make(makeBound(sk, 0, 0, 0), makeBound(ek, 1, 1, 1));
        assertTrue(slice.intersects(cc, Slice.make(makeBound(sk, 1, 0, 0), makeBound(ek, 1, 1, 1))));

        // slice covers entire sstable (with matching start and end)
        slice = Slice.make(makeBound(sk, 1, 0, 0), makeBound(ek, 1, 1, 1));
        assertTrue(slice.intersects(cc, Slice.make(makeBound(sk, 1, 0, 0), makeBound(ek, 1, 1, 1))));


        // slice falls entirely within sstable (with matching start)
        slice = Slice.make(makeBound(sk, 1, 0, 0), makeBound(ek, 1, 1, 0));
        assertTrue(slice.intersects(cc, Slice.make(makeBound(sk, 1, 0, 0), makeBound(ek, 1, 1, 1))));

        // same case, but with a missing end component
        slice = Slice.make(makeBound(sk, 1, 0, 0), makeBound(ek, 1, 1));
        assertTrue(slice.intersects(cc, Slice.make(makeBound(sk, 1, 0, 0), makeBound(ek, 1, 1, 1))));

        // slice falls entirely within sstable (with matching end)
        slice = Slice.make(makeBound(sk, 1, 1, 0), makeBound(ek, 1, 1, 1));
        assertTrue(slice.intersects(cc, Slice.make(makeBound(sk, 1, 0, 0), makeBound(ek, 1, 1, 1))));

        // same case, but with a missing start component
        slice = Slice.make(makeBound(sk, 1, 1), makeBound(ek, 1, 1, 1));
        assertTrue(slice.intersects(cc, Slice.make(makeBound(sk, 1, 0, 0), makeBound(ek, 1, 1, 1))));


        // slice falls entirely within sstable
        slice = Slice.make(makeBound(sk, 1, 1, 0), makeBound(ek, 1, 1, 1));
        assertTrue(slice.intersects(cc, Slice.make(makeBound(sk, 1, 0, 0), makeBound(ek, 2, 2, 2))));

        // same case, but with a missing start component
        slice = Slice.make(makeBound(sk, 1, 1), makeBound(ek, 1, 1, 1));
        assertTrue(slice.intersects(cc, Slice.make(makeBound(sk, 1, 0, 0), makeBound(ek, 2, 2, 2))));

        // same case, but with a missing start and end components
        slice = Slice.make(makeBound(sk, 1), makeBound(ek, 1, 2));
        assertTrue(slice.intersects(cc, Slice.make(makeBound(sk, 1, 0, 0), makeBound(ek, 2, 2, 2))));

        // same case, but with an equal first component and missing start and end components
        slice = Slice.make(makeBound(sk, 1), makeBound(ek, 1));
        assertTrue(slice.intersects(cc, Slice.make(makeBound(sk, 1, 0, 0), makeBound(ek, 2, 2, 2))));

        // slice falls entirely within sstable (slice start and end are the same)
        slice = Slice.make(makeBound(sk, 1, 1, 1), makeBound(ek, 1, 1, 1));
        assertTrue(slice.intersects(cc, Slice.make(makeBound(sk, 1, 0, 0), makeBound(ek, 2, 2, 2))));


        // slice starts within sstable, empty end
        slice = Slice.make(makeBound(sk, 1, 1, 1), makeBound(ek));
        assertTrue(slice.intersects(cc, Slice.make(makeBound(sk, 1, 0, 0), makeBound(ek, 2, 0, 0))));

        // same case, but with missing end components
        slice = Slice.make(makeBound(sk, 1, 1, 1), makeBound(ek, 3));
        assertTrue(slice.intersects(cc, Slice.make(makeBound(sk, 1, 0, 0), makeBound(ek, 2, 0, 0))));

        // slice starts within sstable (matching sstable start), empty end
        slice = Slice.make(makeBound(sk, 1, 0, 0), makeBound(ek));
        assertTrue(slice.intersects(cc, Slice.make(makeBound(sk, 1, 0, 0), makeBound(ek, 2, 0, 0))));

        // same case, but with missing end components
        slice = Slice.make(makeBound(sk, 1, 0, 0), makeBound(ek, 3));
        assertTrue(slice.intersects(cc, Slice.make(makeBound(sk, 1, 0, 0), makeBound(ek, 2, 0, 0))));

        // slice starts within sstable (matching sstable end), empty end
        slice = Slice.make(makeBound(sk, 2, 0, 0), makeBound(ek));
        assertTrue(slice.intersects(cc, Slice.make(makeBound(sk, 1, 0, 0), makeBound(ek, 2, 0, 0))));

        // same case, but with missing end components
        slice = Slice.make(makeBound(sk, 2, 0, 0), makeBound(ek, 3));
        assertTrue(slice.intersects(cc, Slice.make(makeBound(sk, 1, 0, 0), makeBound(ek, 2, 0, 0))));


        // slice ends within sstable, empty end
        slice = Slice.make(makeBound(sk), makeBound(ek, 1, 1, 1));
        assertTrue(slice.intersects(cc, Slice.make(makeBound(sk, 1, 0, 0), makeBound(ek, 2, 0, 0))));

        // same case, but with missing start components
        slice = Slice.make(makeBound(sk, 0), makeBound(ek, 1, 1, 1));
        assertTrue(slice.intersects(cc, Slice.make(makeBound(sk, 1, 0, 0), makeBound(ek, 2, 0, 0))));

        // slice ends within sstable (matching sstable start), empty start
        slice = Slice.make(makeBound(sk), makeBound(ek, 1, 0, 0));
        assertTrue(slice.intersects(cc, Slice.make(makeBound(sk, 1, 0, 0), makeBound(ek, 2, 0, 0))));

        // same case, but with missing start components
        slice = Slice.make(makeBound(sk, 0), makeBound(ek, 1, 0, 0));
        assertTrue(slice.intersects(cc, Slice.make(makeBound(sk, 1, 0, 0), makeBound(ek, 2, 0, 0))));

        // slice ends within sstable (matching sstable end), empty start
        slice = Slice.make(makeBound(sk), makeBound(ek, 2, 0, 0));
        assertTrue(slice.intersects(cc, Slice.make(makeBound(sk, 1, 0, 0), makeBound(ek, 2, 0, 0))));

        // same case, but with missing start components
        slice = Slice.make(makeBound(sk, 0), makeBound(ek, 2, 0, 0));
        assertTrue(slice.intersects(cc, Slice.make(makeBound(sk, 1, 0, 0), makeBound(ek, 2, 0, 0))));

        // empty min/max column names
        slice = Slice.make(makeBound(sk), makeBound(ek));
        assertTrue(slice.intersects(cc, Slice.make(makeBound(sk), makeBound(ek))));

        slice = Slice.make(makeBound(sk, 1), makeBound(ek));
        assertTrue(slice.intersects(cc, Slice.make(makeBound(sk), makeBound(ek))));

        slice = Slice.make(makeBound(sk), makeBound(ek, 1));
        assertTrue(slice.intersects(cc, Slice.make(makeBound(sk), makeBound(ek))));

        slice = Slice.make(makeBound(sk, 1), makeBound(ek, 1));
        assertTrue(slice.intersects(cc, Slice.make(makeBound(sk), makeBound(ek))));

        slice = Slice.make(makeBound(sk), makeBound(ek));
        assertTrue(slice.intersects(cc, Slice.make(makeBound(sk), makeBound(ek, 1))));

        slice = Slice.make(makeBound(sk), makeBound(ek, 1));
        assertTrue(slice.intersects(cc, Slice.make(makeBound(sk), makeBound(ek, 1))));

        slice = Slice.make(makeBound(sk), makeBound(ek, 1));
        assertTrue(slice.intersects(cc, Slice.make(makeBound(sk), makeBound(ek, 2))));

        slice = Slice.make(makeBound(sk), makeBound(ek, 2));
        assertTrue(slice.intersects(cc, Slice.make(makeBound(sk), makeBound(ek, 1))));

        slice = Slice.make(makeBound(sk, 2), makeBound(ek, 3));
        assertFalse(slice.intersects(cc, Slice.make(makeBound(sk), makeBound(ek, 1))));

        // basic check on reversed slices
        slice = Slice.make(makeBound(sk, 1, 0, 0), makeBound(ek, 0, 0, 0));
        assertFalse(slice.intersects(cc, Slice.make(makeBound(sk, 2, 0, 0), makeBound(ek, 3, 0, 0))));

        slice = Slice.make(makeBound(sk, 1, 0, 0), makeBound(ek, 0, 0, 0));
        assertFalse(slice.intersects(cc, Slice.make(makeBound(sk, 1, 1, 0), makeBound(ek, 3, 0, 0))));

        slice = Slice.make(makeBound(sk, 1, 1, 1), makeBound(ek, 1, 1, 0));
        assertFalse(slice.intersects(cc, Slice.make(makeBound(sk, 1, 0, 0), makeBound(ek, 2, 2, 2))));
    }

    @Test
    public void testDifferentMinMaxLengths()
    {
        List<AbstractType<?>> types = new ArrayList<>();
        types.add(Int32Type.instance);
        types.add(Int32Type.instance);
        types.add(Int32Type.instance);
        ClusteringComparator cc = new ClusteringComparator(types);

        ClusteringPrefix.Kind sk = INCL_START_BOUND;
        ClusteringPrefix.Kind ek = INCL_END_BOUND;

        // slice does intersect
        Slice slice = Slice.make(makeBound(sk), makeBound(ek));
        assertTrue(slice.intersects(cc, Slice.make(makeBound(sk), makeBound(ek, 1))));

        slice = Slice.make(makeBound(sk), makeBound(ek));
        assertTrue(slice.intersects(cc, Slice.make(makeBound(sk, 1), makeBound(ek, 1, 2))));

        slice = Slice.make(makeBound(sk), makeBound(ek, 1));
        assertTrue(slice.intersects(cc, Slice.make(makeBound(sk), makeBound(ek, 1))));

        slice = Slice.make(makeBound(sk, 1), makeBound(ek));
        assertTrue(slice.intersects(cc, Slice.make(makeBound(sk), makeBound(ek, 1))));

        slice = Slice.make(makeBound(sk, 1), makeBound(ek, 1));
        assertTrue(slice.intersects(cc, Slice.make(makeBound(sk), makeBound(ek, 1))));

        slice = Slice.make(makeBound(sk, 0), makeBound(ek, 1, 2, 3));
        assertTrue(slice.intersects(cc, Slice.make(makeBound(sk), makeBound(ek, 1))));

        slice = Slice.make(makeBound(sk, 1, 2, 3), makeBound(ek, 2));
        assertTrue(slice.intersects(cc, Slice.make(makeBound(sk), makeBound(ek, 1))));

        // slice does not intersect
        slice = Slice.make(makeBound(sk, 2), makeBound(ek, 3, 4, 5));
        assertFalse(slice.intersects(cc, Slice.make(makeBound(sk), makeBound(ek, 1))));

        slice = Slice.make(makeBound(sk, 0), makeBound(ek, 0, 1, 2));
        assertFalse(slice.intersects(cc, Slice.make(makeBound(sk, 1), makeBound(ek, 1, 2))));
    }

    @Test
    public void testSliceNormalization()
    {
        List<AbstractType<?>> types = new ArrayList<>();
        types.add(Int32Type.instance);
        types.add(Int32Type.instance);
        types.add(Int32Type.instance);
        ClusteringComparator cc = new ClusteringComparator(types);

        assertSlicesNormalization(cc, slices(s(0, 2), s(2, 4)), slices(s(0, 4)));
        assertSlicesNormalization(cc, slices(s(0, 2), s(1, 4)), slices(s(0, 4)));
        assertSlicesNormalization(cc, slices(s(0, 2), s(3, 4), s(3, 4)), slices(s(0, 2), s(3, 4)));
        assertSlicesNormalization(cc, slices(s(-1, 3), s(-1, 4)), slices(s(-1, 4)));
        assertSlicesNormalization(cc, slices(s(-1, 2), s(-1, 3), s(5, 9)), slices(s(-1, 3), s(5, 9)));
    }

    @Test
    public void testIsEmpty()
    {
        List<AbstractType<?>> types = new ArrayList<>();
        types.add(Int32Type.instance);
        types.add(Int32Type.instance);
        ClusteringComparator cc = new ClusteringComparator(types);

        assertFalse(Slice.isEmpty(cc, makeBound(INCL_START_BOUND, 5, 0), makeBound(INCL_END_BOUND, 5, 0)));
        assertFalse(Slice.isEmpty(cc, makeBound(INCL_START_BOUND, 5, 0), makeBound(EXCL_END_BOUND, 5, 1)));
        assertFalse(Slice.isEmpty(cc, makeBound(INCL_START_BOUND, 5), makeBound(EXCL_END_BOUND, 5, 1)));

        assertTrue(Slice.isEmpty(cc, makeBound(EXCL_START_BOUND, 5), makeBound(EXCL_END_BOUND, 5)));
        assertTrue(Slice.isEmpty(cc, makeBound(EXCL_START_BOUND, 5), makeBound(EXCL_END_BOUND, 5, 1)));
        assertTrue(Slice.isEmpty(cc, makeBound(EXCL_START_BOUND, 5, 1), makeBound(EXCL_END_BOUND, 5, 1)));
        assertTrue(Slice.isEmpty(cc, makeBound(INCL_START_BOUND, 5, 0), makeBound(INCL_END_BOUND, 4, 0)));
        assertTrue(Slice.isEmpty(cc, makeBound(INCL_START_BOUND, 5, 0), makeBound(EXCL_END_BOUND, 5)));
        assertTrue(Slice.isEmpty(cc, makeBound(INCL_START_BOUND, 5, 0), makeBound(EXCL_END_BOUND, 3, 0)));
    }

    private static ClusteringBound<?> makeBound(ClusteringPrefix.Kind kind, Integer... components)
    {
        ByteBuffer[] values = new ByteBuffer[components.length];
        for (int i = 0; i < components.length; i++)
        {
            values[i] = ByteBufferUtil.bytes(components[i]);
        }
        return BufferClusteringBound.create(kind, values);
    }

    private static List<ByteBuffer> columnNames(Integer ... components)
    {
        List<ByteBuffer> names = new ArrayList<>(components.length);
        for (int component : components)
            names.add(ByteBufferUtil.bytes(component));
        return names;
    }

    private static Slice s(int start, int finish)
    {
        return Slice.make(makeBound(INCL_START_BOUND, start),
                          makeBound(INCL_END_BOUND, finish));
    }

    private Slice[] slices(Slice... slices)
    {
        return slices;
    }

    private static void assertSlicesNormalization(ClusteringComparator cc, Slice[] original, Slice[] expected)
    {
        Slices.Builder builder = new Slices.Builder(cc);
        for (Slice s : original)
            builder.add(s);
        Slices slices = builder.build();
        assertEquals(expected.length, slices.size());
        for (int i = 0; i < expected.length; i++)
            assertEquals(expected[i], slices.get(i));
    }
}
