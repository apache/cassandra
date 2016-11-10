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

import org.junit.Test;

import org.apache.cassandra.db.composites.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.io.sstable.ColumnNameHelper;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.*;

public class ColumnSliceTest
{
    private static final CellNameType simpleIntType = new SimpleDenseCellNameType(Int32Type.instance);

    @Test
    public void testIntersectsSingleSlice()
    {
        List<AbstractType<?>> types = new ArrayList<>();
        types.add(Int32Type.instance);
        types.add(Int32Type.instance);
        types.add(Int32Type.instance);
        CompoundDenseCellNameType nameType = new CompoundDenseCellNameType(types);

        // filter falls entirely before sstable
        ColumnSlice slice = new ColumnSlice(composite(0, 0, 0), composite(1, 0, 0));
        assertFalse(slice.intersects(columnNames(2, 0, 0), columnNames(3, 0, 0), nameType, false));

        // same case, but with empty start
        slice = new ColumnSlice(composite(), composite(1, 0, 0));
        assertFalse(slice.intersects(columnNames(2, 0, 0), columnNames(3, 0, 0), nameType, false));

        // same case, but with missing components for start
        slice = new ColumnSlice(composite(0), composite(1, 0, 0));
        assertFalse(slice.intersects(columnNames(2, 0, 0), columnNames(3, 0, 0), nameType, false));

        // same case, but with missing components for start and end
        slice = new ColumnSlice(composite(0), composite(1, 0));
        assertFalse(slice.intersects(columnNames(2, 0, 0), columnNames(3, 0, 0), nameType, false));


        // end of slice matches start of sstable for the first component, but not the second component
        slice = new ColumnSlice(composite(0, 0, 0), composite(1, 0, 0));
        assertFalse(slice.intersects(columnNames(1, 1, 0), columnNames(3, 0, 0), nameType, false));

        // same case, but with missing components for start
        slice = new ColumnSlice(composite(0), composite(1, 0, 0));
        assertFalse(slice.intersects(columnNames(1, 1, 0), columnNames(3, 0, 0), nameType, false));

        // same case, but with missing components for start and end
        slice = new ColumnSlice(composite(0), composite(1, 0));
        assertFalse(slice.intersects(columnNames(1, 1, 0), columnNames(3, 0, 0), nameType, false));

        // first two components match, but not the last
        slice = new ColumnSlice(composite(0, 0, 0), composite(1, 1, 0));
        assertFalse(slice.intersects(columnNames(1, 1, 1), columnNames(3, 1, 1), nameType, false));

        // all three components in slice end match the start of the sstable
        slice = new ColumnSlice(composite(0, 0, 0), composite(1, 1, 1));
        assertTrue(slice.intersects(columnNames(1, 1, 1), columnNames(3, 1, 1), nameType, false));


        // filter falls entirely after sstable
        slice = new ColumnSlice(composite(4, 0, 0), composite(4, 0, 0));
        assertFalse(slice.intersects(columnNames(2, 0, 0), columnNames(3, 0, 0), nameType, false));

        // same case, but with empty end
        slice = new ColumnSlice(composite(4, 0, 0), composite());
        assertFalse(slice.intersects(columnNames(2, 0, 0), columnNames(3, 0, 0), nameType, false));

        // same case, but with missing components for end
        slice = new ColumnSlice(composite(4, 0, 0), composite(1));
        assertFalse(slice.intersects(columnNames(2, 0, 0), columnNames(3, 0, 0), nameType, false));

        // same case, but with missing components for start and end
        slice = new ColumnSlice(composite(4, 0), composite(1));
        assertFalse(slice.intersects(columnNames(2, 0, 0), columnNames(3, 0, 0), nameType, false));


        // start of slice matches end of sstable for the first component, but not the second component
        slice = new ColumnSlice(composite(1, 1, 1), composite(2, 0, 0));
        assertFalse(slice.intersects(columnNames(0, 0, 0), columnNames(1, 0, 0), nameType, false));

        // start of slice matches end of sstable for the first two components, but not the last component
        slice = new ColumnSlice(composite(1, 1, 1), composite(2, 0, 0));
        assertFalse(slice.intersects(columnNames(0, 0, 0), columnNames(1, 1, 0), nameType, false));

        // all three components in the slice start match the end of the sstable
        slice = new ColumnSlice(composite(1, 1, 1), composite(2, 0, 0));
        assertTrue(slice.intersects(columnNames(0, 0, 0), columnNames(1, 1, 1), nameType, false));


        // slice covers entire sstable (with no matching edges)
        slice = new ColumnSlice(composite(0, 0, 0), composite(2, 0, 0));
        assertTrue(slice.intersects(columnNames(1, 0, 0), columnNames(1, 1, 1), nameType, false));

        // same case, but with empty ends
        slice = new ColumnSlice(composite(), composite());
        assertTrue(slice.intersects(columnNames(1, 0, 0), columnNames(1, 1, 1), nameType, false));

        // same case, but with missing components
        slice = new ColumnSlice(composite(0), composite(2, 0));
        assertTrue(slice.intersects(columnNames(1, 0, 0), columnNames(1, 1, 1), nameType, false));

        // slice covers entire sstable (with matching start)
        slice = new ColumnSlice(composite(1, 0, 0), composite(2, 0, 0));
        assertTrue(slice.intersects(columnNames(1, 0, 0), columnNames(1, 1, 1), nameType, false));

        // slice covers entire sstable (with matching end)
        slice = new ColumnSlice(composite(0, 0, 0), composite(1, 1, 1));
        assertTrue(slice.intersects(columnNames(1, 0, 0), columnNames(1, 1, 1), nameType, false));

        // slice covers entire sstable (with matching start and end)
        slice = new ColumnSlice(composite(1, 0, 0), composite(1, 1, 1));
        assertTrue(slice.intersects(columnNames(1, 0, 0), columnNames(1, 1, 1), nameType, false));


        // slice falls entirely within sstable (with matching start)
        slice = new ColumnSlice(composite(1, 0, 0), composite(1, 1, 0));
        assertTrue(slice.intersects(columnNames(1, 0, 0), columnNames(1, 1, 1), nameType, false));

        // same case, but with a missing end component
        slice = new ColumnSlice(composite(1, 0, 0), composite(1, 1));
        assertTrue(slice.intersects(columnNames(1, 0, 0), columnNames(1, 1, 1), nameType, false));

        // slice falls entirely within sstable (with matching end)
        slice = new ColumnSlice(composite(1, 1, 0), composite(1, 1, 1));
        assertTrue(slice.intersects(columnNames(1, 0, 0), columnNames(1, 1, 1), nameType, false));

        // same case, but with a missing start component
        slice = new ColumnSlice(composite(1, 1), composite(1, 1, 1));
        assertTrue(slice.intersects(columnNames(1, 0, 0), columnNames(1, 1, 1), nameType, false));


        // slice falls entirely within sstable
        slice = new ColumnSlice(composite(1, 1, 0), composite(1, 1, 1));
        assertTrue(slice.intersects(columnNames(1, 0, 0), columnNames(2, 2, 2), nameType, false));

        // same case, but with a missing start component
        slice = new ColumnSlice(composite(1, 1), composite(1, 1, 1));
        assertTrue(slice.intersects(columnNames(1, 0, 0), columnNames(2, 2, 2), nameType, false));

        // same case, but with a missing start and end components
        slice = new ColumnSlice(composite(1), composite(1, 2));
        assertTrue(slice.intersects(columnNames(1, 0, 0), columnNames(2, 2, 2), nameType, false));

        // same case, but with an equal first component and missing start and end components
        slice = new ColumnSlice(composite(1), composite(1));
        assertTrue(slice.intersects(columnNames(1, 0, 0), columnNames(2, 2, 2), nameType, false));

        // slice falls entirely within sstable (slice start and end are the same)
        slice = new ColumnSlice(composite(1, 1, 1), composite(1, 1, 1));
        assertTrue(slice.intersects(columnNames(1, 0, 0), columnNames(2, 2, 2), nameType, false));


        // slice starts within sstable, empty end
        slice = new ColumnSlice(composite(1, 1, 1), composite());
        assertTrue(slice.intersects(columnNames(1, 0, 0), columnNames(2, 0, 0), nameType, false));

        // same case, but with missing end components
        slice = new ColumnSlice(composite(1, 1, 1), composite(3));
        assertTrue(slice.intersects(columnNames(1, 0, 0), columnNames(2, 0, 0), nameType, false));

        // slice starts within sstable (matching sstable start), empty end
        slice = new ColumnSlice(composite(1, 0, 0), composite());
        assertTrue(slice.intersects(columnNames(1, 0, 0), columnNames(2, 0, 0), nameType, false));

        // same case, but with missing end components
        slice = new ColumnSlice(composite(1, 0, 0), composite(3));
        assertTrue(slice.intersects(columnNames(1, 0, 0), columnNames(2, 0, 0), nameType, false));

        // slice starts within sstable (matching sstable end), empty end
        slice = new ColumnSlice(composite(2, 0, 0), composite());
        assertTrue(slice.intersects(columnNames(1, 0, 0), columnNames(2, 0, 0), nameType, false));

        // same case, but with missing end components
        slice = new ColumnSlice(composite(2, 0, 0), composite(3));
        assertTrue(slice.intersects(columnNames(1, 0, 0), columnNames(2, 0, 0), nameType, false));


        // slice ends within sstable, empty end
        slice = new ColumnSlice(composite(), composite(1, 1, 1));
        assertTrue(slice.intersects(columnNames(1, 0, 0), columnNames(2, 0, 0), nameType, false));

        // same case, but with missing start components
        slice = new ColumnSlice(composite(0), composite(1, 1, 1));
        assertTrue(slice.intersects(columnNames(1, 0, 0), columnNames(2, 0, 0), nameType, false));

        // slice ends within sstable (matching sstable start), empty start
        slice = new ColumnSlice(composite(), composite(1, 0, 0));
        assertTrue(slice.intersects(columnNames(1, 0, 0), columnNames(2, 0, 0), nameType, false));

        // same case, but with missing start components
        slice = new ColumnSlice(composite(0), composite(1, 0, 0));
        assertTrue(slice.intersects(columnNames(1, 0, 0), columnNames(2, 0, 0), nameType, false));

        // slice ends within sstable (matching sstable end), empty start
        slice = new ColumnSlice(composite(), composite(2, 0, 0));
        assertTrue(slice.intersects(columnNames(1, 0, 0), columnNames(2, 0, 0), nameType, false));

        // same case, but with missing start components
        slice = new ColumnSlice(composite(0), composite(2, 0, 0));
        assertTrue(slice.intersects(columnNames(1, 0, 0), columnNames(2, 0, 0), nameType, false));


        // the slice technically falls within the sstable range, but since the first component is restricted to
        // a single value, we can check that the second component does not fall within its min/max
        slice = new ColumnSlice(composite(1, 2, 0), composite(1, 3, 0));
        assertFalse(slice.intersects(columnNames(1, 0, 0), columnNames(2, 1, 0), nameType, false));

        // same case, but with a missing start component
        slice = new ColumnSlice(composite(1, 2), composite(1, 3, 0));
        assertFalse(slice.intersects(columnNames(1, 0, 0), columnNames(2, 1, 0), nameType, false));

        // same case, but with a missing end component
        slice = new ColumnSlice(composite(1, 2, 0), composite(1, 3));
        assertFalse(slice.intersects(columnNames(1, 0, 0), columnNames(2, 1, 0), nameType, false));

        // same case, but with a missing start and end components
        slice = new ColumnSlice(composite(1, 2), composite(1, 3));
        assertFalse(slice.intersects(columnNames(1, 0, 0), columnNames(2, 1, 0), nameType, false));

        // same case, but with missing start and end components and different lengths for start and end
        slice = new ColumnSlice(composite(1, 2), composite(1));
        assertFalse(slice.intersects(columnNames(1, 0, 0), columnNames(2, 1, 0), nameType, false));


        // same as the previous set of tests, but the second component is equal in the slice start and end
        slice = new ColumnSlice(composite(1, 2, 0), composite(1, 2, 0));
        assertFalse(slice.intersects(columnNames(1, 0, 0), columnNames(2, 1, 0), nameType, false));

        // same case, but with a missing start component
        slice = new ColumnSlice(composite(1, 2), composite(1, 2, 0));
        assertFalse(slice.intersects(columnNames(1, 0, 0), columnNames(2, 1, 0), nameType, false));

        // same case, but with a missing end component
        slice = new ColumnSlice(composite(1, 2, 0), composite(1, 2));
        assertFalse(slice.intersects(columnNames(1, 0, 0), columnNames(2, 1, 0), nameType, false));

        // same case, but with a missing start and end components
        slice = new ColumnSlice(composite(1, 2), composite(1, 2));
        assertFalse(slice.intersects(columnNames(1, 0, 0), columnNames(2, 1, 0), nameType, false));

        // same as the previous tests, but it's the third component that doesn't fit in its range this time
        slice = new ColumnSlice(composite(1, 1, 2), composite(1, 1, 3));
        assertFalse(slice.intersects(columnNames(1, 1, 0), columnNames(2, 2, 1), nameType, false));

        // empty min/max column names
        slice = new ColumnSlice(composite(), composite());
        assertTrue(slice.intersects(columnNames(), columnNames(), nameType, false));

        slice = new ColumnSlice(composite(1), composite());
        assertTrue(slice.intersects(columnNames(), columnNames(), nameType, false));

        slice = new ColumnSlice(composite(), composite(1));
        assertTrue(slice.intersects(columnNames(), columnNames(), nameType, false));

        slice = new ColumnSlice(composite(1), composite(1));
        assertTrue(slice.intersects(columnNames(), columnNames(), nameType, false));

        slice = new ColumnSlice(composite(), composite());
        assertTrue(slice.intersects(columnNames(), columnNames(1), nameType, false));

        slice = new ColumnSlice(composite(), composite(1));
        assertTrue(slice.intersects(columnNames(), columnNames(1), nameType, false));

        slice = new ColumnSlice(composite(), composite(1));
        assertTrue(slice.intersects(columnNames(), columnNames(2), nameType, false));

        slice = new ColumnSlice(composite(), composite(2));
        assertTrue(slice.intersects(columnNames(), columnNames(1), nameType, false));

        slice = new ColumnSlice(composite(2), composite(3));
        assertFalse(slice.intersects(columnNames(), columnNames(1), nameType, false));

        // basic check on reversed slices
        slice = new ColumnSlice(composite(1, 0, 0), composite(0, 0, 0));
        assertFalse(slice.intersects(columnNames(2, 0, 0), columnNames(3, 0, 0), nameType, true));

        slice = new ColumnSlice(composite(1, 0, 0), composite(0, 0, 0));
        assertFalse(slice.intersects(columnNames(1, 1, 0), columnNames(3, 0, 0), nameType, true));

        slice = new ColumnSlice(composite(1, 1, 1), composite(1, 1, 0));
        assertTrue(slice.intersects(columnNames(1, 0, 0), columnNames(2, 2, 2), nameType, true));
    }

    @Test
    public void testDifferentMinMaxLengths()
    {
        List<AbstractType<?>> types = new ArrayList<>();
        types.add(Int32Type.instance);
        types.add(Int32Type.instance);
        types.add(Int32Type.instance);
        CompoundDenseCellNameType nameType = new CompoundDenseCellNameType(types);

        // slice does intersect
        ColumnSlice slice = new ColumnSlice(composite(), composite());
        assertTrue(slice.intersects(columnNames(), columnNames(1), nameType, false));

        slice = new ColumnSlice(composite(), composite());
        assertTrue(slice.intersects(columnNames(1), columnNames(1, 2), nameType, false));

        slice = new ColumnSlice(composite(), composite(1));
        assertTrue(slice.intersects(columnNames(), columnNames(1), nameType, false));

        slice = new ColumnSlice(composite(1), composite());
        assertTrue(slice.intersects(columnNames(), columnNames(1), nameType, false));

        slice = new ColumnSlice(composite(1), composite(1));
        assertTrue(slice.intersects(columnNames(), columnNames(1), nameType, false));

        slice = new ColumnSlice(composite(0), composite(1, 2, 3));
        assertTrue(slice.intersects(columnNames(), columnNames(1), nameType, false));

        slice = new ColumnSlice(composite(1, 2, 3), composite(2));
        assertTrue(slice.intersects(columnNames(), columnNames(1), nameType, false));

        // slice does not intersect
        slice = new ColumnSlice(composite(2), composite(3, 4, 5));
        assertFalse(slice.intersects(columnNames(), columnNames(1), nameType, false));

        slice = new ColumnSlice(composite(0), composite(0, 1, 2));
        assertFalse(slice.intersects(columnNames(1), columnNames(1, 2), nameType, false));
    }
    @Test
    public void testColumnNameHelper()
    {
        List<AbstractType<?>> types = new ArrayList<>();
        types.add(Int32Type.instance);
        types.add(Int32Type.instance);
        types.add(Int32Type.instance);
        CompoundDenseCellNameType nameType = new CompoundDenseCellNameType(types);
        assertTrue(ColumnNameHelper.overlaps(columnNames(0, 0, 0), columnNames(3, 3, 3), columnNames(1, 1, 1), columnNames(2, 2, 2), nameType));
        assertFalse(ColumnNameHelper.overlaps(columnNames(0, 0, 0), columnNames(3, 3, 3), columnNames(4, 4, 4), columnNames(5, 5, 5), nameType));
        assertFalse(ColumnNameHelper.overlaps(columnNames(0, 0, 0), columnNames(3, 3, 3), columnNames(3, 3, 4), columnNames(5, 5, 5), nameType));
        assertTrue(ColumnNameHelper.overlaps(columnNames(0), columnNames(3, 3, 3), columnNames(1, 1), columnNames(5), nameType));
    }

    @Test
    public void testDeoverlapSlices()
    {
        ColumnSlice[] slices;
        ColumnSlice[] deoverlapped;

        // Preserve correct slices
        slices = slices(s(0, 3), s(4, 5), s(6, 9));
        assertSlicesValid(slices);
        assertSlicesEquals(slices, deoverlapSlices(slices));

        // Simple overlap
        slices = slices(s(0, 3), s(2, 5), s(8, 9));
        assertSlicesInvalid(slices);
        assertSlicesEquals(slices(s(0, 5), s(8, 9)), deoverlapSlices(slices));

        // Slice overlaps others fully
        slices = slices(s(0, 10), s(2, 5), s(8, 9));
        assertSlicesInvalid(slices);
        assertSlicesEquals(slices(s(0, 10)), deoverlapSlices(slices));

        // Slice with empty end overlaps others fully
        slices = slices(s(0, -1), s(2, 5), s(8, 9));
        assertSlicesInvalid(slices);
        assertSlicesEquals(slices(s(0, -1)), deoverlapSlices(slices));

        // Overlap with slices selecting only one element
        slices = slices(s(0, 4), s(4, 4), s(4, 8));
        assertSlicesInvalid(slices);
        assertSlicesEquals(slices(s(0, 8)), deoverlapSlices(slices));

        // Unordered slices (without overlap)
        slices = slices(s(4, 8), s(0, 3), s(9, 9));
        assertSlicesInvalid(slices);
        assertSlicesEquals(slices(s(0, 3), s(4, 8), s(9, 9)), deoverlapSlices(slices));

        // All range select but not by a single slice
        slices = slices(s(5, -1), s(2, 5), s(-1, 2));
        assertSlicesInvalid(slices);
        assertSlicesEquals(slices(s(-1, -1)), deoverlapSlices(slices));
    }

    @Test
    public void testValidateSlices()
    {
        assertSlicesValid(slices(s(0, 3)));
        assertSlicesValid(slices(s(3, 3)));
        assertSlicesValid(slices(s(3, 3), s(4, 4)));
        assertSlicesValid(slices(s(0, 3), s(4, 5), s(6, 9)));
        assertSlicesValid(slices(s(-1, -1)));
        assertSlicesValid(slices(s(-1, 3), s(4, -1)));

        assertSlicesInvalid(slices(s(3, 0)));
        assertSlicesInvalid(slices(s(0, 2), s(2, 4)));
        assertSlicesInvalid(slices(s(0, 2), s(1, 4)));
        assertSlicesInvalid(slices(s(0, 2), s(3, 4), s(3, 4)));
        assertSlicesInvalid(slices(s(-1, 2), s(3, -1), s(5, 9)));
    }

    private static Composite composite(Integer ... components)
    {
        List<AbstractType<?>> types = new ArrayList<>();
        types.add(Int32Type.instance);
        types.add(Int32Type.instance);
        types.add(Int32Type.instance);
        CompoundDenseCellNameType nameType = new CompoundDenseCellNameType(types);
        return nameType.make((Object[]) components);
    }

    private static List<ByteBuffer> columnNames(Integer ... components)
    {
        List<ByteBuffer> names = new ArrayList<>(components.length);
        for (int component : components)
            names.add(ByteBufferUtil.bytes(component));
        return names;
    }

    private static Composite simpleComposite(int i)
    {
        // We special negative values to mean EMPTY for convenience sake
        if (i < 0)
            return Composites.EMPTY;

        return simpleIntType.make(i);
    }

    private static ColumnSlice s(int start, int finish)
    {
        return new ColumnSlice(simpleComposite(start), simpleComposite(finish));
    }

    private static ColumnSlice[] slices(ColumnSlice... slices)
    {
        return slices;
    }

    private static ColumnSlice[] deoverlapSlices(ColumnSlice[] slices)
    {
        return ColumnSlice.deoverlapSlices(slices, simpleIntType);
    }

    private static void assertSlicesValid(ColumnSlice[] slices)
    {
        assertTrue("Slices " + toString(slices) + " should be valid", ColumnSlice.validateSlices(slices, simpleIntType, false));
    }

    private static void assertSlicesInvalid(ColumnSlice[] slices)
    {
        assertFalse("Slices " + toString(slices) + " shouldn't be valid", ColumnSlice.validateSlices(slices, simpleIntType, false));
    }

    private static void assertSlicesEquals(ColumnSlice[] expected, ColumnSlice[] actual)
    {
        assertTrue("Expected " + toString(expected) + " but got " + toString(actual), Arrays.equals(expected, actual));
    }

    private static String toString(ColumnSlice[] slices)
    {
        StringBuilder sb = new StringBuilder().append("[");
        for (int i = 0; i < slices.length; i++)
        {
            if (i > 0)
                sb.append(", ");

            ColumnSlice slice = slices[i];
            sb.append("(");
            sb.append(slice.start.isEmpty() ? "-1" : simpleIntType.getString(slice.start));
            sb.append(", ");
            sb.append(slice.finish.isEmpty() ? "-1" : simpleIntType.getString(slice.finish));
            sb.append(")");
        }
        return sb.append("]").toString();
    }
}
