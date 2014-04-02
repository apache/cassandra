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

import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.db.composites.CompoundDenseCellNameType;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ColumnSliceTest
{
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


        // basic check on reversed slices
        slice = new ColumnSlice(composite(1, 0, 0), composite(0, 0, 0));
        assertFalse(slice.intersects(columnNames(2, 0, 0), columnNames(3, 0, 0), nameType, true));

        slice = new ColumnSlice(composite(1, 0, 0), composite(0, 0, 0));
        assertFalse(slice.intersects(columnNames(1, 1, 0), columnNames(3, 0, 0), nameType, true));

        slice = new ColumnSlice(composite(1, 1, 1), composite(1, 1, 0));
        assertTrue(slice.intersects(columnNames(1, 0, 0), columnNames(2, 2, 2), nameType, true));
    }

    private static Composite composite(Integer ... components)
    {
        List<AbstractType<?>> types = new ArrayList<>();
        types.add(Int32Type.instance);
        types.add(Int32Type.instance);
        types.add(Int32Type.instance);
        CompoundDenseCellNameType nameType = new CompoundDenseCellNameType(types);
        return nameType.make(components);
    }

    private static List<ByteBuffer> columnNames(Integer ... components)
    {
        List<ByteBuffer> names = new ArrayList<>(components.length);
        for (int component : components)
            names.add(ByteBufferUtil.bytes(component));
        return names;
    }
}
