/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.db.marshal;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.db.filter.SliceQueryFilter;
import org.apache.cassandra.serializers.MarshalException;
import org.junit.Test;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.utils.*;

public class CompositeTypeTest extends SchemaLoader
{
    private static final String cfName = "StandardComposite";
    private static final CompositeType comparator;
    static
    {
        List<AbstractType<?>> subComparators = new ArrayList<AbstractType<?>>();
        subComparators.add(BytesType.instance);
        subComparators.add(TimeUUIDType.instance);
        subComparators.add(IntegerType.instance);
        comparator = CompositeType.getInstance(subComparators);
    }

    private static final int UUID_COUNT = 3;
    private static final UUID[] uuids = new UUID[UUID_COUNT];
    static
    {
        for (int i = 0; i < UUID_COUNT; ++i)
            uuids[i] = UUIDGen.getTimeUUID();
    }

    @Test
    public void testEndOfComponent()
    {
        ByteBuffer[] cnames = {
            createCompositeKey("test1", uuids[0], -1, false),
            createCompositeKey("test1", uuids[1], 24, false),
            createCompositeKey("test1", uuids[1], 42, false),
            createCompositeKey("test1", uuids[1], 83, false),
            createCompositeKey("test1", uuids[2], -1, false),
            createCompositeKey("test1", uuids[2], 42, false),
        };

        ByteBuffer start = createCompositeKey("test1", uuids[1], -1, false);
        ByteBuffer stop = createCompositeKey("test1", uuids[1], -1, true);

        for (int i = 0; i < 1; ++i)
        {
            assert comparator.compare(start, cnames[i]) > 0;
            assert comparator.compare(stop, cnames[i]) > 0;
        }
        for (int i = 1; i < 4; ++i)
        {
            assert comparator.compare(start, cnames[i]) < 0;
            assert comparator.compare(stop, cnames[i]) > 0;
        }
        for (int i = 4; i < cnames.length; ++i)
        {
            assert comparator.compare(start, cnames[i]) < 0;
            assert comparator.compare(stop, cnames[i]) < 0;
        }
    }

    @Test
    public void testGetString()
    {
        String test1Hex = ByteBufferUtil.bytesToHex(ByteBufferUtil.bytes("test1"));
        ByteBuffer key = createCompositeKey("test1", uuids[1], 42, false);
        assert comparator.getString(key).equals(test1Hex + ":" + uuids[1] + ":42");

        key = createCompositeKey("test1", uuids[1], -1, true);
        assert comparator.getString(key).equals(test1Hex + ":" + uuids[1] + ":!");
    }

    @Test
    public void testFromString()
    {
        String test1Hex = ByteBufferUtil.bytesToHex(ByteBufferUtil.bytes("test1"));
        ByteBuffer key = createCompositeKey("test1", uuids[1], 42, false);
        assert key.equals(comparator.fromString(test1Hex + ":" + uuids[1] + ":42"));

        key = createCompositeKey("test1", uuids[1], -1, true);
        assert key.equals(comparator.fromString(test1Hex + ":" + uuids[1] + ":!"));
    }

    @Test
    public void testValidate()
    {
        ByteBuffer key = createCompositeKey("test1", uuids[1], 42, false);
        comparator.validate(key);

        key = createCompositeKey("test1", null, -1, false);
        comparator.validate(key);

        key = createCompositeKey("test1", uuids[2], -1, true);
        comparator.validate(key);

        key.get(); // make sure we're not aligned anymore
        try
        {
            comparator.validate(key);
            fail("Should not validate");
        }
        catch (MarshalException e) {}

        key = ByteBuffer.allocate(3 + "test1".length() + 3 + 14);
        key.putShort((short) "test1".length());
        key.put(ByteBufferUtil.bytes("test1"));
        key.put((byte) 0);
        key.putShort((short) 14);
        key.rewind();
        try
        {
            comparator.validate(key);
            fail("Should not validate");
        }
        catch (MarshalException e)
        {
            assert e.toString().contains("should be 16 or 0 bytes");
        }

        key = createCompositeKey("test1", UUID.randomUUID(), 42, false);
        try
        {
            comparator.validate(key);
            fail("Should not validate");
        }
        catch (MarshalException e)
        {
            assert e.toString().contains("Invalid version for TimeUUID type");
        }
    }

    @Test
    public void testFullRound() throws Exception
    {
        Keyspace keyspace = Keyspace.open("Keyspace1");
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfName);

        ByteBuffer cname1 = createCompositeKey("test1", null, -1, false);
        ByteBuffer cname2 = createCompositeKey("test1", uuids[0], 24, false);
        ByteBuffer cname3 = createCompositeKey("test1", uuids[0], 42, false);
        ByteBuffer cname4 = createCompositeKey("test2", uuids[0], -1, false);
        ByteBuffer cname5 = createCompositeKey("test2", uuids[1], 42, false);

        ByteBuffer key = ByteBufferUtil.bytes("k");
        RowMutation rm = new RowMutation("Keyspace1", key);
        addColumn(rm, cname5);
        addColumn(rm, cname1);
        addColumn(rm, cname4);
        addColumn(rm, cname2);
        addColumn(rm, cname3);
        rm.apply();

        ColumnFamily cf = cfs.getColumnFamily(QueryFilter.getIdentityFilter(Util.dk("k"), cfName, System.currentTimeMillis()));

        Iterator<Column> iter = cf.getSortedColumns().iterator();

        assert iter.next().name().equals(cname1);
        assert iter.next().name().equals(cname2);
        assert iter.next().name().equals(cname3);
        assert iter.next().name().equals(cname4);
        assert iter.next().name().equals(cname5);
    }

    @Test
    public void testEmptyParametersNotallowed()
    {
        try
        {
            TypeParser.parse("CompositeType");
            fail("Shouldn't work");
        }
        catch (ConfigurationException e) {}
        catch (SyntaxException e) {}

        try
        {
            TypeParser.parse("CompositeType()");
            fail("Shouldn't work");
        }
        catch (ConfigurationException e) {}
        catch (SyntaxException e) {}
    }

    @Test
    public void testCompatibility() throws Exception
    {
        assert TypeParser.parse("CompositeType(IntegerType, BytesType)").isCompatibleWith(TypeParser.parse("CompositeType(IntegerType)"));
        assert TypeParser.parse("CompositeType(IntegerType, BytesType)").isCompatibleWith(TypeParser.parse("CompositeType(IntegerType, BytesType)"));
        assert TypeParser.parse("CompositeType(BytesType, BytesType)").isCompatibleWith(TypeParser.parse("CompositeType(AsciiType, BytesType)"));

        assert !TypeParser.parse("CompositeType(IntegerType)").isCompatibleWith(TypeParser.parse("CompositeType(IntegerType, BytesType)"));
        assert !TypeParser.parse("CompositeType(IntegerType)").isCompatibleWith(TypeParser.parse("CompositeType(BytesType)"));
    }

    @Test
    public void testEscapeUnescape()
    {
        List<AbstractType<?>> subComparators = new ArrayList<AbstractType<?>>(){{;
            add(UTF8Type.instance);
            add(UTF8Type.instance);
        }};
        CompositeType comp = CompositeType.getInstance(subComparators);

        String[][] inputs = new String[][]{
            new String[]{ "foo", "bar" },
            new String[]{ "", "" },
            new String[]{ "foo\\", "bar" },
            new String[]{ "foo\\:", "bar" },
            new String[]{ "foo:", "bar" },
            new String[]{ "foo", "b:ar" },
            new String[]{ "foo!", "b:ar" },
        };

        for (String[] input : inputs)
        {
            CompositeType.Builder builder = new CompositeType.Builder(comp);
            for (String part : input)
                builder.add(UTF8Type.instance.fromString(part));

            ByteBuffer value = comp.fromString(comp.getString(builder.build()));
            ByteBuffer[] splitted = comp.split(value);
            for (int i = 0; i < splitted.length; i++)
                assertEquals(input[i], UTF8Type.instance.getString(splitted[i]));
        }
    }

    @Test
    public void testIntersectsSingleSlice()
    {
        CompositeType comparator = CompositeType.getInstance(Int32Type.instance, Int32Type.instance, Int32Type.instance);

        // filter falls entirely before sstable
        SliceQueryFilter filter = new SliceQueryFilter(composite(0, 0, 0), composite(1, 0, 0), false, 1);
        assertFalse(comparator.intersects(columnNames(2, 0, 0), columnNames(3, 0, 0), filter));

        // same case, but with empty start
        filter = new SliceQueryFilter(ByteBufferUtil.EMPTY_BYTE_BUFFER, composite(1, 0, 0), false, 1);
        assertFalse(comparator.intersects(columnNames(2, 0, 0), columnNames(3, 0, 0), filter));

        // same case, but with missing components for start
        filter = new SliceQueryFilter(composite(0), composite(1, 0, 0), false, 1);
        assertFalse(comparator.intersects(columnNames(2, 0, 0), columnNames(3, 0, 0), filter));

        // same case, but with missing components for start and end
        filter = new SliceQueryFilter(composite(0), composite(1, 0), false, 1);
        assertFalse(comparator.intersects(columnNames(2, 0, 0), columnNames(3, 0, 0), filter));


        // end of slice matches start of sstable for the first component, but not the second component
        filter = new SliceQueryFilter(composite(0, 0, 0), composite(1, 0, 0), false, 1);
        assertFalse(comparator.intersects(columnNames(1, 1, 0), columnNames(3, 0, 0), filter));

        // same case, but with missing components for start
        filter = new SliceQueryFilter(composite(0), composite(1, 0, 0), false, 1);
        assertFalse(comparator.intersects(columnNames(1, 1, 0), columnNames(3, 0, 0), filter));

        // same case, but with missing components for start and end
        filter = new SliceQueryFilter(composite(0), composite(1, 0), false, 1);
        assertFalse(comparator.intersects(columnNames(1, 1, 0), columnNames(3, 0, 0), filter));

        // first two components match, but not the last
        filter = new SliceQueryFilter(composite(0, 0, 0), composite(1, 1, 0), false, 1);
        assertFalse(comparator.intersects(columnNames(1, 1, 1), columnNames(3, 1, 1), filter));

        // all three components in slice end match the start of the sstable
        filter = new SliceQueryFilter(composite(0, 0, 0), composite(1, 1, 1), false, 1);
        assertTrue(comparator.intersects(columnNames(1, 1, 1), columnNames(3, 1, 1), filter));


        // filter falls entirely after sstable
        filter = new SliceQueryFilter(composite(4, 0, 0), composite(4, 0, 0), false, 1);
        assertFalse(comparator.intersects(columnNames(2, 0, 0), columnNames(3, 0, 0), filter));

        // same case, but with empty end
        filter = new SliceQueryFilter(composite(4, 0, 0), ByteBufferUtil.EMPTY_BYTE_BUFFER, false, 1);
        assertFalse(comparator.intersects(columnNames(2, 0, 0), columnNames(3, 0, 0), filter));

        // same case, but with missing components for end
        filter = new SliceQueryFilter(composite(4, 0, 0), composite(1), false, 1);
        assertFalse(comparator.intersects(columnNames(2, 0, 0), columnNames(3, 0, 0), filter));

        // same case, but with missing components for start and end
        filter = new SliceQueryFilter(composite(4, 0), composite(1), false, 1);
        assertFalse(comparator.intersects(columnNames(2, 0, 0), columnNames(3, 0, 0), filter));


        // start of slice matches end of sstable for the first component, but not the second component
        filter = new SliceQueryFilter(composite(1, 1, 1), composite(2, 0, 0), false, 1);
        assertFalse(comparator.intersects(columnNames(0, 0, 0), columnNames(1, 0, 0), filter));

        // start of slice matches end of sstable for the first two components, but not the last component
        filter = new SliceQueryFilter(composite(1, 1, 1), composite(2, 0, 0), false, 1);
        assertFalse(comparator.intersects(columnNames(0, 0, 0), columnNames(1, 1, 0), filter));

        // all three components in the slice start match the end of the sstable
        filter = new SliceQueryFilter(composite(1, 1, 1), composite(2, 0, 0), false, 1);
        assertTrue(comparator.intersects(columnNames(0, 0, 0), columnNames(1, 1, 1), filter));


        // slice covers entire sstable (with no matching edges)
        filter = new SliceQueryFilter(composite(0, 0, 0), composite(2, 0, 0), false, 1);
        assertTrue(comparator.intersects(columnNames(1, 0, 0), columnNames(1, 1, 1), filter));

        // same case, but with empty ends
        filter = new SliceQueryFilter(ByteBufferUtil.EMPTY_BYTE_BUFFER, ByteBufferUtil.EMPTY_BYTE_BUFFER, false, 1);
        assertTrue(comparator.intersects(columnNames(1, 0, 0), columnNames(1, 1, 1), filter));

        // same case, but with missing components
        filter = new SliceQueryFilter(composite(0), composite(2, 0), false, 1);
        assertTrue(comparator.intersects(columnNames(1, 0, 0), columnNames(1, 1, 1), filter));

        // slice covers entire sstable (with matching start)
        filter = new SliceQueryFilter(composite(1, 0, 0), composite(2, 0, 0), false, 1);
        assertTrue(comparator.intersects(columnNames(1, 0, 0), columnNames(1, 1, 1), filter));

        // slice covers entire sstable (with matching end)
        filter = new SliceQueryFilter(composite(0, 0, 0), composite(1, 1, 1), false, 1);
        assertTrue(comparator.intersects(columnNames(1, 0, 0), columnNames(1, 1, 1), filter));

        // slice covers entire sstable (with matching start and end)
        filter = new SliceQueryFilter(composite(1, 0, 0), composite(1, 1, 1), false, 1);
        assertTrue(comparator.intersects(columnNames(1, 0, 0), columnNames(1, 1, 1), filter));


        // slice falls entirely within sstable (with matching start)
        filter = new SliceQueryFilter(composite(1, 0, 0), composite(1, 1, 0), false, 1);
        assertTrue(comparator.intersects(columnNames(1, 0, 0), columnNames(1, 1, 1), filter));

        // same case, but with a missing end component
        filter = new SliceQueryFilter(composite(1, 0, 0), composite(1, 1), false, 1);
        assertTrue(comparator.intersects(columnNames(1, 0, 0), columnNames(1, 1, 1), filter));

        // slice falls entirely within sstable (with matching end)
        filter = new SliceQueryFilter(composite(1, 1, 0), composite(1, 1, 1), false, 1);
        assertTrue(comparator.intersects(columnNames(1, 0, 0), columnNames(1, 1, 1), filter));

        // same case, but with a missing start component
        filter = new SliceQueryFilter(composite(1, 1), composite(1, 1, 1), false, 1);
        assertTrue(comparator.intersects(columnNames(1, 0, 0), columnNames(1, 1, 1), filter));


        // slice falls entirely within sstable
        filter = new SliceQueryFilter(composite(1, 1, 0), composite(1, 1, 1), false, 1);
        assertTrue(comparator.intersects(columnNames(1, 0, 0), columnNames(2, 2, 2), filter));

        // same case, but with a missing start component
        filter = new SliceQueryFilter(composite(1, 1), composite(1, 1, 1), false, 1);
        assertTrue(comparator.intersects(columnNames(1, 0, 0), columnNames(2, 2, 2), filter));

        // same case, but with a missing start and end components
        filter = new SliceQueryFilter(composite(1), composite(1, 2), false, 1);
        assertTrue(comparator.intersects(columnNames(1, 0, 0), columnNames(2, 2, 2), filter));

        // slice falls entirely within sstable (slice start and end are the same)
        filter = new SliceQueryFilter(composite(1, 1, 1), composite(1, 1, 1), false, 1);
        assertTrue(comparator.intersects(columnNames(1, 0, 0), columnNames(2, 2, 2), filter));


        // slice starts within sstable, empty end
        filter = new SliceQueryFilter(composite(1, 1, 1), ByteBufferUtil.EMPTY_BYTE_BUFFER, false, 1);
        assertTrue(comparator.intersects(columnNames(1, 0, 0), columnNames(2, 0, 0), filter));

        // same case, but with missing end components
        filter = new SliceQueryFilter(composite(1, 1, 1), composite(3), false, 1);
        assertTrue(comparator.intersects(columnNames(1, 0, 0), columnNames(2, 0, 0), filter));

        // slice starts within sstable (matching sstable start), empty end
        filter = new SliceQueryFilter(composite(1, 0, 0), ByteBufferUtil.EMPTY_BYTE_BUFFER, false, 1);
        assertTrue(comparator.intersects(columnNames(1, 0, 0), columnNames(2, 0, 0), filter));

        // same case, but with missing end components
        filter = new SliceQueryFilter(composite(1, 0, 0), composite(3), false, 1);
        assertTrue(comparator.intersects(columnNames(1, 0, 0), columnNames(2, 0, 0), filter));

        // slice starts within sstable (matching sstable end), empty end
        filter = new SliceQueryFilter(composite(2, 0, 0), ByteBufferUtil.EMPTY_BYTE_BUFFER, false, 1);
        assertTrue(comparator.intersects(columnNames(1, 0, 0), columnNames(2, 0, 0), filter));

        // same case, but with missing end components
        filter = new SliceQueryFilter(composite(2, 0, 0), composite(3), false, 1);
        assertTrue(comparator.intersects(columnNames(1, 0, 0), columnNames(2, 0, 0), filter));


        // slice ends within sstable, empty end
        filter = new SliceQueryFilter(ByteBufferUtil.EMPTY_BYTE_BUFFER, composite(1, 1, 1), false, 1);
        assertTrue(comparator.intersects(columnNames(1, 0, 0), columnNames(2, 0, 0), filter));

        // same case, but with missing start components
        filter = new SliceQueryFilter(composite(0), composite(1, 1, 1), false, 1);
        assertTrue(comparator.intersects(columnNames(1, 0, 0), columnNames(2, 0, 0), filter));

        // slice ends within sstable (matching sstable start), empty start
        filter = new SliceQueryFilter(ByteBufferUtil.EMPTY_BYTE_BUFFER, composite(1, 0, 0), false, 1);
        assertTrue(comparator.intersects(columnNames(1, 0, 0), columnNames(2, 0, 0), filter));

        // same case, but with missing start components
        filter = new SliceQueryFilter(composite(0), composite(1, 0, 0), false, 1);
        assertTrue(comparator.intersects(columnNames(1, 0, 0), columnNames(2, 0, 0), filter));

        // slice ends within sstable (matching sstable end), empty start
        filter = new SliceQueryFilter(ByteBufferUtil.EMPTY_BYTE_BUFFER, composite(2, 0, 0), false, 1);
        assertTrue(comparator.intersects(columnNames(1, 0, 0), columnNames(2, 0, 0), filter));

        // same case, but with missing start components
        filter = new SliceQueryFilter(composite(0), composite(2, 0, 0), false, 1);
        assertTrue(comparator.intersects(columnNames(1, 0, 0), columnNames(2, 0, 0), filter));


        // the slice technically falls within the sstable range, but since the first component is restricted to
        // a single value, we can check that the second component does not fall within its min/max
        filter = new SliceQueryFilter(composite(1, 2, 0), composite(1, 3, 0), false, 1);
        assertFalse(comparator.intersects(columnNames(1, 0, 0), columnNames(2, 1, 0), filter));

        // same case, but with a missing start component
        filter = new SliceQueryFilter(composite(1, 2), composite(1, 3, 0), false, 1);
        assertFalse(comparator.intersects(columnNames(1, 0, 0), columnNames(2, 1, 0), filter));

        // same case, but with a missing end component
        filter = new SliceQueryFilter(composite(1, 2, 0), composite(1, 3), false, 1);
        assertFalse(comparator.intersects(columnNames(1, 0, 0), columnNames(2, 1, 0), filter));

        // same case, but with a missing start and end components
        filter = new SliceQueryFilter(composite(1, 2), composite(1, 3), false, 1);
        assertFalse(comparator.intersects(columnNames(1, 0, 0), columnNames(2, 1, 0), filter));

        // same case, but with missing start and end components and different lengths for start and end
        filter = new SliceQueryFilter(composite(1, 2), composite(1), false, 1);
        assertFalse(comparator.intersects(columnNames(1, 0, 0), columnNames(2, 1, 0), filter));


        // same as the previous set of tests, but the second component is equal in the slice start and end
        filter = new SliceQueryFilter(composite(1, 2, 0), composite(1, 2, 0), false, 1);
        assertFalse(comparator.intersects(columnNames(1, 0, 0), columnNames(2, 1, 0), filter));

        // same case, but with a missing start component
        filter = new SliceQueryFilter(composite(1, 2), composite(1, 2, 0), false, 1);
        assertFalse(comparator.intersects(columnNames(1, 0, 0), columnNames(2, 1, 0), filter));

        // same case, but with a missing end component
        filter = new SliceQueryFilter(composite(1, 2, 0), composite(1, 2), false, 1);
        assertFalse(comparator.intersects(columnNames(1, 0, 0), columnNames(2, 1, 0), filter));

        // same case, but with a missing start and end components
        filter = new SliceQueryFilter(composite(1, 2), composite(1, 2), false, 1);
        assertFalse(comparator.intersects(columnNames(1, 0, 0), columnNames(2, 1, 0), filter));

        // same as the previous tests, but it's the third component that doesn't fit in its range this time
        filter = new SliceQueryFilter(composite(1, 1, 2), composite(1, 1, 3), false, 1);
        assertFalse(comparator.intersects(columnNames(1, 1, 0), columnNames(2, 2, 1), filter));


        // basic check on reversed slices
        filter = new SliceQueryFilter(composite(1, 0, 0), composite(0, 0, 0), true, 1);
        assertFalse(comparator.intersects(columnNames(2, 0, 0), columnNames(3, 0, 0), filter));

        filter = new SliceQueryFilter(composite(1, 0, 0), composite(0, 0, 0), true, 1);
        assertFalse(comparator.intersects(columnNames(1, 1, 0), columnNames(3, 0, 0), filter));

        filter = new SliceQueryFilter(composite(1, 1, 1), composite(1, 1, 0), true, 1);
        assertTrue(comparator.intersects(columnNames(1, 0, 0), columnNames(2, 2, 2), filter));
    }

    @Test
    public void testIntersectsMultipleSlices()
    {
        CompositeType comparator = CompositeType.getInstance(Int32Type.instance, Int32Type.instance, Int32Type.instance);

        // all slices intersect
        SliceQueryFilter filter = new SliceQueryFilter(new ColumnSlice[]{
            new ColumnSlice(composite(1, 0, 0), composite(2, 0, 0)),
            new ColumnSlice(composite(3, 0, 0), composite(4, 0, 0)),
            new ColumnSlice(composite(5, 0, 0), composite(6, 0, 0)),
        }, false, 1);

        // first slice doesn't intersect
        assertTrue(comparator.intersects(columnNames(0, 0, 0), columnNames(7, 0, 0), filter));
        filter = new SliceQueryFilter(new ColumnSlice[]{
                new ColumnSlice(composite(1, 0, 0), composite(2, 0, 0)),
                new ColumnSlice(composite(3, 0, 0), composite(4, 0, 0)),
                new ColumnSlice(composite(5, 0, 0), composite(6, 0, 0)),
        }, false, 1);
        assertTrue(comparator.intersects(columnNames(3, 0, 0), columnNames(7, 0, 0), filter));

        // first two slices don't intersect
        assertTrue(comparator.intersects(columnNames(0, 0, 0), columnNames(7, 0, 0), filter));
        filter = new SliceQueryFilter(new ColumnSlice[]{
                new ColumnSlice(composite(1, 0, 0), composite(2, 0, 0)),
                new ColumnSlice(composite(3, 0, 0), composite(4, 0, 0)),
                new ColumnSlice(composite(5, 0, 0), composite(6, 0, 0)),
        }, false, 1);
        assertTrue(comparator.intersects(columnNames(5, 0, 0), columnNames(7, 0, 0), filter));

        // none of the slices intersect
        assertTrue(comparator.intersects(columnNames(0, 0, 0), columnNames(7, 0, 0), filter));
        filter = new SliceQueryFilter(new ColumnSlice[]{
                new ColumnSlice(composite(1, 0, 0), composite(2, 0, 0)),
                new ColumnSlice(composite(3, 0, 0), composite(4, 0, 0)),
                new ColumnSlice(composite(5, 0, 0), composite(6, 0, 0)),
        }, false, 1);
        assertFalse(comparator.intersects(columnNames(7, 0, 0), columnNames(8, 0, 0), filter));
    }


    private static ByteBuffer composite(Integer ... components)
    {
        CompositeType comparator = CompositeType.getInstance(Int32Type.instance, Int32Type.instance, Int32Type.instance);
        CompositeType.Builder builder = comparator.builder();
        for (int component : components)
            builder.add(ByteBufferUtil.bytes(component));
        return builder.build();
    }

    private static List<ByteBuffer> columnNames(Integer ... components)
    {
        List<ByteBuffer> names = new ArrayList<>(components.length);
        for (int component : components)
            names.add(ByteBufferUtil.bytes(component));
        return names;
    }

    private void addColumn(RowMutation rm, ByteBuffer cname)
    {
        rm.add(cfName, cname, ByteBufferUtil.EMPTY_BYTE_BUFFER, 0);
    }

    private ByteBuffer createCompositeKey(String s, UUID uuid, int i, boolean lastIsOne)
    {
        ByteBuffer bytes = ByteBufferUtil.bytes(s);
        int totalSize = 0;
        if (s != null)
        {
            totalSize += 2 + bytes.remaining() + 1;
            if (uuid != null)
            {
                totalSize += 2 + 16 + 1;
                if (i != -1)
                {
                    totalSize += 2 + 1 + 1;
                }
            }
        }

        ByteBuffer bb = ByteBuffer.allocate(totalSize);

        if (s != null)
        {
            bb.putShort((short) bytes.remaining());
            bb.put(bytes);
            bb.put(uuid == null && lastIsOne ? (byte)1 : (byte)0);
            if (uuid != null)
            {
                bb.putShort((short) 16);
                bb.put(UUIDGen.decompose(uuid));
                bb.put(i == -1 && lastIsOne ? (byte)1 : (byte)0);
                if (i != -1)
                {
                    // We are putting a byte only because our test use ints that fit in a byte *and* IntegerType.fromString() will
                    // return something compatible (i.e, putting a full int here would break 'fromStringTest')
                    bb.putShort((short) 1);
                    bb.put((byte)i);
                    bb.put(lastIsOne ? (byte)1 : (byte)0);
                }
            }
        }
        bb.rewind();
        return bb;
    }
}
