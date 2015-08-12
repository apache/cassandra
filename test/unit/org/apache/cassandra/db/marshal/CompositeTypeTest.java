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
import java.nio.charset.CharacterCodingException;
import java.util.*;

import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.partitions.ImmutableBTreePartition;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.*;

public class CompositeTypeTest
{
    private static final String KEYSPACE1 = "CompositeTypeTest";
    private static final String CF_STANDARDCOMPOSITE = "StandardComposite";
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

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        AbstractType<?> composite = CompositeType.getInstance(Arrays.asList(new AbstractType<?>[]{BytesType.instance, TimeUUIDType.instance, IntegerType.instance}));
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.denseCFMD(KEYSPACE1, CF_STANDARDCOMPOSITE, composite));
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
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_STANDARDCOMPOSITE);

        ByteBuffer cname1 = createCompositeKey("test1", null, -1, false);
        ByteBuffer cname2 = createCompositeKey("test1", uuids[0], 24, false);
        ByteBuffer cname3 = createCompositeKey("test1", uuids[0], 42, false);
        ByteBuffer cname4 = createCompositeKey("test2", uuids[0], -1, false);
        ByteBuffer cname5 = createCompositeKey("test2", uuids[1], 42, false);

        ByteBuffer key = ByteBufferUtil.bytes("k");

        long ts = FBUtilities.timestampMicros();
        new RowUpdateBuilder(cfs.metadata, ts, key).clustering(cname5).add("val", "cname5").build().applyUnsafe();
        new RowUpdateBuilder(cfs.metadata, ts, key).clustering(cname1).add("val", "cname1").build().applyUnsafe();
        new RowUpdateBuilder(cfs.metadata, ts, key).clustering(cname4).add("val", "cname4").build().applyUnsafe();
        new RowUpdateBuilder(cfs.metadata, ts, key).clustering(cname2).add("val", "cname2").build().applyUnsafe();
        new RowUpdateBuilder(cfs.metadata, ts, key).clustering(cname3).add("val", "cname3").build().applyUnsafe();

        ColumnDefinition cdef = cfs.metadata.getColumnDefinition(ByteBufferUtil.bytes("val"));

        ImmutableBTreePartition readPartition = Util.getOnlyPartitionUnfiltered(Util.cmd(cfs, key).build());
        Iterator<Row> iter = readPartition.iterator();

        compareValues(iter.next().getCell(cdef), "cname1");
        compareValues(iter.next().getCell(cdef), "cname2");
        compareValues(iter.next().getCell(cdef), "cname3");
        compareValues(iter.next().getCell(cdef), "cname4");
        compareValues(iter.next().getCell(cdef), "cname5");
    }
    private void compareValues(Cell c, String r) throws CharacterCodingException
    {
        assert ByteBufferUtil.string(c.value()).equals(r) : "Expected: {" + ByteBufferUtil.string(c.value()) + "} got: {" + r + "}";
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
