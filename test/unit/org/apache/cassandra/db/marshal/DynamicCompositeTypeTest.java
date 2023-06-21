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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

import com.google.common.collect.ImmutableMap;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;
import static org.junit.Assert.fail;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.partitions.ImmutableBTreePartition;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.*;
import org.assertj.core.api.Assertions;

public class DynamicCompositeTypeTest
{
    private static final String KEYSPACE1 = "DynamicCompositeType";
    private static final String CF_STANDARDDYNCOMPOSITE = "StandardDynamicComposite";
    public static Map<Byte, AbstractType<?>> aliases = new HashMap<>();

    private static final DynamicCompositeType comparator;
    static
    {
        aliases.put((byte)'b', BytesType.instance);
        aliases.put((byte)'B', ReversedType.getInstance(BytesType.instance));
        aliases.put((byte)'t', TimeUUIDType.instance);
        aliases.put((byte)'T', ReversedType.getInstance(TimeUUIDType.instance));
        comparator = DynamicCompositeType.getInstance(aliases);
    }

    private static final int UUID_COUNT = 3;
    public static final UUID[] uuids = new UUID[UUID_COUNT];
    static
    {
        for (int i = 0; i < UUID_COUNT; ++i)
            uuids[i] = nextTimeUUID().asUUID();
    }

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        AbstractType<?> dynamicComposite = DynamicCompositeType.getInstance(aliases);
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARDDYNCOMPOSITE,
                                                              0,
                                                              AsciiType.instance, // key
                                                              AsciiType.instance, // value
                                                              dynamicComposite)); // clustering
    }

    @Test
    public void testEndOfComponent()
    {
        ByteBuffer[] cnames = {
            createDynamicCompositeKey("test1", uuids[0], -1, false),
            createDynamicCompositeKey("test1", uuids[1], 24, false),
            createDynamicCompositeKey("test1", uuids[1], 42, false),
            createDynamicCompositeKey("test1", uuids[1], 83, false),
            createDynamicCompositeKey("test1", uuids[2], -1, false),
            createDynamicCompositeKey("test1", uuids[2], 42, false),
        };

        ByteBuffer start = createDynamicCompositeKey("test1", uuids[1], -1, false);
        ByteBuffer stop = createDynamicCompositeKey("test1", uuids[1], -1, true);

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
        ByteBuffer key = createDynamicCompositeKey("test1", uuids[1], 42, false);
        assert comparator.getString(key).equals("b@" + test1Hex + ":t@" + uuids[1] + ":IntegerType@42");

        key = createDynamicCompositeKey("test1", uuids[1], -1, true);
        assert comparator.getString(key).equals("b@" + test1Hex + ":t@" + uuids[1] + ":!");
    }

    @Test
    public void testFromString()
    {
        String test1Hex = ByteBufferUtil.bytesToHex(ByteBufferUtil.bytes("test1"));
        ByteBuffer key = createDynamicCompositeKey("test1", uuids[1], 42, false);
        assert key.equals(comparator.fromString("b@" + test1Hex + ":t@" + uuids[1] + ":IntegerType@42"));

        key = createDynamicCompositeKey("test1", uuids[1], -1, true);
        assert key.equals(comparator.fromString("b@" + test1Hex + ":t@" + uuids[1] + ":!"));
    }

    @Test
    public void testValidate()
    {
        ByteBuffer key = createDynamicCompositeKey("test1", uuids[1], 42, false);
        comparator.validate(key);

        key = createDynamicCompositeKey("test1", null, -1, false);
        comparator.validate(key);

        key = createDynamicCompositeKey("test1", uuids[2], -1, true);
        comparator.validate(key);

        key.get(); // make sure we're not aligned anymore
        try
        {
            comparator.validate(key);
            fail("Should not validate");
        }
        catch (MarshalException e) {}

        key = ByteBuffer.allocate(5 + "test1".length() + 5 + 14);
        key.putShort((short) (0x8000 | 'b'));
        key.putShort((short) "test1".length());
        key.put(ByteBufferUtil.bytes("test1"));
        key.put((byte) 0);
        key.putShort((short) (0x8000 | 't'));
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

        key = createDynamicCompositeKey("test1", UUID.randomUUID(), 42, false);
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
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_STANDARDDYNCOMPOSITE);

        ByteBuffer cname1 = createDynamicCompositeKey("test1", null, -1, false);
        ByteBuffer cname2 = createDynamicCompositeKey("test1", uuids[0], 24, false);
        ByteBuffer cname3 = createDynamicCompositeKey("test1", uuids[0], 42, false);
        ByteBuffer cname4 = createDynamicCompositeKey("test2", uuids[0], -1, false);
        ByteBuffer cname5 = createDynamicCompositeKey("test2", uuids[1], 42, false);

        ByteBuffer key = ByteBufferUtil.bytes("k");
        long ts = FBUtilities.timestampMicros();
        new RowUpdateBuilder(cfs.metadata(), ts, key).clustering(cname5).add("val", "cname5").build().applyUnsafe();
        new RowUpdateBuilder(cfs.metadata(), ts, key).clustering(cname1).add("val", "cname1").build().applyUnsafe();
        new RowUpdateBuilder(cfs.metadata(), ts, key).clustering(cname4).add("val", "cname4").build().applyUnsafe();
        new RowUpdateBuilder(cfs.metadata(), ts, key).clustering(cname2).add("val", "cname2").build().applyUnsafe();
        new RowUpdateBuilder(cfs.metadata(), ts, key).clustering(cname3).add("val", "cname3").build().applyUnsafe();

        ColumnMetadata cdef = cfs.metadata().getColumn(ByteBufferUtil.bytes("val"));

        ImmutableBTreePartition readPartition = Util.getOnlyPartitionUnfiltered(Util.cmd(cfs, key).build());
        Iterator<Row> iter = readPartition.iterator();

        compareValues(iter.next().getCell(cdef), "cname1");
        compareValues(iter.next().getCell(cdef), "cname2");
        compareValues(iter.next().getCell(cdef), "cname3");
        compareValues(iter.next().getCell(cdef), "cname4");
        compareValues(iter.next().getCell(cdef), "cname5");
    }
    private void compareValues(Cell<?> c, String r) throws CharacterCodingException
    {
        assert ByteBufferUtil.string(c.buffer()).equals(r) : "Expected: {" + ByteBufferUtil.string(c.buffer()) + "} got: {" + r + "}";
    }

    @Test
    public void testFullRoundReversed() throws Exception
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_STANDARDDYNCOMPOSITE);

        ByteBuffer cname1 = createDynamicCompositeKey("test1", null, -1, false, true);
        ByteBuffer cname2 = createDynamicCompositeKey("test1", uuids[0], 24, false, true);
        ByteBuffer cname3 = createDynamicCompositeKey("test1", uuids[0], 42, false, true);
        ByteBuffer cname4 = createDynamicCompositeKey("test2", uuids[0], -1, false, true);
        ByteBuffer cname5 = createDynamicCompositeKey("test2", uuids[1], 42, false, true);

        ByteBuffer key = ByteBufferUtil.bytes("kr");

        long ts = FBUtilities.timestampMicros();
        new RowUpdateBuilder(cfs.metadata(), ts, key).clustering(cname5).add("val", "cname5").build().applyUnsafe();
        new RowUpdateBuilder(cfs.metadata(), ts, key).clustering(cname1).add("val", "cname1").build().applyUnsafe();
        new RowUpdateBuilder(cfs.metadata(), ts, key).clustering(cname4).add("val", "cname4").build().applyUnsafe();
        new RowUpdateBuilder(cfs.metadata(), ts, key).clustering(cname2).add("val", "cname2").build().applyUnsafe();
        new RowUpdateBuilder(cfs.metadata(), ts, key).clustering(cname3).add("val", "cname3").build().applyUnsafe();

        ColumnMetadata cdef = cfs.metadata().getColumn(ByteBufferUtil.bytes("val"));

        ImmutableBTreePartition readPartition = Util.getOnlyPartitionUnfiltered(Util.cmd(cfs, key).build());
        Iterator<Row> iter = readPartition.iterator();

        compareValues(iter.next().getCell(cdef), "cname5");
        compareValues(iter.next().getCell(cdef), "cname4");
        compareValues(iter.next().getCell(cdef), "cname1"); // null UUID < reversed value
        compareValues(iter.next().getCell(cdef), "cname3");
        compareValues(iter.next().getCell(cdef), "cname2");
    }

    @Test
    public void testUncomparableColumns()
    {
        ByteBuffer bytes = ByteBuffer.allocate(2 + 2 + 4 + 1);
        bytes.putShort((short)(0x8000 | 'b'));
        bytes.putShort((short) 4);
        bytes.put(new byte[4]);
        bytes.put((byte) 0);
        bytes.rewind();

        ByteBuffer uuid = ByteBuffer.allocate(2 + 2 + 16 + 1);
        uuid.putShort((short)(0x8000 | 't'));
        uuid.putShort((short) 16);
        uuid.put(UUIDGen.decompose(uuids[0]));
        uuid.put((byte) 0);
        uuid.rewind();

        try
        {
            int c = comparator.compare(bytes, uuid);
            assert c == -1 : "Expecting bytes to sort before uuid, but got " + c;
        }
        catch (Exception e)
        {
            fail("Shouldn't throw exception");
        }
    }

    @Test
    public void testUncomparableReversedColumns()
    {
        ByteBuffer uuid = ByteBuffer.allocate(2 + 2 + 16 + 1);
        uuid.putShort((short)(0x8000 | 'T'));
        uuid.putShort((short) 16);
        uuid.put(UUIDGen.decompose(uuids[0]));
        uuid.put((byte) 0);
        uuid.rewind();

        ByteBuffer bytes = ByteBuffer.allocate(2 + 2 + 4 + 1);
        bytes.putShort((short)(0x8000 | 'B'));
        bytes.putShort((short) 4);
        bytes.put(new byte[4]);
        bytes.put((byte) 0);
        bytes.rewind();

        try
        {
            int c = comparator.compare(uuid, bytes);
            assert c == 1 : "Expecting bytes to sort before uuid, but got " + c;
        }
        catch (Exception e)
        {
            fail("Shouldn't throw exception");
        }
    }

    public void testCompatibility() throws Exception
    {
        assert TypeParser.parse("DynamicCompositeType()").isCompatibleWith(TypeParser.parse("DynamicCompositeType()"));
        assert TypeParser.parse("DynamicCompositeType(a => IntegerType)").isCompatibleWith(TypeParser.parse("DynamicCompositeType()"));
        assert TypeParser.parse("DynamicCompositeType(b => BytesType, a => IntegerType)").isCompatibleWith(TypeParser.parse("DynamicCompositeType(a => IntegerType)"));

        assert !TypeParser.parse("DynamicCompositeType(a => BytesType)").isCompatibleWith(TypeParser.parse("DynamicCompositeType(a => AsciiType)"));
        assert !TypeParser.parse("DynamicCompositeType(a => BytesType)").isCompatibleWith(TypeParser.parse("DynamicCompositeType(a => BytesType, b => AsciiType)"));
    }

    private static ByteBuffer createDynamicCompositeKey(String s, UUID uuid, int i, boolean lastIsOne)
    {
        return createDynamicCompositeKey(s, uuid, i, lastIsOne, false);
    }

    public static ByteBuffer createDynamicCompositeKey(String s, UUID uuid, int i, boolean lastIsOne, boolean reversed)
    {
        String intType = (reversed ? "ReversedType(IntegerType)" : "IntegerType");
        ByteBuffer bytes = ByteBufferUtil.bytes(s);
        int totalSize = 0;
        if (s != null)
        {
            totalSize += 2 + 2 + bytes.remaining() + 1;
            if (uuid != null)
            {
                totalSize += 2 + 2 + 16 + 1;
                if (i != -1)
                {
                    totalSize += 2 + intType.length() + 2 + 1 + 1;
                }
            }
        }

        ByteBuffer bb = ByteBuffer.allocate(totalSize);

        if (s != null)
        {
            bb.putShort((short)(0x8000 | (reversed ? 'B' : 'b')));
            bb.putShort((short) bytes.remaining());
            bb.put(bytes);
            bb.put(uuid == null && lastIsOne ? (byte)1 : (byte)0);
            if (uuid != null)
            {
                bb.putShort((short)(0x8000 | (reversed ? 'T' : 't')));
                bb.putShort((short) 16);
                bb.put(UUIDGen.decompose(uuid));
                bb.put(i == -1 && lastIsOne ? (byte)1 : (byte)0);
                if (i != -1)
                {
                    bb.putShort((short) intType.length());
                    bb.put(ByteBufferUtil.bytes(intType));
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

    @Test
    public void testEmptyValue()
    {
        DynamicCompositeType type = DynamicCompositeType.getInstance(ImmutableMap.of((byte) 'V', BytesType.instance));

        String cqlLiteral = "0x8056000000";
        ByteBuffer bb = type.asCQL3Type().fromCQLLiteral(cqlLiteral);
        type.validate(bb);

        String str = type.getString(bb);
        ByteBuffer read = type.fromString(str);
        Assertions.assertThat(read).isEqualTo(bb);
    }
}
