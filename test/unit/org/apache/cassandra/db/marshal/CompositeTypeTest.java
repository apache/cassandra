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

import org.junit.Test;
import static org.junit.Assert.fail;

import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.columniterator.IdentityQueryFilter;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.utils.*;

public class CompositeTypeTest extends CleanupHelper
{
    private static final String cfName = "StandardComposite";
    private static final CompositeType comparator;
    static
    {
        List<AbstractType> subComparators = new ArrayList<AbstractType>();
        subComparators.add(BytesType.instance);
        subComparators.add(TimeUUIDType.instance);
        subComparators.add(IntegerType.instance);
        try
        {
            comparator = CompositeType.getInstance(subComparators);
        }
        catch (ConfigurationException e)
        {
            throw new RuntimeException(e);
        }

    }

    private static final int UUID_COUNT = 3;
    private static final UUID[] uuids = new UUID[UUID_COUNT];
    static
    {
        for (int i = 0; i < UUID_COUNT; ++i)
            uuids[i] = UUIDGen.makeType1UUIDFromHost(FBUtilities.getBroadcastAddress());
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
            assert e.toString().contains("TimeUUID should be 16 or 0 bytes");
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
        Table table = Table.open("Keyspace1");
        ColumnFamilyStore cfs = table.getColumnFamilyStore(cfName);

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

        ColumnFamily cf = cfs.getColumnFamily(QueryFilter.getIdentityFilter(Util.dk("k"), new QueryPath(cfName, null, null)));

        Iterator<IColumn> iter = cf.getSortedColumns().iterator();

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

        try
        {
            TypeParser.parse("CompositeType()");
            fail("Shouldn't work");
        }
        catch (ConfigurationException e) {}
    }


    private void addColumn(RowMutation rm, ByteBuffer cname)
    {
        rm.add(new QueryPath(cfName, null , cname), ByteBufferUtil.EMPTY_BYTE_BUFFER, 0);
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
