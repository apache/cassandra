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
package org.apache.cassandra.db;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.cassandra.SchemaLoader;
import org.apache.commons.lang.ArrayUtils;
import org.junit.Test;

import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.io.util.DataOutputBuffer;

public class ReadMessageTest extends SchemaLoader
{
    @Test
    public void testMakeReadMessage() throws IOException
    {
        ArrayList<byte[]> colList = new ArrayList<byte[]>();
        colList.add("col1".getBytes());
        colList.add("col2".getBytes());
        
        ReadCommand rm, rm2;
        
        rm = new SliceByNamesReadCommand("Keyspace1", "row1", new QueryPath("Standard1"), colList);
        rm2 = serializeAndDeserializeReadMessage(rm);
        assert rm2.toString().equals(rm.toString());

        rm = new SliceFromReadCommand("Keyspace1", "row1", new QueryPath("Standard1"), ArrayUtils.EMPTY_BYTE_ARRAY, ArrayUtils.EMPTY_BYTE_ARRAY, new ArrayList<byte[]>(0), true, 2);
        rm2 = serializeAndDeserializeReadMessage(rm);
        assert rm2.toString().equals(rm.toString());
        
        rm = new SliceFromReadCommand("Keyspace1", "row1", new QueryPath("Standard1"), "a".getBytes(), "z".getBytes(), new ArrayList<byte[]>(0), true, 5);
        rm2 = serializeAndDeserializeReadMessage(rm);
        assertEquals(rm2.toString(), rm.toString());

        rm = new SliceFromReadCommand("Keyspace1", "row1", new QueryPath("Standard1"), ArrayUtils.EMPTY_BYTE_ARRAY, ArrayUtils.EMPTY_BYTE_ARRAY, null, true, 2);
        rm2 = serializeAndDeserializeReadMessage(rm);
        assert rm2.toString().equals(rm.toString());

        rm = new SliceFromReadCommand("Keyspace1", "row1", new QueryPath("Standard1"), "a".getBytes(), "z".getBytes(), null, true, 5);
        rm2 = serializeAndDeserializeReadMessage(rm);
        assertEquals(rm2.toString(), rm.toString());

        for (String[] bitmaskTests: new String[][] { {}, {"test one", "test two" }, { new String(new byte[] { 0, 1, 0x20, (byte) 0xff }) } })
        {
            ArrayList<byte[]> bitmasks = new ArrayList<byte[]>(bitmaskTests.length);
            for (String bitmaskTest : bitmaskTests)
            {
                bitmasks.add(bitmaskTest.getBytes("UTF-8"));
            }

            rm = new SliceFromReadCommand("Keyspace1", "row1", new QueryPath("Standard1"), ArrayUtils.EMPTY_BYTE_ARRAY, ArrayUtils.EMPTY_BYTE_ARRAY, bitmasks, true, 2);
            rm2 = serializeAndDeserializeReadMessage(rm);
            assert rm2.toString().equals(rm.toString());

            rm = new SliceFromReadCommand("Keyspace1", "row1", new QueryPath("Standard1"), "a".getBytes(), "z".getBytes(), bitmasks, true, 5);
            rm2 = serializeAndDeserializeReadMessage(rm);
            assertEquals(rm2.toString(), rm.toString());
        }
    }

    private ReadCommand serializeAndDeserializeReadMessage(ReadCommand rm) throws IOException
    {
        ReadCommandSerializer rms = ReadCommand.serializer();
        DataOutputBuffer dos = new DataOutputBuffer();
        ByteArrayInputStream bis;

        rms.serialize(rm, dos);
        bis = new ByteArrayInputStream(dos.getData(), 0, dos.getLength());
        return rms.deserialize(new DataInputStream(bis));
    }
    
    @Test
    public void testGetColumn() throws IOException, ColumnFamilyNotDefinedException
    {
        Table table = Table.open("Keyspace1");
        RowMutation rm;

        // add data
        rm = new RowMutation("Keyspace1", "key1");
        rm.add(new QueryPath("Standard1", null, "Column1".getBytes()), "abcd".getBytes(), 0);
        rm.apply();

        ReadCommand command = new SliceByNamesReadCommand("Keyspace1", "key1", new QueryPath("Standard1"), Arrays.asList("Column1".getBytes()));
        Row row = command.getRow(table);
        IColumn col = row.cf.getColumn("Column1".getBytes());
        assert Arrays.equals(col.value(), "abcd".getBytes());  
    }
}
