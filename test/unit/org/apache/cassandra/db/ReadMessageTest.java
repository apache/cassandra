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

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.io.util.DataOutputBuffer;

import org.junit.Test;
import org.apache.cassandra.utils.ByteBufferUtil;


public class ReadMessageTest extends SchemaLoader
{
    @Test
    public void testMakeReadMessage() throws IOException
    {
        ArrayList<ByteBuffer> colList = new ArrayList<ByteBuffer>();
        colList.add(ByteBufferUtil.bytes("col1"));
        colList.add(ByteBufferUtil.bytes("col2"));
        
        ReadCommand rm, rm2;
        DecoratedKey dk = Util.dk("row1");
        
        rm = new SliceByNamesReadCommand("Keyspace1", dk.key, new QueryPath("Standard1"), colList);
        rm2 = serializeAndDeserializeReadMessage(rm);
        assert rm2.toString().equals(rm.toString());

        rm = new SliceFromReadCommand("Keyspace1", dk.key, new QueryPath("Standard1"), ByteBufferUtil.EMPTY_BYTE_BUFFER, ByteBufferUtil.EMPTY_BYTE_BUFFER, true, 2);
        rm2 = serializeAndDeserializeReadMessage(rm);
        assert rm2.toString().equals(rm.toString());
        
        rm = new SliceFromReadCommand("Keyspace1", dk.key, new QueryPath("Standard1"), ByteBufferUtil.bytes("a"), ByteBufferUtil.bytes("z"), true, 5);
        rm2 = serializeAndDeserializeReadMessage(rm);
        assertEquals(rm2.toString(), rm.toString());

        rm = new SliceFromReadCommand("Keyspace1", dk.key, new QueryPath("Standard1"), ByteBufferUtil.EMPTY_BYTE_BUFFER, ByteBufferUtil.EMPTY_BYTE_BUFFER, true, 2);
        rm2 = serializeAndDeserializeReadMessage(rm);
        assert rm2.toString().equals(rm.toString());

        rm = new SliceFromReadCommand("Keyspace1", dk.key, new QueryPath("Standard1"), ByteBufferUtil.bytes("a"), ByteBufferUtil.bytes("z"), true, 5);
        rm2 = serializeAndDeserializeReadMessage(rm);
        assertEquals(rm2.toString(), rm.toString());
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
        DecoratedKey dk = Util.dk("key1");

        // add data
        rm = new RowMutation("Keyspace1", dk.key);
        rm.add(new QueryPath("Standard1", null, ByteBufferUtil.bytes("Column1")), ByteBufferUtil.bytes("abcd"), 0);
        rm.apply();

        ReadCommand command = new SliceByNamesReadCommand("Keyspace1", dk.key, new QueryPath("Standard1"), Arrays.asList(ByteBufferUtil.bytes("Column1")));
        Row row = command.getRow(table);
        IColumn col = row.cf.getColumn(ByteBufferUtil.bytes("Column1"));
        assert Arrays.equals(col.value().array(), "abcd".getBytes());  
    }
}
