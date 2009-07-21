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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.io.DataInputBuffer;
import org.apache.cassandra.io.DataOutputBuffer;
import org.apache.cassandra.db.filter.QueryPath;

public class ReadMessageTest
{
    @Test
    public void testMakeReadMessage()
    {
        ArrayList<String> colList = new ArrayList<String>();
        colList.add("col1");
        colList.add("col2");
        
        ReadCommand rm, rm2;
        
        rm = new SliceByNamesReadCommand("Table1", "row1", new QueryPath("foo"), colList);
        rm2 = serializeAndDeserializeReadMessage(rm);
        assert rm2.toString().equals(rm.toString());

        rm = new SliceFromReadCommand("Table1", "row1", new QueryPath("foo"), "", "", true, 2);
        rm2 = serializeAndDeserializeReadMessage(rm);
        assert rm2.toString().equals(rm.toString());
        
        rm = new SliceFromReadCommand("Table1", "row1", new QueryPath("foo"), "a", "z", true, 5);
        rm2 = serializeAndDeserializeReadMessage(rm);
        assertEquals(rm2.toString(), rm.toString());
    }

    private ReadCommand serializeAndDeserializeReadMessage(ReadCommand rm)
    {
        ReadCommand rm2 = null;
        ReadCommandSerializer rms = ReadCommand.serializer();
        DataOutputBuffer dos = new DataOutputBuffer();
        DataInputBuffer dis = new DataInputBuffer();

        try
        {
            rms.serialize(rm, dos);
            dis.reset(dos.getData(), dos.getLength());
            rm2 = rms.deserialize(dis);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        return rm2;
    }
    
    @Test
    public void testGetColumn() throws IOException, ColumnFamilyNotDefinedException
    {
        Table table = Table.open("Table1");
        RowMutation rm;

        // add data
        rm = new RowMutation("Table1", "key1");
        rm.add(new QueryPath("Standard1", null, "Column1"), "abcd".getBytes(), 0);
        rm.apply();

        ReadCommand command = new SliceByNamesReadCommand("Table1", "key1", new QueryPath("Standard1"), Arrays.asList("Column1"));
        Row row = command.getRow(table);
        ColumnFamily cf = row.getColumnFamily("Standard1");
        IColumn col = cf.getColumn("Column1");
        assert Arrays.equals(((Column)col).value(), "abcd".getBytes());  
    }
}
