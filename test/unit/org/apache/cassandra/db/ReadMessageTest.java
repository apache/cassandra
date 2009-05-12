package org.apache.cassandra.db;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.junit.Test;

import org.apache.cassandra.io.DataInputBuffer;
import org.apache.cassandra.io.DataOutputBuffer;

public class ReadMessageTest
{
    @Test
    public void testMakeReadMessage()
    {
        ArrayList<String> colList = new ArrayList<String>();
        colList.add("col1");
        colList.add("col2");
        
        ReadCommand rm, rm2;
        
        rm = new SliceByNamesReadCommand("Table1", "row1", "foo", colList);
        rm2 = serializeAndDeserializeReadMessage(rm);
        assert rm2.toString().equals(rm.toString());

        rm = new ColumnReadCommand("Table1", "row1", "foo:col1");
        rm2 = serializeAndDeserializeReadMessage(rm);
        assert rm2.toString().equals(rm.toString());

        rm = new RowReadCommand("Table1", "row1");
        rm2 = serializeAndDeserializeReadMessage(rm);
        assert rm2.toString().equals(rm.toString());

        rm = new ColumnsSinceReadCommand("Table1", "row1", "foo", 1);
        rm2 = serializeAndDeserializeReadMessage(rm);
        assert rm2.toString().equals(rm.toString());

        rm = new SliceReadCommand("Table1", "row1", "foo", 1, 2);
        rm2 = serializeAndDeserializeReadMessage(rm);
        assert rm2.toString().equals(rm.toString());
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
        rm.add("Standard1:Column1", "abcd".getBytes(), 0);
        rm.apply();

        ReadCommand command = new ColumnReadCommand("Table1", "key1", "Standard1:Column1");
        Row row = command.getRow(table);
        ColumnFamily cf = row.getColumnFamily("Standard1");
        IColumn col = cf.getColumn("Column1");
        assert Arrays.equals(((Column)col).value(), "abcd".getBytes());  
    }
}
