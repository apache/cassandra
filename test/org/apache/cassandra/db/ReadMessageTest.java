package org.apache.cassandra.db;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.cassandra.io.DataInputBuffer;
import org.apache.cassandra.io.DataOutputBuffer;
import org.testng.annotations.Test;

public class ReadMessageTest
{
    @Test
    public void testMakeReadMessage()
    {
        ArrayList<String> colList = new ArrayList<String>();
        colList.add("col1");
        colList.add("col2");

        ReadMessage rm = new ReadMessage("Table1", "row1", "foo", colList);
        ReadMessage rm2 = serializeAndDeserializeReadMessage(rm);

        assert rm2.toString().equals(rm.toString());
    }

    private ReadMessage serializeAndDeserializeReadMessage(ReadMessage rm)
    {
        ReadMessage rm2 = null;
        ReadMessageSerializer rms = (ReadMessageSerializer) ReadMessage.serializer();
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
}
