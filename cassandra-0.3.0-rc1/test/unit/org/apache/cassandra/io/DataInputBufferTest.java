package org.apache.cassandra.io;

import java.io.IOException;

import org.junit.Test;

public class DataInputBufferTest {
    @Test
    public void testSmall() throws IOException {
        DataOutputBuffer bufOut = new DataOutputBuffer();
        bufOut.writeUTF("Avinash");
        bufOut.writeInt(41*1024*1024);
        DataInputBuffer bufIn = new DataInputBuffer();
        bufIn.reset(bufOut.getData(), bufOut.getLength());
        assert bufIn.readUTF().equals("Avinash");
        assert bufIn.readInt() == 41 * 1024 * 1024;
    }

}
