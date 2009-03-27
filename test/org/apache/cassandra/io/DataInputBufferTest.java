package org.apache.cassandra.io;

import org.testng.annotations.Test;

import java.util.Random;
import java.io.IOException;

public class DataInputBufferTest {
    @Test
    public void testRandom() throws IOException {
        Random random = new Random();
        byte[] bytes = new byte[1024*1024];
        random.nextBytes(bytes);

        DataInputBuffer.FastByteArrayInputStream bis = new DataInputBuffer.FastByteArrayInputStream(bytes);
        int read = 0;
        int n = 0;
        while ( true )
        {
            read = bis.read();
            if ( read == -1 )
                break;
            assert read == ((int)bytes[n++]&0xFF);
        }
        assert n == bytes.length;
    }

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
