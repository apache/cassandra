package org.apache.cassandra.gms;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.cassandra.io.DataInputBuffer;
import org.apache.cassandra.io.DataOutputBuffer;
import org.apache.cassandra.net.EndPoint;
import org.junit.Test;

public class GossipDigestTest
{

    @Test
    public void test() throws IOException
    {
        EndPoint endPoint = new EndPoint("127.0.0.1", 3333);
        int generation = 0;
        int maxVersion = 123;
        GossipDigest expected = new GossipDigest(endPoint, generation, maxVersion);
        //make sure we get the same values out
        assertEquals(endPoint, expected.getEndPoint());
        assertEquals(generation, expected.getGeneration());
        assertEquals(maxVersion, expected.getMaxVersion());
        
        //test the serialization and equals
        DataOutputBuffer output = new DataOutputBuffer();
        GossipDigest.serializer().serialize(expected, output);
        
        DataInputBuffer input = new DataInputBuffer();
        input.reset(output.getData(), output.getLength());
        GossipDigest actual = GossipDigest.serializer().deserialize(input);
        assertEquals(0, expected.compareTo(actual));
    }

}
