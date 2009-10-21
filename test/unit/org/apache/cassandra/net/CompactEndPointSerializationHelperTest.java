package org.apache.cassandra.net;

import java.net.UnknownHostException;

import org.junit.Test;

import org.apache.cassandra.utils.FBUtilities;

public class CompactEndPointSerializationHelperTest
{
    @Test
    public void testSerialize() throws UnknownHostException
    {
        EndPoint ep = new EndPoint(FBUtilities.getHostAddress(), 7000);
        byte[] bytes = ep.getAddress();
        System.out.println(bytes.length);
        EndPoint ep2 = EndPoint.getByAddress(bytes);
        System.out.println(ep2);
    }
}
