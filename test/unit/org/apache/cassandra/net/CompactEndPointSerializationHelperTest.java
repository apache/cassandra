package org.apache.cassandra.net;

import java.net.UnknownHostException;
import java.net.InetAddress;

import org.junit.Test;

import org.apache.cassandra.utils.FBUtilities;

public class CompactEndPointSerializationHelperTest
{
    @Test
    public void testSerialize() throws UnknownHostException
    {
        InetAddress ep = FBUtilities.getLocalAddress();
        byte[] bytes = ep.getAddress();
        System.out.println(bytes.length);
        InetAddress ep2 = InetAddress.getByAddress(bytes);
        System.out.println(ep2);
    }
}
