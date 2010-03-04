package org.apache.cassandra.utils;

import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.junit.Test;

import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;


public class UUIDTests
{
    @Test
    public void verifyType1() throws UnknownHostException
    {
        
        UUID uuid = UUIDGen.makeType1UUIDFromHost(InetAddress.getByName("127.0.0.1"));
        assert uuid.version() == 1;
    }

    @Test
    public void verifyOrdering1() throws UnknownHostException
    {
        UUID one = UUIDGen.makeType1UUIDFromHost(InetAddress.getByName("127.0.0.1"));
        UUID two = UUIDGen.makeType1UUIDFromHost(InetAddress.getByName("127.0.0.2"));
        assert one.timestamp() < two.timestamp();
    }


    @Test
    public void testDecomposeAndRaw() throws UnknownHostException
    {
        UUID a = UUIDGen.makeType1UUIDFromHost(InetAddress.getByName("127.0.0.1"));
        byte[] decomposed = UUIDGen.decompose(a);
        UUID b = UUIDGen.makeType1UUID(decomposed);
        assert a.equals(b);
    }

    @Test
    public void testTimeUUIDType() throws UnknownHostException
    {
        TimeUUIDType comp = new TimeUUIDType();
        byte[] first = UUIDGen.decompose(UUIDGen.makeType1UUIDFromHost(InetAddress.getByName("127.0.0.1")));
        byte[] second = UUIDGen.decompose(UUIDGen.makeType1UUIDFromHost(InetAddress.getByName("127.0.0.1")));
        assert comp.compare(first, second) < 0;
        assert comp.compare(second, first) > 0;
        byte[] sameAsFirst = UUIDGen.decompose(UUIDGen.makeType1UUID(first));
        assert comp.compare(first, sameAsFirst) == 0;
    }

    private void assertNonZero(BigInteger i)
    {
        assert i.toString(2).indexOf("1") > -1;
    }
}
