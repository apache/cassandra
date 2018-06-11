package org.apache.cassandra.locator;

import java.net.UnknownHostException;

import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;

public abstract class ReplicaCollectionTest
{
    static final InetAddressAndPort EP1;
    static final InetAddressAndPort EP2;
    static final InetAddressAndPort EP3;

    static
    {
        try
        {
            EP1 = InetAddressAndPort.getByName("127.0.0.1");
            EP2 = InetAddressAndPort.getByName("127.0.0.2");
            EP3 = InetAddressAndPort.getByName("127.0.0.3");
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
    }

    static Token tk(long t)
    {
        return new Murmur3Partitioner.LongToken(t);
    }

    static Range<Token> range(long left, long right)
    {
        return new Range<>(tk(left), tk(right));
    }


}
