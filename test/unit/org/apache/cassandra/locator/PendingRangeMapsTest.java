package org.apache.cassandra.locator;

import org.apache.cassandra.dht.RandomPartitioner.BigIntegerToken;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PendingRangeMapsTest {

    private Range<Token> genRange(String left, String right)
    {
        return new Range<Token>(new BigIntegerToken(left), new BigIntegerToken(right));
    }

    @Test
    public void testPendingEndpoints() throws UnknownHostException
    {
        PendingRangeMaps pendingRangeMaps = new PendingRangeMaps();

        pendingRangeMaps.addPendingRange(genRange("5", "15"), InetAddress.getByName("127.0.0.1"));
        pendingRangeMaps.addPendingRange(genRange("15", "25"), InetAddress.getByName("127.0.0.2"));
        pendingRangeMaps.addPendingRange(genRange("25", "35"), InetAddress.getByName("127.0.0.3"));
        pendingRangeMaps.addPendingRange(genRange("35", "45"), InetAddress.getByName("127.0.0.4"));
        pendingRangeMaps.addPendingRange(genRange("45", "55"), InetAddress.getByName("127.0.0.5"));
        pendingRangeMaps.addPendingRange(genRange("45", "65"), InetAddress.getByName("127.0.0.6"));

        assertEquals(0, pendingRangeMaps.pendingEndpointsFor(new BigIntegerToken("0")).size());
        assertEquals(0, pendingRangeMaps.pendingEndpointsFor(new BigIntegerToken("5")).size());
        assertEquals(1, pendingRangeMaps.pendingEndpointsFor(new BigIntegerToken("10")).size());
        assertEquals(1, pendingRangeMaps.pendingEndpointsFor(new BigIntegerToken("15")).size());
        assertEquals(1, pendingRangeMaps.pendingEndpointsFor(new BigIntegerToken("20")).size());
        assertEquals(1, pendingRangeMaps.pendingEndpointsFor(new BigIntegerToken("25")).size());
        assertEquals(1, pendingRangeMaps.pendingEndpointsFor(new BigIntegerToken("35")).size());
        assertEquals(1, pendingRangeMaps.pendingEndpointsFor(new BigIntegerToken("45")).size());
        assertEquals(2, pendingRangeMaps.pendingEndpointsFor(new BigIntegerToken("55")).size());
        assertEquals(1, pendingRangeMaps.pendingEndpointsFor(new BigIntegerToken("65")).size());

        Collection<InetAddress> endpoints = pendingRangeMaps.pendingEndpointsFor(new BigIntegerToken("15"));
        assertTrue(endpoints.contains(InetAddress.getByName("127.0.0.1")));
    }

    @Test
    public void testWrapAroundRanges() throws UnknownHostException
    {
        PendingRangeMaps pendingRangeMaps = new PendingRangeMaps();

        pendingRangeMaps.addPendingRange(genRange("5", "15"), InetAddress.getByName("127.0.0.1"));
        pendingRangeMaps.addPendingRange(genRange("15", "25"), InetAddress.getByName("127.0.0.2"));
        pendingRangeMaps.addPendingRange(genRange("25", "35"), InetAddress.getByName("127.0.0.3"));
        pendingRangeMaps.addPendingRange(genRange("35", "45"), InetAddress.getByName("127.0.0.4"));
        pendingRangeMaps.addPendingRange(genRange("45", "55"), InetAddress.getByName("127.0.0.5"));
        pendingRangeMaps.addPendingRange(genRange("45", "65"), InetAddress.getByName("127.0.0.6"));
        pendingRangeMaps.addPendingRange(genRange("65", "7"), InetAddress.getByName("127.0.0.7"));

        assertEquals(1, pendingRangeMaps.pendingEndpointsFor(new BigIntegerToken("0")).size());
        assertEquals(1, pendingRangeMaps.pendingEndpointsFor(new BigIntegerToken("5")).size());
        assertEquals(2, pendingRangeMaps.pendingEndpointsFor(new BigIntegerToken("7")).size());
        assertEquals(1, pendingRangeMaps.pendingEndpointsFor(new BigIntegerToken("10")).size());
        assertEquals(1, pendingRangeMaps.pendingEndpointsFor(new BigIntegerToken("15")).size());
        assertEquals(1, pendingRangeMaps.pendingEndpointsFor(new BigIntegerToken("20")).size());
        assertEquals(1, pendingRangeMaps.pendingEndpointsFor(new BigIntegerToken("25")).size());
        assertEquals(1, pendingRangeMaps.pendingEndpointsFor(new BigIntegerToken("35")).size());
        assertEquals(1, pendingRangeMaps.pendingEndpointsFor(new BigIntegerToken("45")).size());
        assertEquals(2, pendingRangeMaps.pendingEndpointsFor(new BigIntegerToken("55")).size());
        assertEquals(1, pendingRangeMaps.pendingEndpointsFor(new BigIntegerToken("65")).size());

        Collection<InetAddress> endpoints = pendingRangeMaps.pendingEndpointsFor(new BigIntegerToken("6"));
        assertTrue(endpoints.contains(InetAddress.getByName("127.0.0.1")));
        assertTrue(endpoints.contains(InetAddress.getByName("127.0.0.7")));
    }
}
