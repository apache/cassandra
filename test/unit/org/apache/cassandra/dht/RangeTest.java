package org.apache.cassandra.dht;

import org.testng.annotations.Test;

import java.math.BigInteger;

public class RangeTest {
    @Test
    public void testRange() {
        Range left = new Range(new BigInteger("0"), new BigInteger("100"));
        assert left.contains(new BigInteger("10"));
        assert !left.contains(new BigInteger("-1"));
        assert !left.contains(new BigInteger("101"));

        Range right = new Range(new BigInteger("100"), new BigInteger("0"));
        assert right.contains(new BigInteger("200"));
        assert right.contains(new BigInteger("-10"));
        assert !right.contains(new BigInteger("1"));
    }
}
