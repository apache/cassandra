package org.apache.cassandra.dht;

import org.testng.annotations.Test;

public class RangeTest {
    @Test
    public void testRange() {
        Range left = new Range(new BigIntegerToken("0"), new BigIntegerToken("100"));
        assert left.contains(new BigIntegerToken("10"));
        assert !left.contains(new BigIntegerToken("-1"));
        assert !left.contains(new BigIntegerToken("101"));

        Range right = new Range(new BigIntegerToken("100"), new BigIntegerToken("0"));
        assert right.contains(new BigIntegerToken("200"));
        assert right.contains(new BigIntegerToken("-10"));
        assert !right.contains(new BigIntegerToken("1"));
    }
}
