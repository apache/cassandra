package org.apache.cassandra.dht;

import java.math.BigInteger;

public class BigIntegerToken extends Token<BigInteger>
{
    public BigIntegerToken(BigInteger token)
    {
        super(token);
    }

    // convenience method for testing
    public BigIntegerToken(String token) {
        this(new BigInteger(token));
    }
}
