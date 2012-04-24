package org.apache.cassandra.cql.jdbc;

import java.math.BigDecimal;
import java.nio.ByteBuffer;

import org.junit.Assert;
import org.junit.Test;

public class JdbcDecimalTest
{
    @Test
    public void testComposeDecompose()
    {
        BigDecimal expected = new BigDecimal("123456789123456789.987654321");
        JdbcDecimal decimal = new JdbcDecimal();
        
        ByteBuffer buffer = decimal.decompose(expected);     
        BigDecimal actual = decimal.compose(buffer);
        Assert.assertEquals(expected, actual);
    }
}
