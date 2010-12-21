package org.apache.cassandra.thrift;

import org.junit.Test;

import org.apache.cassandra.CleanupHelper;

public class ThriftValidationTest extends CleanupHelper
{
    @Test(expected=InvalidRequestException.class)
    public void testValidateCommutativeWithStandard() throws InvalidRequestException
    {
        ThriftValidation.validateCommutative("Keyspace1", "Standard1");
    }

    @Test
    public void testValidateCommutativeWithCounter() throws InvalidRequestException
    {
        ThriftValidation.validateCommutative("Keyspace1", "Counter1");
    }
}
