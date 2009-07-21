package org.apache.cassandra.db.marshal;

public class MarshalException extends RuntimeException
{
    public MarshalException(String message)
    {
        super(message);
    }
}
