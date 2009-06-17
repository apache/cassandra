package org.apache.cassandra.db;

import org.apache.cassandra.service.InvalidRequestException;

public class TableNotDefinedException extends InvalidRequestException
{
    public TableNotDefinedException(String why)
    {
        super(why);
    }
}
