package org.apache.cassandra.tools;

public class BulkLoadException extends Exception
{

    private static final long serialVersionUID = 1L;

    public BulkLoadException(Throwable cause)
    {
        super(cause);
    }

}
