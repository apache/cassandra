package org.apache.cassandra.exceptions;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.WriteType;

public class CasWriteTimeoutException extends WriteTimeoutException
{
    public final int contentions;

    public CasWriteTimeoutException(WriteType writeType, ConsistencyLevel consistency, int received, int blockFor, int contentions)
    {
        super(writeType, consistency, received, blockFor, String.format("CAS operation timed out - encountered contentions: %d", contentions));
        this.contentions = contentions;
    }
}
