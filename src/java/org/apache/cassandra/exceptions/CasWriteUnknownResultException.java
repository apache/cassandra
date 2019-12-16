package org.apache.cassandra.exceptions;

import org.apache.cassandra.db.ConsistencyLevel;

public class CasWriteUnknownResultException extends RequestExecutionException
{
    public final ConsistencyLevel consistency;
    public final int received;
    public final int blockFor;

    public CasWriteUnknownResultException(ConsistencyLevel consistency, int received, int blockFor)
    {
        super(ExceptionCode.CAS_WRITE_UNKNOWN, String.format("CAS operation result is unknown - proposal accepted by %d but not a quorum.", received));
        this.consistency = consistency;
        this.received = received;
        this.blockFor = blockFor;
    }
}
