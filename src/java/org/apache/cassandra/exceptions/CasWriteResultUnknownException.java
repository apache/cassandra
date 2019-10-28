package org.apache.cassandra.exceptions;

import org.apache.cassandra.db.ConsistencyLevel;

public class CasWriteResultUnknownException extends RequestExecutionException
{
    public final ConsistencyLevel consistency;
    public final int received;
    public final int blockFor;

    public CasWriteResultUnknownException(ConsistencyLevel consistency, int received, int blockFor)
    {
        super(ExceptionCode.CAS_WRITE_RESULT_UNKNOWNN, String.format("Cas operation result is unknown - proposal accepted by %d but not a quorum.", received));
        this.consistency = consistency;
        this.received = received;
        this.blockFor = blockFor;
    }
}
