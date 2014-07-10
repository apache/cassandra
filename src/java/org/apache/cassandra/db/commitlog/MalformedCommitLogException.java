package org.apache.cassandra.db.commitlog;

import java.io.IOException;

// represents a non-fatal commit log replay exception (i.e. can be skipped with -Dcassandra.commitlog.ignoreerrors=true)
public class MalformedCommitLogException extends IOException
{
    public MalformedCommitLogException(String message)
    {
        super(message);
    }
    public MalformedCommitLogException(String message, Throwable cause)
    {
        super(message, cause);
    }
}
