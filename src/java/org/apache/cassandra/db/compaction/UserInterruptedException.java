package org.apache.cassandra.db.compaction;

public class UserInterruptedException extends RuntimeException
{
    private static final long serialVersionUID = -8651427062512310398L;

    public UserInterruptedException(CompactionInfo info)
    {
        super(String.format("While processing %s", info));
    }
}
