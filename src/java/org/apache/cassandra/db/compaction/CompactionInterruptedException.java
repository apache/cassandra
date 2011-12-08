package org.apache.cassandra.db.compaction;

public class CompactionInterruptedException extends RuntimeException
{
    private static final long serialVersionUID = -8651427062512310398L;

    public CompactionInterruptedException(CompactionInfo info)
    {
        super("Compaction interrupted: " + info);
    }
}
