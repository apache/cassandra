package org.apache.cassandra.db.compaction;

import java.util.Collection;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.CompactionTask;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.sstable.SSTableWriter;

public class LeveledCompactionTask extends CompactionTask
{
    private final int sstableSizeInMB;

    public LeveledCompactionTask(ColumnFamilyStore cfs, Collection<SSTableReader> sstables, final int gcBefore, int sstableSizeInMB)
    {
        super(cfs, sstables, gcBefore);
        this.sstableSizeInMB = sstableSizeInMB;
    }

    @Override
    protected boolean newSSTableSegmentThresholdReached(SSTableWriter writer, long position)
    {
        return position > sstableSizeInMB * 1024 * 1024;
    }

    @Override
    protected boolean allowSingletonCompaction()
    {
        return true;
    }
}
