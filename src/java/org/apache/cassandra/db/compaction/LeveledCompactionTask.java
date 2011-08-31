package org.apache.cassandra.db.compaction;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.sstable.SSTableWriter;

public class LeveledCompactionTask extends CompactionTask
{
    private final int sstableSizeInMB;

    private final CountDownLatch latch = new CountDownLatch(1);

    public LeveledCompactionTask(ColumnFamilyStore cfs, Collection<SSTableReader> sstables, final int gcBefore, int sstableSizeInMB)
    {
        super(cfs, sstables, gcBefore);
        this.sstableSizeInMB = sstableSizeInMB;
    }

    @Override
    public int execute(CompactionManager.CompactionExecutorStatsCollector collector) throws IOException
    {
        int n = super.execute(collector);
        latch.countDown();
        return n;
    }

    public boolean isDone()
    {
        return latch.getCount() == 0;
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
