package org.apache.cassandra.db.compaction;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.utils.concurrent.Refs;

public class TenantAwaredSplitCompactionTask extends CompactionTask
{

    private String tenantId;
    private boolean split;
    private Iterable<SSTableReader> sstables;

    public TenantAwaredSplitCompactionTask(ColumnFamilyStore cfs, Iterable<SSTableReader> sstables, String tenantId,
            boolean split,
            int gcBefore)
    {
        super(cfs, sstables, gcBefore, false);
        this.tenantId = tenantId;
        this.sstables = sstables;
        this.split = split;
    }

    protected String getTenantId()
    {
        return tenantId;
    }

    @Override
    protected void runMayThrow() throws Exception
    {
        if (!split)
        {
            super.runMayThrow();
        }
        else
        {
            // customized do-split-compaction..
            Refs<SSTableReader> refs = Refs.tryRef(sstables);
            CompactionManager.instance.doSplitCompaction(cfs, refs);
        }
    }
}
