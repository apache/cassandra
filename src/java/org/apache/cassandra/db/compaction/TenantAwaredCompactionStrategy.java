package org.apache.cassandra.db.compaction;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TenantAwaredCompactionStrategy extends AbstractCompactionStrategy
{
    private static final Logger logger = LoggerFactory.getLogger(TenantAwaredCompactionStrategy.class);

    private final TenantAwaredManifest manifest;

    public TenantAwaredCompactionStrategy(ColumnFamilyStore cfs, Map<String, String> options)
    {
        super(cfs, options);
        manifest = new TenantAwaredManifest(cfs);
    }

    @Override
    public AbstractCompactionTask getNextBackgroundTask(int gcBefore)
    {
        Collection<AbstractCompactionTask> tasks = getMaximalTask(gcBefore);
        if (tasks == null || tasks.size() == 0)
            return null;
        return tasks.iterator().next();
    }

    @Override
    public Collection<AbstractCompactionTask> getMaximalTask(int gcBefore)
    {
        while (true)
        {
            AbstractCompactionTask task = manifest.getCompactionCandidates(gcBefore);

            if (task == null)
            {
                return null;
            }
            else
            {
                return Arrays.asList(task);
            }
        }
    }

    @Override
    public AbstractCompactionTask getUserDefinedTask(Collection<SSTableReader> sstables, int gcBefore)
    {
        throw new UnsupportedOperationException(
                "TenantAwared compaction strategy does not allow user-specified compactions");
    }

    @Override
    public int getEstimatedRemainingTasks()
    {
        return manifest.getEstimatedTasks();
    }

    @Override
    public long getMaxSSTableBytes()
    {
        // same as SizeTired Compaction
        return Long.MAX_VALUE;
    }

    @Override
    public void addSSTable(SSTableReader added)
    {
        manifest.add(added);
    }

    @Override
    public void removeSSTable(SSTableReader sstable)
    {
        manifest.remove(sstable);
    }

}
