package org.apache.cassandra.db.compaction;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.DebuggableScheduledThreadPoolExecutor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataTracker;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.notifications.INotification;
import org.apache.cassandra.notifications.INotificationConsumer;
import org.apache.cassandra.notifications.SSTableAddedNotification;
import org.apache.cassandra.notifications.SSTableListChangedNotification;
import org.apache.cassandra.service.StorageService;

public class LeveledCompactionStrategy extends AbstractCompactionStrategy implements INotificationConsumer
{
    private static final Logger logger = LoggerFactory.getLogger(LeveledCompactionStrategy.class);

    private LeveledManifest manifest;
    private final String SSTABLE_SIZE_OPTION = "sstable_size_in_mb";
    private final int maxSSTableSize;

    public class ScheduledBackgroundCompaction implements Runnable
    {
        ColumnFamilyStore cfs;

        public ScheduledBackgroundCompaction(ColumnFamilyStore cfs)
        {
            this.cfs = cfs;
        }

        public void run()
        {
            if (CompactionManager.instance.getActiveCompactions() == 0)
            {
                CompactionManager.instance.submitBackground(cfs);
            }
        }
    }

    public LeveledCompactionStrategy(ColumnFamilyStore cfs, Map<String, String> options)
    {
        super(cfs, options);
        int configuredMaxSSTableSize = 5;
        if (options != null)
        {
            String value = options.containsKey(SSTABLE_SIZE_OPTION) ? options.get(SSTABLE_SIZE_OPTION) : null;
            if (null != value)
            {
                try
                {
                    configuredMaxSSTableSize = Integer.parseInt(value);
                }
                catch (NumberFormatException ex)
                {
                    logger.warn(String.format("%s is not a parsable int (base10) for %s using default value",
                                              value, SSTABLE_SIZE_OPTION));
                }
            }
        }
        maxSSTableSize = configuredMaxSSTableSize;

        cfs.getDataTracker().subscribe(this);
        logger.info(this + " subscribed to the data tracker.");

        manifest = LeveledManifest.create(cfs, this.maxSSTableSize);
        // override min/max for this strategy
        cfs.setMaximumCompactionThreshold(Integer.MAX_VALUE);
        cfs.setMinimumCompactionThreshold(1);

        DebuggableScheduledThreadPoolExecutor st = StorageService.scheduledTasks;
        st.scheduleAtFixedRate(new ScheduledBackgroundCompaction(cfs), 10000, 3000, TimeUnit.MILLISECONDS);
    }

    public void shutdown()
    {
        cfs.getDataTracker().unsubscribe(this);
    }

    public int getLevelSize(int i)
    {
        return manifest.getLevelSize(i);
    }

    public synchronized List<AbstractCompactionTask> getBackgroundTasks(int gcBefore)
    {
        Collection<SSTableReader> sstables = manifest.getCompactionCandidates();
        logger.debug("CompactionManager candidates are {}", StringUtils.join(sstables, ","));
        if (sstables.isEmpty())
            return Collections.emptyList();
        LeveledCompactionTask task = new LeveledCompactionTask(cfs, sstables, gcBefore, this.maxSSTableSize);
        return Collections.<AbstractCompactionTask>singletonList(task);
    }

    public List<AbstractCompactionTask> getMaximalTasks(int gcBefore)
    {
        return Collections.emptyList();
    }

    public AbstractCompactionTask getUserDefinedTask(Collection<SSTableReader> sstables, int gcBefore)
    {
        throw new UnsupportedOperationException("LevelDB compaction strategy does not allow user-specified compactions");
    }

    public int getEstimatedRemainingTasks()
    {
        return 0;
    }

    public void handleNotification(INotification notification, Object sender)
    {
        if (notification instanceof SSTableAddedNotification)
        {
            SSTableAddedNotification flushedNotification = (SSTableAddedNotification) notification;
            manifest.add(flushedNotification.added);
            manifest.logDistribution();
        }
        else if (notification instanceof SSTableListChangedNotification)
        {
            SSTableListChangedNotification listChangedNotification = (SSTableListChangedNotification) notification;
            manifest.promote(listChangedNotification.removed, listChangedNotification.added);
            manifest.logDistribution();
        }
    }
}