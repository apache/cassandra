package org.apache.cassandra.db.compaction;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */


import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.DebuggableScheduledThreadPoolExecutor;
import org.apache.cassandra.db.ColumnFamilyStore;
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
    private final AtomicReference<LeveledCompactionTask> task = new AtomicReference<LeveledCompactionTask>();

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
        logger.debug("Created {}", manifest);
        // override min/max for this strategy
        cfs.setMaximumCompactionThreshold(Integer.MAX_VALUE);
        cfs.setMinimumCompactionThreshold(1);

        // TODO this is redundant wrt the kickoff in AbstractCompactionStrategy, once CASSANDRA-X is done
        Runnable runnable = new Runnable()
        {
            public void run()
            {
                CompactionManager.instance.submitBackground(LeveledCompactionStrategy.this.cfs);
            }
        };
        StorageService.optionalTasks.scheduleAtFixedRate(runnable, 5 * 60, 5, TimeUnit.SECONDS);
    }

    public void shutdown()
    {
        super.shutdown();
        cfs.getDataTracker().unsubscribe(this);
    }

    public int getLevelSize(int i)
    {
        return manifest.getLevelSize(i);
    }

    public List<AbstractCompactionTask> getBackgroundTasks(int gcBefore)
    {
        LeveledCompactionTask currentTask = task.get();
        if (currentTask != null && !currentTask.isDone())
        {
            logger.debug("Compaction still in progress for {}", this);
            return Collections.emptyList();
        }

        Collection<SSTableReader> sstables = manifest.getCompactionCandidates();
        if (sstables.isEmpty())
        {
            logger.debug("No compaction necessary for {}", this);
            return Collections.emptyList();
        }

        LeveledCompactionTask newTask = new LeveledCompactionTask(cfs, sstables, gcBefore, this.maxSSTableSize);
        return task.compareAndSet(currentTask, newTask)
               ? Collections.<AbstractCompactionTask>singletonList(newTask)
               : Collections.<AbstractCompactionTask>emptyList();
    }

    public List<AbstractCompactionTask> getMaximalTasks(int gcBefore)
    {
        return getBackgroundTasks(gcBefore);
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

    @Override
    public String toString()
    {
        return String.format("LCS@%d(%s)", hashCode(), cfs.columnFamily);
    }
}
