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


import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.notifications.INotification;
import org.apache.cassandra.notifications.INotificationConsumer;
import org.apache.cassandra.notifications.SSTableAddedNotification;
import org.apache.cassandra.notifications.SSTableListChangedNotification;
import org.apache.cassandra.service.StorageService;

public class LeveledCompactionStrategy extends AbstractCompactionStrategy implements INotificationConsumer
{
    private static final Logger logger = LoggerFactory.getLogger(LeveledCompactionStrategy.class);

    private final LeveledManifest manifest;
    private final String SSTABLE_SIZE_OPTION = "sstable_size_in_mb";
    private final int maxSSTableSizeInMB;
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
        maxSSTableSizeInMB = configuredMaxSSTableSize;

        cfs.getDataTracker().subscribe(this);
        logger.debug("{} subscribed to the data tracker.", this);

        manifest = LeveledManifest.create(cfs, this.maxSSTableSizeInMB);
        logger.debug("Created {}", manifest);
        // override min/max for this strategy
        cfs.setMaximumCompactionThreshold(Integer.MAX_VALUE);
        cfs.setMinimumCompactionThreshold(1);
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

        LeveledCompactionTask newTask = new LeveledCompactionTask(cfs, sstables, gcBefore, this.maxSSTableSizeInMB);
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
        return manifest.getEstimatedTasks();
    }

    public void handleNotification(INotification notification, Object sender)
    {
        if (notification instanceof SSTableAddedNotification)
        {
            SSTableAddedNotification flushedNotification = (SSTableAddedNotification) notification;
            manifest.add(flushedNotification.added);
        }
        else if (notification instanceof SSTableListChangedNotification)
        {
            SSTableListChangedNotification listChangedNotification = (SSTableListChangedNotification) notification;
            manifest.promote(listChangedNotification.removed, listChangedNotification.added);
        }
    }

    public long getMaxSSTableSize()
    {
        return maxSSTableSizeInMB * 1024L * 1024L;
    }

    public boolean isKeyExistenceExpensive(Set<? extends SSTable> sstablesToIgnore)
    {
        Set<SSTableReader> L0 = ImmutableSet.copyOf(manifest.getLevel(0));
        return Sets.difference(L0, sstablesToIgnore).size() + manifest.getLevelCount() > 20;
    }

    @Override
    public String toString()
    {
        return String.format("LCS@%d(%s)", hashCode(), cfs.columnFamily);
    }
}
