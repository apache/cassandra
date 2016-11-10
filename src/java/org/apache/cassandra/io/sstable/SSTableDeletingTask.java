/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.io.sstable;

import java.io.File;
import java.util.Collections;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.Blocker;

public class SSTableDeletingTask implements Runnable
{
    private static final Logger logger = LoggerFactory.getLogger(SSTableDeletingTask.class);

    // Deleting sstables is tricky because the mmapping might not have been finalized yet,
    // and delete will fail (on Windows) until it is (we only force the unmapping on SUN VMs).
    // Additionally, we need to make sure to delete the data file first, so on restart the others
    // will be recognized as GCable.
    private static final Queue<Runnable> failedTasks = new ConcurrentLinkedQueue<>();
    private static final Blocker blocker = new Blocker();

    private final Descriptor desc;
    private final Set<Component> components;
    private final long bytesOnDisk;
    private final Counter totalDiskSpaceUsed;

    /**
     * realDescriptor is the actual descriptor for the sstable, the descriptor inside
     * referent can be 'faked' as FINAL for early opened files. We need the real one
     * to be able to remove the files.
     */
    public SSTableDeletingTask(Descriptor realDescriptor, Set<Component> components, Counter totalDiskSpaceUsed, long bytesOnDisk)
    {
        this.desc = realDescriptor;
        this.bytesOnDisk = bytesOnDisk;
        this.totalDiskSpaceUsed = totalDiskSpaceUsed;
        switch (desc.type)
        {
            case FINAL:
                this.components = components;
                break;
            case TEMPLINK:
                this.components = Sets.newHashSet(Component.DATA, Component.PRIMARY_INDEX);
                break;
            default:
                throw new IllegalStateException();
        }
    }

    public void run()
    {
        blocker.ask();
        // If we can't successfully delete the DATA component, set the task to be retried later: see above
        File datafile = new File(desc.filenameFor(Component.DATA));
        if (!datafile.delete())
        {
            logger.error("Unable to delete {} (it will be removed on server restart; we'll also retry after GC)", datafile);
            failedTasks.add(this);
            return;
        }
        // let the remainder be cleaned up by delete
        SSTable.delete(desc, Sets.difference(components, Collections.singleton(Component.DATA)));
        if (totalDiskSpaceUsed != null)
            totalDiskSpaceUsed.dec(bytesOnDisk);
    }

    /**
     * Retry all deletions that failed the first time around (presumably b/c the sstable was still mmap'd.)
     * Useful because there are times when we know GC has been invoked; also exposed as an mbean.
     */
    public static void rescheduleFailedTasks()
    {
        Runnable task;
        while ( null != (task = failedTasks.poll()))
            ScheduledExecutors.nonPeriodicTasks.submit(task);

        // On Windows, snapshots cannot be deleted so long as a segment of the root element is memory-mapped in NTFS.
        SnapshotDeletingTask.rescheduleFailedTasks();
    }

    /** for tests */
    @VisibleForTesting
    public static void waitForDeletions()
    {
        Runnable runnable = new Runnable()
        {
            public void run()
            {
            }
        };

        FBUtilities.waitOnFuture(ScheduledExecutors.nonPeriodicTasks.schedule(runnable, 0, TimeUnit.MILLISECONDS));
    }

    @VisibleForTesting
    public static void pauseDeletions(boolean stop)
    {
        blocker.block(stop);
    }
}

