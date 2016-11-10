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
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.db.WindowsFailedSnapshotTracker;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.util.FileUtils;

public class SnapshotDeletingTask implements Runnable
{
    private static final Logger logger = LoggerFactory.getLogger(SnapshotDeletingTask.class);

    public final File path;
    private static final Queue<Runnable> failedTasks = new ConcurrentLinkedQueue<>();

    public static void addFailedSnapshot(File path)
    {
        logger.warn("Failed to delete snapshot [{}]. Will retry after further sstable deletions. Folder will be deleted on JVM shutdown or next node restart on crash.", path);
        WindowsFailedSnapshotTracker.handleFailedSnapshot(path);
        failedTasks.add(new SnapshotDeletingTask(path));
    }

    private SnapshotDeletingTask(File path)
    {
        this.path = path;
    }

    public void run()
    {
        try
        {
            FileUtils.deleteRecursive(path);
            logger.info("Successfully deleted snapshot {}.", path);
        }
        catch (FSWriteError e)
        {
            failedTasks.add(this);
        }
    }

    /**
     * Retry all failed deletions.
     */
    public static void rescheduleFailedTasks()
    {
        Runnable task;
        while ( null != (task = failedTasks.poll()))
            ScheduledExecutors.nonPeriodicTasks.submit(task);
    }

    @VisibleForTesting
    public static int pendingDeletionCount()
    {
        return failedTasks.size();
    }
}
