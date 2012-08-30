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
package org.apache.cassandra.db.commitlog;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.util.*;
import org.apache.cassandra.metrics.CommitLogMetrics;
import org.apache.cassandra.net.MessagingService;

/*
 * Commit Log tracks every write operation into the system. The aim of the commit log is to be able to
 * successfully recover data that was not stored to disk via the Memtable.
 */
public class CommitLog implements CommitLogMBean
{
    private static final Logger logger = LoggerFactory.getLogger(CommitLog.class);

    public static final CommitLog instance = new CommitLog();

    private final ICommitLogExecutorService executor;

    public final CommitLogAllocator allocator;

    public final CommitLogArchiver archiver = new CommitLogArchiver();

    public static final int END_OF_SEGMENT_MARKER = 0;          // this is written out at the end of a segment
    public static final int END_OF_SEGMENT_MARKER_SIZE = 4;     // number of bytes of ^^^

    public CommitLogSegment activeSegment;

    private final CommitLogMetrics metrics;

    private CommitLog()
    {
        DatabaseDescriptor.createAllDirectories();

        allocator = new CommitLogAllocator();
        activateNextSegment();

        executor = DatabaseDescriptor.getCommitLogSync() == Config.CommitLogSync.batch
                 ? new BatchCommitLogExecutorService()
                 : new PeriodicCommitLogExecutorService(this);

        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try
        {
            mbs.registerMBean(this, new ObjectName("org.apache.cassandra.db:type=Commitlog"));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }

        // register metrics
        metrics = new CommitLogMetrics(executor, allocator);
    }

    /**
     * FOR TESTING PURPOSES. See CommitLogAllocator.
     */
    public void resetUnsafe()
    {
        allocator.resetUnsafe();
        activateNextSegment();
    }

    /**
     * Perform recovery on commit logs located in the directory specified by the config file.
     *
     * @return the number of mutations replayed
     */
    public int recover() throws IOException
    {
        archiver.maybeRestoreArchive();

        File[] files = new File(DatabaseDescriptor.getCommitLogLocation()).listFiles(new FilenameFilter()
        {
            public boolean accept(File dir, String name)
            {
                // we used to try to avoid instantiating commitlog (thus creating an empty segment ready for writes)
                // until after recover was finished.  this turns out to be fragile; it is less error-prone to go
                // ahead and allow writes before recover(), and just skip active segments when we do.
                return CommitLogDescriptor.isValid(name) && !instance.allocator.manages(name);
            }
        });

        int replayed = 0;
        if (files.length == 0)
        {
            logger.info("No commitlog files found; skipping replay");
        }
        else
        {
            Arrays.sort(files, new FileUtils.FileComparator());
            logger.info("Replaying " + StringUtils.join(files, ", "));
            replayed = recover(files);
            logger.info("Log replay complete, " + replayed + " replayed mutations");

            for (File f : files)
                CommitLog.instance.allocator.recycleSegment(f);
        }

        allocator.enableReserveSegmentCreation();
        return replayed;
    }

    /**
     * Perform recovery on a list of commit log files.
     *
     * @param clogs   the list of commit log files to replay
     * @return the number of mutations replayed
     */
    public int recover(File... clogs) throws IOException
    {
        CommitLogReplayer recovery = new CommitLogReplayer();
        recovery.recover(clogs);
        return recovery.blockForWrites();
    }

    /**
     * Perform recovery on a single commit log.
     */
    public void recover(String path) throws IOException
    {
        recover(new File(path));
    }

    /**
     * @return a Future representing a ReplayPosition such that when it is ready,
     * all commitlog tasks enqueued prior to the getContext call will be complete (i.e., appended to the log)
     */
    public Future<ReplayPosition> getContext()
    {
        Callable<ReplayPosition> task = new Callable<ReplayPosition>()
        {
            public ReplayPosition call()
            {
                return activeSegment.getContext();
            }
        };
        return executor.submit(task);
    }

    /**
     * Used by tests.
     *
     * @return the number of active segments (segments with unflushed data in them)
     */
    public int activeSegments()
    {
        return allocator.getActiveSegments().size();
    }

    /**
     * Add a RowMutation to the commit log.
     *
     * @param rm the RowMutation to add to the log
     */
    public void add(RowMutation rm)
    {
        long totalSize = RowMutation.serializer.serializedSize(rm, MessagingService.current_version) + CommitLogSegment.ENTRY_OVERHEAD_SIZE;
        if (totalSize > DatabaseDescriptor.getCommitLogSegmentSize())
        {
            logger.warn("Skipping commitlog append of extremely large mutation ({} bytes)", totalSize);
            return;
        }

        executor.add(new LogRecordAdder(rm));
    }

    /**
     * Modifies the per-CF dirty cursors of any commit log segments for the column family according to the position
     * given. Discards any commit log segments that are no longer used.
     *
     * @param cfId    the column family ID that was flushed
     * @param context the replay position of the flush
     */
    public void discardCompletedSegments(final UUID cfId, final ReplayPosition context)
    {
        Callable task = new Callable()
        {
            public Object call()
            {
                logger.debug("discard completed log segments for {}, column family {}", context, cfId);

                // Go thru the active segment files, which are ordered oldest to newest, marking the
                // flushed CF as clean, until we reach the segment file containing the ReplayPosition passed
                // in the arguments. Any segments that become unused after they are marked clean will be
                // recycled or discarded.
                for (Iterator<CommitLogSegment> iter = allocator.getActiveSegments().iterator(); iter.hasNext();)
                {
                    CommitLogSegment segment = iter.next();
                    segment.markClean(cfId, context);

                    // If the segment is no longer needed, and we have another spare segment in the hopper
                    // (to keep the last segment from getting discarded), pursue either recycling or deleting
                    // this segment file.
                    if (segment.isUnused() && iter.hasNext())
                    {
                        logger.debug("Commit log segment {} is unused", segment);
                        allocator.recycleSegment(segment);
                    }
                    else
                    {
                        if (logger.isDebugEnabled())
                            logger.debug(String.format("Not safe to delete commit log %s; dirty is %s; hasNext: %s",
                                                       segment, segment.dirtyString(), iter.hasNext()));
                    }

                    // Don't mark or try to delete any newer segments once we've reached the one containing the
                    // position of the flush.
                    if (segment.contains(context))
                        break;
                }

                return null;
            }
        };

        try
        {
            executor.submit(task).get();
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
        catch (ExecutionException e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Forces a disk flush on the commit log files that need it.
     */
    public void sync()
    {
        for (CommitLogSegment segment : allocator.getActiveSegments())
        {
            segment.sync();
        }
    }

    /**
     * @return the number of tasks completed by the commit log executor
     */
    public long getCompletedTasks()
    {
        return metrics.completedTasks.value();
    }

    /**
     * @return the depth of pending commit log executor queue
     */
    public long getPendingTasks()
    {
        return metrics.pendingTasks.value();
    }

    /**
     * @return the total size occupied by commitlo segments expressed in bytes. (used by MBean)
     */
    public long getTotalCommitlogSize()
    {
        return metrics.totalCommitLogSize.value();
    }

    /**
     * Forces a new segment file to be allocated and activated. Used mainly by truncate.
     */
    public void forceNewSegment() throws ExecutionException, InterruptedException
    {
        logger.debug("Forcing new segment creation");

        Callable<?> task = new Callable()
        {
            public Object call()
            {
                if (activeSegment.position() > 0)
                    activateNextSegment();

                return null;
            }
        };

        executor.submit(task).get();
    }

    /**
     * Fetches a new segment file from the allocator and activates it.
     *
     * @return the newly activated segment
     */
    private void activateNextSegment()
    {
        activeSegment = allocator.fetchSegment();
        logger.debug("Active segment is now {}", activeSegment);
    }

    public List<String> getActiveSegmentNames()
    {
        List<String> segmentNames = new ArrayList<String>();
        for (CommitLogSegment segment : allocator.getActiveSegments())
            segmentNames.add(segment.getName());
        return segmentNames;
    }

    public List<String> getArchivingSegmentNames()
    {
        return new ArrayList<String>(archiver.archivePending.keySet());
    }

    /**
     * Shuts down the threads used by the commit log, blocking until completion.
     */
    public void shutdownBlocking() throws InterruptedException
    {
        executor.shutdown();
        executor.awaitTermination();
        allocator.shutdown();
        allocator.awaitTermination();
    }

    // TODO this should be a Runnable since it doesn't actually return anything, but it's difficult to do that
    // without breaking the fragile CheaterFutureTask in BatchCLES.
    class LogRecordAdder implements Callable, Runnable
    {
        final RowMutation rowMutation;

        LogRecordAdder(RowMutation rm)
        {
            this.rowMutation = rm;
        }

        public void run()
        {
            if (!activeSegment.hasCapacityFor(rowMutation))
            {
                CommitLogSegment oldSegment = activeSegment;
                activateNextSegment();
                // Now we can run the user defined command just before switching to the new commit log.
                // (Do this here instead of in the recycle call so we can get a head start on the archive.)
                archiver.maybeArchive(oldSegment.getPath(), oldSegment.getName());
            }
            try
            {
                activeSegment.write(rowMutation);
            }
            catch (IOException e)
            {
                throw new FSWriteError(e, activeSegment.getPath());
            }
        }

        public Object call()
        {
            run();
            return null;
        }
    }
}
