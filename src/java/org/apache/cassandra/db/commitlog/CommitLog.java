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
import java.nio.ByteBuffer;
import java.util.*;
import java.util.zip.CRC32;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.StringUtils;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.db.*;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.io.util.BufferedDataOutputStreamPlus;
import org.apache.cassandra.io.util.DataOutputBufferFixed;
import org.apache.cassandra.metrics.CommitLogMetrics;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.security.EncryptionContext;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;

import static org.apache.cassandra.db.commitlog.CommitLogSegment.*;
import static org.apache.cassandra.utils.FBUtilities.updateChecksum;
import static org.apache.cassandra.utils.FBUtilities.updateChecksumInt;

/*
 * Commit Log tracks every write operation into the system. The aim of the commit log is to be able to
 * successfully recover data that was not stored to disk via the Memtable.
 */
public class CommitLog implements CommitLogMBean
{
    private static final Logger logger = LoggerFactory.getLogger(CommitLog.class);

    public static final CommitLog instance = CommitLog.construct();

    // we only permit records HALF the size of a commit log, to ensure we don't spin allocating many mostly
    // empty segments when writing large records
    private final long MAX_MUTATION_SIZE = DatabaseDescriptor.getMaxMutationSize();

    public final CommitLogSegmentManager allocator;
    public final CommitLogArchiver archiver;
    final CommitLogMetrics metrics;
    final AbstractCommitLogService executor;

    final ICompressor compressor;
    public ParameterizedClass compressorClass;
    public EncryptionContext encryptionContext;
    final public String location;

    private static CommitLog construct()
    {
        CommitLog log = new CommitLog(DatabaseDescriptor.getCommitLogLocation(), CommitLogArchiver.construct());

        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try
        {
            mbs.registerMBean(log, new ObjectName("org.apache.cassandra.db:type=Commitlog"));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
        return log.start();
    }

    @VisibleForTesting
    CommitLog(String location, CommitLogArchiver archiver)
    {
        compressorClass = DatabaseDescriptor.getCommitLogCompression();
        this.location = location;
        ICompressor compressor = compressorClass != null ? CompressionParams.createCompressor(compressorClass) : null;
        DatabaseDescriptor.createAllDirectories();
        encryptionContext = DatabaseDescriptor.getEncryptionContext();

        this.compressor = compressor;
        this.archiver = archiver;
        metrics = new CommitLogMetrics();

        executor = DatabaseDescriptor.getCommitLogSync() == Config.CommitLogSync.batch
                ? new BatchCommitLogService(this)
                : new PeriodicCommitLogService(this);

        allocator = new CommitLogSegmentManager(this);

        // register metrics
        metrics.attach(executor, allocator);
    }

    CommitLog start()
    {
        executor.start();
        allocator.start();
        return this;
    }

    /**
     * Perform recovery on commit logs located in the directory specified by the config file.
     *
     * @return the number of mutations replayed
     */
    public int recover() throws IOException
    {
        // If createReserveSegments is already flipped, the CLSM is running and recovery has already taken place.
        if (allocator.createReserveSegments)
            return 0;

        // Allocator could be in the process of initial startup with 0 active and available segments. We need to wait for
        // the allocation manager to finish allocation and add it to available segments so we don't get an invalid response
        // on allocator.manages(...) below by grabbing a file off the filesystem before it's added to the CLQ.
        allocator.allocatingFrom();

        FilenameFilter unmanagedFilesFilter = new FilenameFilter()
        {
            public boolean accept(File dir, String name)
            {
                // we used to try to avoid instantiating commitlog (thus creating an empty segment ready for writes)
                // until after recover was finished.  this turns out to be fragile; it is less error-prone to go
                // ahead and allow writes before recover(), and just skip active segments when we do.
                return CommitLogDescriptor.isValid(name) && !allocator.manages(name);
            }
        };

        // submit all existing files in the commit log dir for archiving prior to recovery - CASSANDRA-6904
        for (File file : new File(DatabaseDescriptor.getCommitLogLocation()).listFiles(unmanagedFilesFilter))
        {
            archiver.maybeArchive(file.getPath(), file.getName());
            archiver.maybeWaitForArchiving(file.getName());
        }

        assert archiver.archivePending.isEmpty() : "Not all commit log archive tasks were completed before restore";
        archiver.maybeRestoreArchive();

        File[] files = new File(DatabaseDescriptor.getCommitLogLocation()).listFiles(unmanagedFilesFilter);
        int replayed = 0;
        if (files.length == 0)
        {
            logger.info("No commitlog files found; skipping replay");
        }
        else
        {
            Arrays.sort(files, new CommitLogSegmentFileComparator());
            logger.info("Replaying {}", StringUtils.join(files, ", "));
            replayed = recover(files);
            logger.info("Log replay complete, {} replayed mutations", replayed);

            for (File f : files)
                allocator.recycleSegment(f);
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
        CommitLogReplayer recovery = CommitLogReplayer.construct(this);
        recovery.recover(clogs);
        return recovery.blockForWrites();
    }

    /**
     * Perform recovery on a single commit log.
     */
    public void recover(String path) throws IOException
    {
        CommitLogReplayer recovery = CommitLogReplayer.construct(this);
        recovery.recover(new File(path), false);
        recovery.blockForWrites();
    }

    /**
     * @return a ReplayPosition which, if {@code >= one} returned from add(), implies add() was started
     * (but not necessarily finished) prior to this call
     */
    public ReplayPosition getContext()
    {
        return allocator.allocatingFrom().getContext();
    }

    /**
     * Flushes all dirty CFs, waiting for them to free and recycle any segments they were retaining
     */
    public void forceRecycleAllSegments(Iterable<UUID> droppedCfs)
    {
        allocator.forceRecycleAll(droppedCfs);
    }

    /**
     * Flushes all dirty CFs, waiting for them to free and recycle any segments they were retaining
     */
    public void forceRecycleAllSegments()
    {
        allocator.forceRecycleAll(Collections.<UUID>emptyList());
    }

    /**
     * Forces a disk flush on the commit log files that need it.  Blocking.
     */
    public void sync(boolean syncAllSegments)
    {
        CommitLogSegment current = allocator.allocatingFrom();
        for (CommitLogSegment segment : allocator.getActiveSegments())
        {
            if (!syncAllSegments && segment.id > current.id)
                return;
            segment.sync();
        }
    }

    /**
     * Preempts the CLExecutor, telling to to sync immediately
     */
    public void requestExtraSync()
    {
        executor.requestExtraSync();
    }

    /**
     * Add a Mutation to the commit log.
     *
     * @param mutation the Mutation to add to the log
     */
    public ReplayPosition add(Mutation mutation)
    {
        assert mutation != null;

        int size = (int) Mutation.serializer.serializedSize(mutation, MessagingService.current_version);

        int totalSize = size + ENTRY_OVERHEAD_SIZE;
        if (totalSize > MAX_MUTATION_SIZE)
        {
            throw new IllegalArgumentException(String.format("Mutation of %s is too large for the maximum size of %s",
                                                             FBUtilities.prettyPrintMemory(totalSize),
                                                             FBUtilities.prettyPrintMemory(MAX_MUTATION_SIZE)));
        }

        Allocation alloc = allocator.allocate(mutation, (int) totalSize);
        CRC32 checksum = new CRC32();
        final ByteBuffer buffer = alloc.getBuffer();
        try (BufferedDataOutputStreamPlus dos = new DataOutputBufferFixed(buffer))
        {
            // checksummed length
            dos.writeInt(size);
            updateChecksumInt(checksum, size);
            buffer.putInt((int) checksum.getValue());

            // checksummed mutation
            Mutation.serializer.serialize(mutation, dos, MessagingService.current_version);
            updateChecksum(checksum, buffer, buffer.position() - size, size);
            buffer.putInt((int) checksum.getValue());
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, alloc.getSegment().getPath());
        }
        finally
        {
            alloc.markWritten();
        }

        executor.finishWriteFor(alloc);
        return alloc.getReplayPosition();
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
        logger.trace("discard completed log segments for {}, table {}", context, cfId);

        // Go thru the active segment files, which are ordered oldest to newest, marking the
        // flushed CF as clean, until we reach the segment file containing the ReplayPosition passed
        // in the arguments. Any segments that become unused after they are marked clean will be
        // recycled or discarded.
        for (Iterator<CommitLogSegment> iter = allocator.getActiveSegments().iterator(); iter.hasNext();)
        {
            CommitLogSegment segment = iter.next();
            segment.markClean(cfId, context);

            if (segment.isUnused())
            {
                logger.trace("Commit log segment {} is unused", segment);
                allocator.recycleSegment(segment);
            }
            else
            {
                logger.trace("Not safe to delete{} commit log segment {}; dirty is {}",
                        (iter.hasNext() ? "" : " active"), segment, segment.dirtyString());
            }

            // Don't mark or try to delete any newer segments once we've reached the one containing the
            // position of the flush.
            if (segment.contains(context))
                break;
        }
    }

    @Override
    public String getArchiveCommand()
    {
        return archiver.archiveCommand;
    }

    @Override
    public String getRestoreCommand()
    {
        return archiver.restoreCommand;
    }

    @Override
    public String getRestoreDirectories()
    {
        return archiver.restoreDirectories;
    }

    @Override
    public long getRestorePointInTime()
    {
        return archiver.restorePointInTime;
    }

    @Override
    public String getRestorePrecision()
    {
        return archiver.precision.toString();
    }

    public List<String> getActiveSegmentNames()
    {
        List<String> segmentNames = new ArrayList<>();
        for (CommitLogSegment segment : allocator.getActiveSegments())
            segmentNames.add(segment.getName());
        return segmentNames;
    }

    public List<String> getArchivingSegmentNames()
    {
        return new ArrayList<>(archiver.archivePending.keySet());
    }

    @Override
    public long getActiveContentSize()
    {
        long size = 0;
        for (CommitLogSegment segment : allocator.getActiveSegments())
            size += segment.contentSize();
        return size;
    }

    @Override
    public long getActiveOnDiskSize()
    {
        return allocator.onDiskSize();
    }

    @Override
    public Map<String, Double> getActiveSegmentCompressionRatios()
    {
        Map<String, Double> segmentRatios = new TreeMap<>();
        for (CommitLogSegment segment : allocator.getActiveSegments())
            segmentRatios.put(segment.getName(), 1.0 * segment.onDiskSize() / segment.contentSize());
        return segmentRatios;
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

    /**
     * FOR TESTING PURPOSES. See CommitLogAllocator.
     * @return the number of files recovered
     */
    public int resetUnsafe(boolean deleteSegments) throws IOException
    {
        stopUnsafe(deleteSegments);
        return restartUnsafe();
    }

    /**
     * FOR TESTING PURPOSES. See CommitLogAllocator.
     */
    public void stopUnsafe(boolean deleteSegments)
    {
        executor.shutdown();
        try
        {
            executor.awaitTermination();
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
        allocator.stopUnsafe(deleteSegments);
    }

    /**
     * FOR TESTING PURPOSES.  See CommitLogAllocator
     */
    public int restartUnsafe() throws IOException
    {
        allocator.start();
        executor.restartUnsafe();
        try
        {
            return recover();
        }
        catch (FSWriteError e)
        {
            // Workaround for a class of races that keeps showing up on Windows tests.
            // stop/start/reset path on Windows with segment deletion is very touchy/brittle
            // and the timing keeps getting screwed up. Rather than chasing our tail further
            // or rewriting the CLSM, just report that we didn't recover anything back up
            // the chain. This will silence most intermittent test failures on Windows
            // and appropriately fail tests that expected segments to be recovered that
            // were not.
            return 0;
        }
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

    @VisibleForTesting
    public static boolean handleCommitError(String message, Throwable t)
    {
        JVMStabilityInspector.inspectCommitLogThrowable(t);
        switch (DatabaseDescriptor.getCommitFailurePolicy())
        {
            // Needed here for unit tests to not fail on default assertion
            case die:
            case stop:
                StorageService.instance.stopTransports();
                //$FALL-THROUGH$
            case stop_commit:
                logger.error(String.format("%s. Commit disk failure policy is %s; terminating thread", message, DatabaseDescriptor.getCommitFailurePolicy()), t);
                return false;
            case ignore:
                logger.error(message, t);
                return true;
            default:
                throw new AssertionError(DatabaseDescriptor.getCommitFailurePolicy());
        }
    }
}
