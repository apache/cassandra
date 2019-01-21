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
import java.nio.ByteBuffer;
import java.util.*;
import java.util.zip.CRC32;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.db.*;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.io.util.BufferedDataOutputStreamPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputBufferFixed;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.metrics.CommitLogMetrics;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.security.EncryptionContext;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.MBeanWrapper;

import static org.apache.cassandra.db.commitlog.CommitLogSegment.Allocation;
import static org.apache.cassandra.db.commitlog.CommitLogSegment.CommitLogSegmentFileComparator;
import static org.apache.cassandra.db.commitlog.CommitLogSegment.ENTRY_OVERHEAD_SIZE;
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
    final long MAX_MUTATION_SIZE = DatabaseDescriptor.getMaxMutationSize();

    final public AbstractCommitLogSegmentManager segmentManager;

    public final CommitLogArchiver archiver;
    final CommitLogMetrics metrics;
    final AbstractCommitLogService executor;

    volatile Configuration configuration;

    private static CommitLog construct()
    {
        CommitLog log = new CommitLog(CommitLogArchiver.construct());

        MBeanWrapper.instance.registerMBean(log, "org.apache.cassandra.db:type=Commitlog");
        return log.start();
    }

    @VisibleForTesting
    CommitLog(CommitLogArchiver archiver)
    {
        this.configuration = new Configuration(DatabaseDescriptor.getCommitLogCompression(),
                                               DatabaseDescriptor.getEncryptionContext());
        DatabaseDescriptor.createAllDirectories();

        this.archiver = archiver;
        metrics = new CommitLogMetrics();

        executor = DatabaseDescriptor.getCommitLogSync() == Config.CommitLogSync.batch
                ? new BatchCommitLogService(this)
                : new PeriodicCommitLogService(this);

        segmentManager = DatabaseDescriptor.isCDCEnabled()
                         ? new CommitLogSegmentManagerCDC(this, DatabaseDescriptor.getCommitLogLocation())
                         : new CommitLogSegmentManagerStandard(this, DatabaseDescriptor.getCommitLogLocation());

        // register metrics
        metrics.attach(executor, segmentManager);
    }

    CommitLog start()
    {
        segmentManager.start();
        executor.start();
        return this;
    }

    /**
     * Perform recovery on commit logs located in the directory specified by the config file.
     *
     * @return the number of mutations replayed
     * @throws IOException
     */
    public int recoverSegmentsOnDisk() throws IOException
    {
        FilenameFilter unmanagedFilesFilter = (dir, name) -> CommitLogDescriptor.isValid(name) && CommitLogSegment.shouldReplay(name);

        // submit all files for this segment manager for archiving prior to recovery - CASSANDRA-6904
        // The files may have already been archived by normal CommitLog operation. This may cause errors in this
        // archiving pass, which we should not treat as serious. 
        for (File file : new File(segmentManager.storageDirectory).listFiles(unmanagedFilesFilter))
        {
            archiver.maybeArchive(file.getPath(), file.getName());
            archiver.maybeWaitForArchiving(file.getName());
        }

        assert archiver.archivePending.isEmpty() : "Not all commit log archive tasks were completed before restore";
        archiver.maybeRestoreArchive();

        // List the files again as archiver may have added segments.
        File[] files = new File(segmentManager.storageDirectory).listFiles(unmanagedFilesFilter);
        int replayed = 0;
        if (files.length == 0)
        {
            logger.info("No commitlog files found; skipping replay");
        }
        else
        {
            Arrays.sort(files, new CommitLogSegmentFileComparator());
            logger.info("Replaying {}", StringUtils.join(files, ", "));
            replayed = recoverFiles(files);
            logger.info("Log replay complete, {} replayed mutations", replayed);

            for (File f : files)
                segmentManager.handleReplayedSegment(f);
        }

        return replayed;
    }

    /**
     * Perform recovery on a list of commit log files.
     *
     * @param clogs   the list of commit log files to replay
     * @return the number of mutations replayed
     */
    public int recoverFiles(File... clogs) throws IOException
    {
        CommitLogReplayer replayer = CommitLogReplayer.construct(this);
        replayer.replayFiles(clogs);
        return replayer.blockForWrites();
    }

    public void recoverPath(String path) throws IOException
    {
        CommitLogReplayer replayer = CommitLogReplayer.construct(this);
        replayer.replayPath(new File(path), false);
        replayer.blockForWrites();
    }

    /**
     * Perform recovery on a single commit log. Kept w/sub-optimal name due to coupling w/MBean / JMX
     */
    public void recover(String path) throws IOException
    {
        recoverPath(path);
    }

    /**
     * @return a CommitLogPosition which, if {@code >= one} returned from add(), implies add() was started
     * (but not necessarily finished) prior to this call
     */
    public CommitLogPosition getCurrentPosition()
    {
        return segmentManager.getCurrentPosition();
    }

    /**
     * Flushes all dirty CFs, waiting for them to free and recycle any segments they were retaining
     */
    public void forceRecycleAllSegments(Iterable<UUID> droppedCfs)
    {
        segmentManager.forceRecycleAll(droppedCfs);
    }

    /**
     * Flushes all dirty CFs, waiting for them to free and recycle any segments they were retaining
     */
    public void forceRecycleAllSegments()
    {
        segmentManager.forceRecycleAll(Collections.<UUID>emptyList());
    }

    /**
     * Forces a disk flush on the commit log files that need it.  Blocking.
     */
    public void sync(boolean flush) throws IOException
    {
        segmentManager.sync(flush);
    }

    /**
     * Preempts the CLExecutor, telling to to sync immediately
     */
    public void requestExtraSync()
    {
        executor.requestExtraSync();
    }

    /**
     * Add a Mutation to the commit log. If CDC is enabled, this can fail.
     *
     * @param mutation the Mutation to add to the log
     * @throws WriteTimeoutException
     */
    public CommitLogPosition add(Mutation mutation) throws WriteTimeoutException
    {
        assert mutation != null;

        try (DataOutputBuffer dob = DataOutputBuffer.scratchBuffer.get())
        {
            Mutation.serializer.serialize(mutation, dob, MessagingService.current_version);
            int size = dob.getLength();

            int totalSize = size + ENTRY_OVERHEAD_SIZE;
            if (totalSize > MAX_MUTATION_SIZE)
            {
                throw new IllegalArgumentException(String.format("Mutation of %s is too large for the maximum size of %s",
                                                                 FBUtilities.prettyPrintMemory(totalSize),
                                                                 FBUtilities.prettyPrintMemory(MAX_MUTATION_SIZE)));
            }

            Allocation alloc = segmentManager.allocate(mutation, totalSize);

            CRC32 checksum = new CRC32();
            final ByteBuffer buffer = alloc.getBuffer();
            try (BufferedDataOutputStreamPlus dos = new DataOutputBufferFixed(buffer))
            {
                // checksummed length
                dos.writeInt(size);
                updateChecksumInt(checksum, size);
                buffer.putInt((int) checksum.getValue());

                // checksummed mutation
                dos.write(dob.getData(), 0, size);
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
            return alloc.getCommitLogPosition();
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, segmentManager.allocatingFrom().getPath());
        }
    }

    /**
     * Modifies the per-CF dirty cursors of any commit log segments for the column family according to the position
     * given. Discards any commit log segments that are no longer used.
     *
     * @param cfId    the column family ID that was flushed
     * @param lowerBound the lowest covered replay position of the flush
     * @param lowerBound the highest covered replay position of the flush
     */
    public void discardCompletedSegments(final UUID cfId, final CommitLogPosition lowerBound, final CommitLogPosition upperBound)
    {
        logger.trace("discard completed log segments for {}-{}, table {}", lowerBound, upperBound, cfId);

        // Go thru the active segment files, which are ordered oldest to newest, marking the
        // flushed CF as clean, until we reach the segment file containing the CommitLogPosition passed
        // in the arguments. Any segments that become unused after they are marked clean will be
        // recycled or discarded.
        for (Iterator<CommitLogSegment> iter = segmentManager.getActiveSegments().iterator(); iter.hasNext();)
        {
            CommitLogSegment segment = iter.next();
            segment.markClean(cfId, lowerBound, upperBound);

            if (segment.isUnused())
            {
                logger.debug("Commit log segment {} is unused", segment);
                segmentManager.archiveAndDiscard(segment);
            }
            else
            {
                if (logger.isTraceEnabled())
                    logger.trace("Not safe to delete{} commit log segment {}; dirty is {}",
                            (iter.hasNext() ? "" : " active"), segment, segment.dirtyString());
            }

            // Don't mark or try to delete any newer segments once we've reached the one containing the
            // position of the flush.
            if (segment.contains(upperBound))
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
        for (CommitLogSegment seg : segmentManager.getActiveSegments())
            segmentNames.add(seg.getName());
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
        for (CommitLogSegment seg : segmentManager.getActiveSegments())
            size += seg.contentSize();
        return size;
    }

    @Override
    public long getActiveOnDiskSize()
    {
        return segmentManager.onDiskSize();
    }

    @Override
    public Map<String, Double> getActiveSegmentCompressionRatios()
    {
        Map<String, Double> segmentRatios = new TreeMap<>();
        for (CommitLogSegment seg : segmentManager.getActiveSegments())
            segmentRatios.put(seg.getName(), 1.0 * seg.onDiskSize() / seg.contentSize());
        return segmentRatios;
    }

    /**
     * Shuts down the threads used by the commit log, blocking until completion.
     */
    public void shutdownBlocking() throws InterruptedException
    {
        executor.shutdown();
        executor.awaitTermination();
        segmentManager.shutdown();
        segmentManager.awaitTermination();
    }

    /**
     * FOR TESTING PURPOSES
     * @return the number of files recovered
     */
    public int resetUnsafe(boolean deleteSegments) throws IOException
    {
        stopUnsafe(deleteSegments);
        resetConfiguration();
        return restartUnsafe();
    }

    /**
     * FOR TESTING PURPOSES.
     */
    public void resetConfiguration()
    {
        configuration = new Configuration(DatabaseDescriptor.getCommitLogCompression(),
                                          DatabaseDescriptor.getEncryptionContext());
    }

    /**
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
        segmentManager.stopUnsafe(deleteSegments);
        CommitLogSegment.resetReplayLimit();
        if (DatabaseDescriptor.isCDCEnabled() && deleteSegments)
            for (File f : new File(DatabaseDescriptor.getCDCLogLocation()).listFiles())
                FileUtils.deleteWithConfirm(f);

    }

    /**
     * FOR TESTING PURPOSES
     */
    public int restartUnsafe() throws IOException
    {
        return start().recoverSegmentsOnDisk();
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

    public static final class Configuration
    {
        /**
         * The compressor class.
         */
        private final ParameterizedClass compressorClass;

        /**
         * The compressor used to compress the segments.
         */
        private final ICompressor compressor;

        /**
         * The encryption context used to encrypt the segments.
         */
        private EncryptionContext encryptionContext;

        public Configuration(ParameterizedClass compressorClass, EncryptionContext encryptionContext)
        {
            this.compressorClass = compressorClass;
            this.compressor = compressorClass != null ? CompressionParams.createCompressor(compressorClass) : null;
            this.encryptionContext = encryptionContext;
        }

        /**
         * Checks if the segments must be compressed.
         * @return <code>true</code> if the segments must be compressed, <code>false</code> otherwise.
         */
        public boolean useCompression()
        {
            return compressor != null;
        }

        /**
         * Checks if the segments must be encrypted.
         * @return <code>true</code> if the segments must be encrypted, <code>false</code> otherwise.
         */
        public boolean useEncryption()
        {
            return encryptionContext.isEnabled();
        }

        /**
         * Returns the compressor used to compress the segments.
         * @return the compressor used to compress the segments
         */
        public ICompressor getCompressor()
        {
            return compressor;
        }

        /**
         * Returns the compressor class.
         * @return the compressor class
         */
        public ParameterizedClass getCompressorClass()
        {
            return compressorClass;
        }

        /**
         * Returns the compressor name.
         * @return the compressor name.
         */
        public String getCompressorName()
        {
            return useCompression() ? compressor.getClass().getSimpleName() : "none";
        }

        /**
         * Returns the encryption context used to encrypt the segments.
         * @return the encryption context used to encrypt the segments
         */
        public EncryptionContext getEncryptionContext()
        {
            return encryptionContext;
        }
    }
}
