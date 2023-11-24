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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.FileStore;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.zip.CRC32;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.exceptions.CDCWriteException;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.io.util.BufferedDataOutputStreamPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputBufferFixed;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.PathUtils;
import org.apache.cassandra.metrics.CommitLogMetrics;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.security.EncryptionContext;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.MBeanWrapper;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

import static org.apache.cassandra.db.commitlog.CommitLogSegment.Allocation;
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

    private static final BiPredicate<File, String> unmanagedFilesFilter = (dir, name) -> CommitLogDescriptor.isValid(name) && CommitLogSegment.shouldReplay(name);

    final public AbstractCommitLogSegmentManager segmentManager;

    public final CommitLogArchiver archiver;
    public final CommitLogMetrics metrics;
    final AbstractCommitLogService executor;

    volatile Configuration configuration;
    private boolean started = false;

    private static CommitLog construct()
    {
        CommitLog log = new CommitLog(CommitLogArchiver.construct(), DatabaseDescriptor.getCommitLogSegmentMgrProvider());
        MBeanWrapper.instance.registerMBean(log, "org.apache.cassandra.db:type=Commitlog");
        return log;
    }

    @VisibleForTesting
    CommitLog(CommitLogArchiver archiver)
    {
        this(archiver, DatabaseDescriptor.getCommitLogSegmentMgrProvider());
    }

    @VisibleForTesting
    CommitLog(CommitLogArchiver archiver, Function<CommitLog, AbstractCommitLogSegmentManager> segmentManagerProvider)
    {
        this.configuration = new Configuration(DatabaseDescriptor.getCommitLogCompression(),
                                               DatabaseDescriptor.getEncryptionContext(),
                                               DatabaseDescriptor.getCommitLogWriteDiskAccessMode());
        DatabaseDescriptor.createAllDirectories();

        this.archiver = archiver;
        metrics = new CommitLogMetrics();

        switch (DatabaseDescriptor.getCommitLogSync())
        {
            case periodic:
                executor = new PeriodicCommitLogService(this);
                break;
            case batch:
                executor = new BatchCommitLogService(this);
                break;
            case group:
                executor = new GroupCommitLogService(this);
                break;
            default:
                throw new IllegalArgumentException("Unknown commitlog service type: " + DatabaseDescriptor.getCommitLogSync());
        }

        segmentManager = segmentManagerProvider.apply(this);

        // register metrics
        metrics.attach(executor, segmentManager);
    }

    /**
     * Tries to start the CommitLog if not already started.
     */
    synchronized public CommitLog start()
    {
        if (started)
            return this;

        try
        {
            segmentManager.start();
            executor.start();
            started = true;
        } catch (Throwable t)
        {
            started = false;
            throw t;
        }
        return this;
    }

    public boolean isStarted()
    {
        return started;
    }

    public boolean hasFilesToReplay()
    {
        return getUnmanagedFiles().length > 0;
    }

    private File[] getUnmanagedFiles()
    {
        File[] files = new File(segmentManager.storageDirectory).tryList(unmanagedFilesFilter);
        if (files == null)
            return new File[0];
        return files;
    }

    /**
     * Perform recovery on commit logs located in the directory specified by the config file.
     *
     * @return the number of mutations replayed
     * @throws IOException
     */
    public int recoverSegmentsOnDisk() throws IOException
    {
        // submit all files for this segment manager for archiving prior to recovery - CASSANDRA-6904
        // The files may have already been archived by normal CommitLog operation. This may cause errors in this
        // archiving pass, which we should not treat as serious.
        for (File file : getUnmanagedFiles())
        {
            archiver.maybeArchive(file.path(), file.name());
            archiver.maybeWaitForArchiving(file.name());
        }

        assert archiver.archivePending.isEmpty() : "Not all commit log archive tasks were completed before restore";
        archiver.maybeRestoreArchive();

        // List the files again as archiver may have added segments.
        File[] files = getUnmanagedFiles();
        int replayed = 0;
        if (files.length == 0)
        {
            logger.info("No commitlog files found; skipping replay");
        }
        else
        {
            Arrays.sort(files, new CommitLogSegment.CommitLogSegmentFileComparator());
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
        CommitLogReplayer replayer = CommitLogReplayer.construct(this, getLocalHostId());
        replayer.replayFiles(clogs);
        return replayer.blockForWrites();
    }

    public void recoverPath(String path) throws IOException
    {
        CommitLogReplayer replayer = CommitLogReplayer.construct(this, getLocalHostId());
        replayer.replayPath(new File(path), false);
        replayer.blockForWrites();
    }

    private static UUID getLocalHostId()
    {
        return StorageService.instance.getLocalHostUUID();
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
    public void forceRecycleAllSegments(Collection<TableId> droppedTables)
    {
        segmentManager.forceRecycleAll(droppedTables);
    }

    /**
     * Flushes all dirty CFs, waiting for them to free and recycle any segments they were retaining
     */
    public void forceRecycleAllSegments()
    {
        segmentManager.forceRecycleAll(Collections.emptyList());
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
     * @throws CDCWriteException
     */
    public CommitLogPosition add(Mutation mutation) throws CDCWriteException
    {
        assert mutation != null;

        mutation.validateSize(MessagingService.current_version, ENTRY_OVERHEAD_SIZE);

        try (DataOutputBuffer dob = DataOutputBuffer.scratchBuffer.get())
        {
            Mutation.serializer.serialize(mutation, dob, MessagingService.current_version);
            int size = dob.getLength();
            int totalSize = size + ENTRY_OVERHEAD_SIZE;
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
                dos.write(dob.unsafeGetBufferAndFlip());
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
     * @param id         the table that was flushed
     * @param lowerBound the lowest covered replay position of the flush
     * @param lowerBound the highest covered replay position of the flush
     */
    public void discardCompletedSegments(final TableId id, final CommitLogPosition lowerBound, final CommitLogPosition upperBound)
    {
        logger.trace("discard completed log segments for {}-{}, table {}", lowerBound, upperBound, id);

        // Go thru the active segment files, which are ordered oldest to newest, marking the
        // flushed CF as clean, until we reach the segment file containing the CommitLogPosition passed
        // in the arguments. Any segments that become unused after they are marked clean will be
        // recycled or discarded.
        for (Iterator<CommitLogSegment> iter = segmentManager.getActiveSegments().iterator(); iter.hasNext();)
        {
            CommitLogSegment segment = iter.next();
            segment.markClean(id, lowerBound, upperBound);

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
        Collection<CommitLogSegment> segments = segmentManager.getActiveSegments();
        List<String> segmentNames = new ArrayList<>(segments.size());
        for (CommitLogSegment seg : segments)
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

    @Override
    public boolean getCDCBlockWrites()
    {
        return DatabaseDescriptor.getCDCBlockWrites();
    }

    @Override
    public void setCDCBlockWrites(boolean val)
    {
        ensureCDCEnabled("Unable to set block_writes.");
        boolean oldVal = DatabaseDescriptor.getCDCBlockWrites();
        CommitLogSegment currentSegment = segmentManager.allocatingFrom();
        // Update the current segment CDC state to PERMITTED if block_writes is disabled now, and it was in FORBIDDEN state
        if (!val && currentSegment.getCDCState() == CommitLogSegment.CDCState.FORBIDDEN)
            currentSegment.setCDCState(CommitLogSegment.CDCState.PERMITTED);
        DatabaseDescriptor.setCDCBlockWrites(val);
        logger.info("Updated CDC block_writes from {} to {}", oldVal, val);
    }


    @Override
    public boolean isCDCOnRepairEnabled()
    {
        return DatabaseDescriptor.isCDCOnRepairEnabled();
    }

    @Override
    public void setCDCOnRepairEnabled(boolean value)
    {
        ensureCDCEnabled("Unable to set cdc_on_repair_enabled.");
        DatabaseDescriptor.setCDCOnRepairEnabled(value);
        logger.info("Set cdc_on_repair_enabled to {}", value);
    }

    private void ensureCDCEnabled(String hint)
    {
        Preconditions.checkState(DatabaseDescriptor.isCDCEnabled(), "CDC is not enabled. %s", hint);
        Preconditions.checkState(segmentManager instanceof CommitLogSegmentManagerCDC,
                                 "CDC is enabled but we have the wrong CommitLogSegmentManager type: %s. " +
                                 "Please report this as bug.", segmentManager.getClass().getName());
    }

    /**
     * Shuts down the threads used by the commit log, blocking until completion.
     * TODO this should accept a timeout, and throw TimeoutException
     */
    synchronized public void shutdownBlocking() throws InterruptedException
    {
        if (!started)
            return;

        started = false;
        executor.shutdown();
        executor.awaitTermination();
        segmentManager.shutdown();
        segmentManager.awaitTermination(1L, TimeUnit.MINUTES);
    }

    /**
     * FOR TESTING PURPOSES
     * @return the number of files recovered
     */
    @VisibleForTesting
    synchronized public int resetUnsafe(boolean deleteSegments) throws IOException
    {
        stopUnsafe(deleteSegments);
        resetConfiguration();
        return restartUnsafe();
    }

    /**
     * FOR TESTING PURPOSES.
     */
    @VisibleForTesting
    synchronized public void resetConfiguration()
    {
        configuration = new Configuration(DatabaseDescriptor.getCommitLogCompression(),
                                          DatabaseDescriptor.getEncryptionContext(),
                                          DatabaseDescriptor.getCommitLogWriteDiskAccessMode());
    }

    /**
     * FOR TESTING PURPOSES
     */
    @VisibleForTesting
    synchronized public void stopUnsafe(boolean deleteSegments)
    {
        if (!started)
            return;

        started = false;
        executor.shutdown();
        try
        {
            executor.awaitTermination();
        }
        catch (InterruptedException e)
        {
            throw new UncheckedInterruptedException(e);
        }
        segmentManager.stopUnsafe(deleteSegments);
        CommitLogSegment.resetReplayLimit();
        if (DatabaseDescriptor.isCDCEnabled() && deleteSegments)
            for (File f : new File(DatabaseDescriptor.getCDCLogLocation()).tryList())
                f.delete();
    }

    /**
     * FOR TESTING PURPOSES
     */
    @VisibleForTesting
    synchronized public int restartUnsafe() throws IOException
    {
        started = false;
        return start().recoverSegmentsOnDisk();
    }

    public static long freeDiskSpace()
    {
        return PathUtils.tryGetSpace(new File(DatabaseDescriptor.getCommitLogLocation()).toPath(), FileStore::getTotalSpace);
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
                String errorMsg = String.format("%s. Commit disk failure policy is %s; terminating thread.", message, DatabaseDescriptor.getCommitFailurePolicy());
                logger.error(addAdditionalInformationIfPossible(errorMsg), t);
                return false;
            case ignore:
                logger.error(addAdditionalInformationIfPossible(message), t);
                return true;
            default:
                throw new AssertionError(DatabaseDescriptor.getCommitFailurePolicy());
        }
    }

    /**
     * Add additional information to the error message if the commit directory does not have enough free space.
     *
     * @param msg the original error message
     * @return the message with additional information if possible
     */
    private static String addAdditionalInformationIfPossible(String msg)
    {
        long unallocatedSpace = freeDiskSpace();
        int segmentSize = DatabaseDescriptor.getCommitLogSegmentSize();

        if (unallocatedSpace < segmentSize)
        {
            return String.format("%s. %d bytes required for next commitlog segment but only %d bytes available. Check %s to see if not enough free space is the reason for this error.",
                                 msg, segmentSize, unallocatedSpace, DatabaseDescriptor.getCommitLogLocation());
        }
        return msg;
    }

    public static final class Configuration
    {
        /**
         * Flag used to shows user configured Direct-IO status.
         */
        public final Config.DiskAccessMode diskAccessMode;

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
        private final EncryptionContext encryptionContext;

        public Configuration(ParameterizedClass compressorClass, EncryptionContext encryptionContext,
                             Config.DiskAccessMode diskAccessMode)
        {
            this.compressorClass = compressorClass;
            this.compressor = compressorClass != null ? CompressionParams.createCompressor(compressorClass) : null;
            this.encryptionContext = encryptionContext;
            this.diskAccessMode = diskAccessMode;
        }

        /**
         * @return <code>true</code> if the segments must be compressed, <code>false</code> otherwise.
         */
        public boolean useCompression()
        {
            return compressor != null;
        }

        /**
         * @return <code>true</code> if the segments must be encrypted, <code>false</code> otherwise.
         */
        public boolean useEncryption()
        {
            return encryptionContext != null && encryptionContext.isEnabled();
        }

        /**
         * @return the compressor used to compress the segments
         */
        public ICompressor getCompressor()
        {
            return compressor;
        }

        /**
         * @return the compressor class
         */
        public ParameterizedClass getCompressorClass()
        {
            return compressorClass;
        }

        /**
         * @return the compressor name.
         */
        public String getCompressorName()
        {
            return useCompression() ? compressor.getClass().getSimpleName() : "none";
        }

        /**
         * @return the encryption context used to encrypt the segments
         */
        public EncryptionContext getEncryptionContext()
        {
            return encryptionContext;
        }

        /**
         * @return Direct-IO used for CommitLog IO
         */
        public boolean isDirectIOEnabled()
        {
            return diskAccessMode == Config.DiskAccessMode.direct;
        }

        /**
         * @return Standard or buffered I/O used for CommitLog IO
         */
        public boolean isStandardModeEnable()
        {
            return diskAccessMode == Config.DiskAccessMode.standard;
        }
    }
}
