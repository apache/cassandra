/**
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import com.google.common.collect.Ordering;

import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.*;
import org.apache.cassandra.io.IColumnSerializer;
import org.apache.cassandra.io.util.*;
import org.apache.cassandra.net.MessagingService;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.FastByteArrayInputStream;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.WrappedRunnable;

import javax.management.MBeanServer;
import javax.management.ObjectName;

/*
 * Commit Log tracks every write operation into the system. The aim of the commit log is to be able to
 * successfully recover data that was not stored to disk via the Memtable.
 */
public class CommitLog implements CommitLogMBean
{
    private static final int MAX_OUTSTANDING_REPLAY_COUNT = 1024;
    
    static final Logger logger = LoggerFactory.getLogger(CommitLog.class);

    public static final CommitLog instance = new CommitLog();

    private final ICommitLogExecutorService executor;

    private final CommitLogAllocator allocator;

    public static final int END_OF_SEGMENT_MARKER = 0;          // this is written out at the end of a segment
    public static final int END_OF_SEGMENT_MARKER_SIZE = 4;     // number of bytes of ^^^

    /** size of commitlog segments to allocate */
    public static final int SEGMENT_SIZE = 128 * 1024 * 1024;
    public CommitLogSegment activeSegment;

    private CommitLog()
    {
        try
        {
            DatabaseDescriptor.createAllDirectories();

            allocator = new CommitLogAllocator();
            activateNextSegment();
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }

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
    }

    /**
     * FOR TESTING PURPOSES. See CommitLogAllocator.
     */
    public void resetUnsafe() throws IOException
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
        File[] files = new File(DatabaseDescriptor.getCommitLogLocation()).listFiles(new FilenameFilter()
        {
            public boolean accept(File dir, String name)
            {
                // we used to try to avoid instantiating commitlog (thus creating an empty segment ready for writes)
                // until after recover was finished.  this turns out to be fragile; it is less error-prone to go
                // ahead and allow writes before recover(), and just skip active segments when we do.
                return CommitLogSegment.possibleCommitLogFile(name) && !instance.allocator.manages(name);
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
    public int recover(File[] clogs) throws IOException
    {
        final Set<Table> tablesRecovered = new HashSet<Table>();
        List<Future<?>> futures = new ArrayList<Future<?>>();
        byte[] bytes = new byte[4096];
        Map<Integer, AtomicInteger> invalidMutations = new HashMap<Integer, AtomicInteger>();

        // count the number of replayed mutation. We don't really care about atomicity, but we need it to be a reference.
        final AtomicInteger replayedCount = new AtomicInteger();

        // compute per-CF and global replay positions
        final Map<Integer, ReplayPosition> cfPositions = new HashMap<Integer, ReplayPosition>();
        for (ColumnFamilyStore cfs : ColumnFamilyStore.all())
        {
            // it's important to call RP.gRP per-cf, before aggregating all the positions w/ the Ordering.min call
            // below: gRP will return NONE if there are no flushed sstables, which is important to have in the
            // list (otherwise we'll just start replay from the first flush position that we do have, which is not correct).
            ReplayPosition rp = ReplayPosition.getReplayPosition(cfs.getSSTables());
            cfPositions.put(cfs.metadata.cfId, rp);
        }
        final ReplayPosition globalPosition = Ordering.from(ReplayPosition.comparator).min(cfPositions.values());

        Checksum checksum = new CRC32();
        for (final File file : clogs)
        {
            logger.info("Replaying " + file.getPath());

            final long segment = CommitLogSegment.idFromFilename(file.getName());

            RandomAccessReader reader = RandomAccessReader.open(new File(file.getAbsolutePath()), true);
            assert reader.length() <= Integer.MAX_VALUE;

            try
            {
                int replayPosition;
                if (globalPosition.segment < segment)
                    replayPosition = 0;
                else if (globalPosition.segment == segment)
                    replayPosition = globalPosition.position;
                else
                    replayPosition = (int) reader.length();

                if (replayPosition < 0 || replayPosition >= reader.length())
                {
                    // replayPosition > reader.length() can happen if some data gets flushed before it is written to the commitlog
                    // (see https://issues.apache.org/jira/browse/CASSANDRA-2285)
                    logger.debug("skipping replay of fully-flushed {}", file);
                    continue;
                }

                reader.seek(replayPosition);

                if (logger.isDebugEnabled())
                    logger.debug("Replaying " + file + " starting at " + reader.getFilePointer());

                /* read the logs populate RowMutation and apply */
                while (!reader.isEOF())
                {
                    if (logger.isDebugEnabled())
                        logger.debug("Reading mutation at " + reader.getFilePointer());

                    long claimedCRC32;
                    int serializedSize;
                    try
                    {
                        // any of the reads may hit EOF
                        serializedSize = reader.readInt();
                        if (serializedSize == CommitLog.END_OF_SEGMENT_MARKER)
                        {
                            logger.debug("Encountered end of segment marker at " + reader.getFilePointer());
                            break;
                        }

                        // RowMutation must be at LEAST 10 bytes:
                        // 3 each for a non-empty Table and Key (including the 2-byte length from
                        // writeUTF/writeWithShortLength) and 4 bytes for column count.
                        // This prevents CRC by being fooled by special-case garbage in the file; see CASSANDRA-2128
                        if (serializedSize < 10)
                            break;
                        long claimedSizeChecksum = reader.readLong();
                        checksum.reset();
                        checksum.update(serializedSize);
                        if (checksum.getValue() != claimedSizeChecksum)
                            break; // entry wasn't synced correctly/fully.  that's ok.

                        if (serializedSize > bytes.length)
                            bytes = new byte[(int) (1.2 * serializedSize)];
                        reader.readFully(bytes, 0, serializedSize);
                        claimedCRC32 = reader.readLong();
                    }
                    catch(EOFException eof)
                    {
                        break; // last CL entry didn't get completely written.  that's ok.
                    }

                    checksum.update(bytes, 0, serializedSize);
                    if (claimedCRC32 != checksum.getValue())
                    {
                        // this entry must not have been fsynced.  probably the rest is bad too,
                        // but just in case there is no harm in trying them (since we still read on an entry boundary)
                        continue;
                    }

                    /* deserialize the commit log entry */
                    FastByteArrayInputStream bufIn = new FastByteArrayInputStream(bytes, 0, serializedSize);
                    RowMutation rm = null;
                    try
                    {
                        // assuming version here. We've gone to lengths to make sure what gets written to the CL is in
                        // the current version.  so do make sure the CL is drained prior to upgrading a node.
                        rm = RowMutation.serializer().deserialize(new DataInputStream(bufIn), MessagingService.version_, IColumnSerializer.Flag.LOCAL);
                    }
                    catch (UnserializableColumnFamilyException ex)
                    {
                        AtomicInteger i = invalidMutations.get(ex.cfId);
                        if (i == null)
                        {
                            i = new AtomicInteger(1);
                            invalidMutations.put(ex.cfId, i);
                        }
                        else
                            i.incrementAndGet();
                        continue;
                    }
                    
                    if (logger.isDebugEnabled())
                        logger.debug(String.format("replaying mutation for %s.%s: %s",
                                                    rm.getTable(),
                                                    ByteBufferUtil.bytesToHex(rm.key()),
                                                    "{" + StringUtils.join(rm.getColumnFamilies().iterator(), ", ") + "}"));

                    final long entryLocation = reader.getFilePointer();
                    final RowMutation frm = rm;
                    Runnable runnable = new WrappedRunnable()
                    {
                        public void runMayThrow() throws IOException
                        {
                            if (Schema.instance.getKSMetaData(frm.getTable()) == null)
                                return;
                            final Table table = Table.open(frm.getTable());
                            RowMutation newRm = new RowMutation(frm.getTable(), frm.key());

                            // Rebuild the row mutation, omitting column families that a) have already been flushed,
                            // b) are part of a cf that was dropped. Keep in mind that the cf.name() is suspect. do every
                            // thing based on the cfid instead.
                            for (ColumnFamily columnFamily : frm.getColumnFamilies())
                            {
                                if (Schema.instance.getCF(columnFamily.id()) == null)
                                    // null means the cf has been dropped
                                    continue;

                                ReplayPosition rp = cfPositions.get(columnFamily.id());

                                // replay if current segment is newer than last flushed one or, if it is the last known
                                // segment, if we are after the replay position
                                if (segment > rp.segment || (segment == rp.segment && entryLocation > rp.position))
                                {
                                    newRm.add(columnFamily);
                                    replayedCount.incrementAndGet();
                                }
                            }
                            if (!newRm.isEmpty())
                            {
                                Table.open(newRm.getTable()).apply(newRm, false);
                                tablesRecovered.add(table);
                            }
                        }
                    };
                    futures.add(StageManager.getStage(Stage.MUTATION).submit(runnable));
                    if (futures.size() > MAX_OUTSTANDING_REPLAY_COUNT)
                    {
                        FBUtilities.waitOnFutures(futures);
                        futures.clear();
                    }
                }
            }
            finally
            {
                FileUtils.closeQuietly(reader);
                logger.info("Finished reading " + file);
            }
        }
        
        for (Map.Entry<Integer, AtomicInteger> entry : invalidMutations.entrySet())
            logger.info(String.format("Skipped %d mutations from unknown (probably removed) CF with id %d", entry.getValue().intValue(), entry.getKey()));

        // wait for all the writes to finish on the mutation stage
        FBUtilities.waitOnFutures(futures);
        logger.debug("Finished waiting on mutations from recovery");

        // flush replayed tables
        futures.clear();
        for (Table table : tablesRecovered)
            futures.addAll(table.flush());
        FBUtilities.waitOnFutures(futures);

        return replayedCount.get();
    }

    /**
     * @return the current ReplayPosition of the current segment file
     */
    public ReplayPosition getContext()
    {
        Callable<ReplayPosition> task = new Callable<ReplayPosition>()
        {
            public ReplayPosition call() throws Exception
            {
                return activeSegment.getContext();
            }
        };
        try
        {
            return executor.submit(task).get();
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
     * Used by tests.
     *
     * @return the number of active segments (segments with unflushed data in them)
     */
    public int activeSegments()
    {
        return allocator.activeSegments.size();
    }

    /**
     * Add a RowMutation to the commit log.
     *
     * @param rm the RowMutation to add to the log
     */
    public void add(RowMutation rm) throws IOException
    {
        long totalSize = RowMutation.serializer().serializedSize(rm, MessagingService.version_) + CommitLogSegment.ENTRY_OVERHEAD_SIZE;
        if (totalSize > CommitLog.SEGMENT_SIZE)
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
    public void discardCompletedSegments(final Integer cfId, final ReplayPosition context) throws IOException
    {
        Callable task = new Callable()
        {
            public Object call() throws IOException
            {
                logger.debug("discard completed log segments for {}, column family {}", context, cfId);

                // Go thru the active segment files, which are ordered oldest to newest, marking the
                // flushed CF as clean, until we reach the segment file containing the ReplayPosition passed
                // in the arguments. Any segments that become unused after they are marked clean will be
                // recycled or discarded.
                for (Iterator<CommitLogSegment> iter = allocator.activeSegments.iterator(); iter.hasNext(); )
                {
                    CommitLogSegment segment = iter.next();
                    segment.markClean(cfId, context);

                    // If the segment is no longer needed, and we have another spare segment in the hopper
                    // (to keep the last segment from getting discarded), pursue either recycling or deleting
                    // this segment file.
                    if (segment.isUnused() && iter.hasNext())
                    {
                        logger.debug("Commit log segment {} is unused", segment);
                        iter.remove();
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
    public void sync() throws IOException
    {
        for (CommitLogSegment segment : allocator.activeSegments)
        {
            if (segment.needsSync())
            {
                segment.sync();
            }
        }
    }

    /**
     * @return the number of tasks completed by the commit log executor
     */
    public long getCompletedTasks()
    {
        return executor.getCompletedTasks();
    }

    /**
     * @return the depth of pending commit log executor queue
     */
    public long getPendingTasks()
    {
        return executor.getPendingTasks();
    }

    /**
     * @return the total size occupied by commitlo segments expressed in bytes. (used by MBean)
     */
    public long getTotalCommitlogSize()
    {
        return allocator.bytesUsed();
    }

    /**
     * Forces a new segment file to be allocated and activated. Used mainly by truncate. 
     */
    public void forceNewSegment() throws ExecutionException, InterruptedException
    {
        Callable<?> task = new Callable()
        {
            public Object call() throws IOException
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
    private void activateNextSegment() throws IOException
    {
        activeSegment = allocator.fetchSegment();
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
            try
            {
                if (!activeSegment.hasCapacityFor(rowMutation))
                    activateNextSegment();
                activeSegment.write(rowMutation);
            }
            catch (IOException e)
            {
                throw new IOError(e);
            }
        }

        public Object call() throws Exception
        {
            run();
            return null;
        }
    }
}
