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
import org.apache.cassandra.io.util.*;
import org.apache.cassandra.net.MessagingService;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.IColumnSerializer;
import org.apache.cassandra.io.util.FastByteArrayInputStream;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.WrappedRunnable;

import javax.management.MBeanServer;
import javax.management.ObjectName;

/*
 * Commit Log tracks every write operation into the system. The aim
 * of the commit log is to be able to successfully recover data that was
 * not stored to disk via the Memtable. Every Commit Log maintains a
 * header represented by the abstraction CommitLogHeader. The header
 * contains a bit array and an array of longs and both the arrays are
 * of size, #column families for the Table, the Commit Log represents.
 *
 * Whenever a ColumnFamily is written to, for the first time its bit flag
 * is set to one in the CommitLogHeader. When it is flushed to disk by the
 * Memtable its corresponding bit in the header is set to zero. This helps
 * track which CommitLogs can be thrown away as a result of Memtable flushes.
 * Additionally, when a ColumnFamily is flushed and written to disk, its
 * entry in the array of longs is updated with the offset in the Commit Log
 * file where it was written. This helps speed up recovery since we can seek
 * to these offsets and start processing the commit log.
 *
 * Every Commit Log is rolled over everytime it reaches its threshold in size;
 * the new log inherits the "dirty" bits from the old.
 *
 * Over time there could be a number of commit logs that would be generated.
 * To allow cleaning up non-active commit logs, whenever we flush a column family and update its bit flag in
 * the active CL, we take the dirty bit array and bitwise & it with the headers of the older logs.
 * If the result is 0, then it is safe to remove the older file.  (Since the new CL
 * inherited the old's dirty bitflags, getting a zero for any given bit in the anding
 * means that either the CF was clean in the old CL or it has been flushed since the
 * switch in the new.)
 */
public class CommitLog implements CommitLogMBean
{
    private static final int MAX_OUTSTANDING_REPLAY_COUNT = 1024;
    
    static final Logger logger = LoggerFactory.getLogger(CommitLog.class);

    public static final CommitLog instance = new CommitLog();

    private final Deque<CommitLogSegment> segments = new ArrayDeque<CommitLogSegment>();

    private final ICommitLogExecutorService executor;

    private static final int SEGMENT_SIZE = 128*1024*1024; // roll after log gets this big

    /**
     * param @ table - name of table for which we are maintaining
     *                 this commit log.
     * param @ recoverymode - is commit log being instantiated in
     *                        in recovery mode.
    */
    private CommitLog()
    {
        try
        {
            DatabaseDescriptor.createAllDirectories();
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }

        // all old segments are recovered and deleted before CommitLog is instantiated.
        // All we need to do is create a new one.
        segments.add(new CommitLogSegment());

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

    public void resetUnsafe()
    {
        for (CommitLogSegment segment : segments)
            segment.close();
        segments.clear();
        segments.add(new CommitLogSegment());
    }

    private boolean manages(String name)
    {
        for (CommitLogSegment segment : segments)
        {
            if (segment.getPath().endsWith(name))
                return true;
        }
        return false;
    }

    public static int recover() throws IOException
    {
        String directory = DatabaseDescriptor.getCommitLogLocation();
        File[] files = new File(directory).listFiles(new FilenameFilter()
        {
            public boolean accept(File dir, String name)
            {
                // we used to try to avoid instantiating commitlog (thus creating an empty segment ready for writes)
                // until after recover was finished.  this turns out to be fragile; it is less error-prone to go
                // ahead and allow writes before recover(), and just skip active segments when we do.
                return CommitLogSegment.possibleCommitLogFile(name) && !instance.manages(name);
            }
        });
        if (files.length == 0)
        {
            logger.info("No commitlog files found; skipping replay");
            return 0;
        }

        Arrays.sort(files, new FileUtils.FileComparator());
        logger.info("Replaying " + StringUtils.join(files, ", "));
        int replayed = recover(files);
        for (File f : files)
        {
            if (!f.delete())
                logger.error("Unable to remove " + f + "; you should remove it manually or next restart will replay it again (harmless, but time-consuming)");
        }
        logger.info("Log replay complete, " + replayed + " replayed mutations");
        return replayed;
    }

    // returns the number of replayed mutation (useful for tests in particular)
    public static int recover(File[] clogs) throws IOException
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
                                                    "{" + StringUtils.join(rm.getColumnFamilies(), ", ") + "}"));

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

    private CommitLogSegment currentSegment()
    {
        return segments.getLast();
    }
    
    public ReplayPosition getContext()
    {
        Callable<ReplayPosition> task = new Callable<ReplayPosition>()
        {
            public ReplayPosition call() throws Exception
            {
                return currentSegment().getContext();
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

    // for tests mainly
    public int segmentsCount()
    {
        return segments.size();
    }

    /*
     * Adds the specified row to the commit log. This method will reset the
     * file offset to what it is before the start of the operation in case
     * of any problems. This way we can assume that the subsequent commit log
     * entry will override the garbage left over by the previous write.
    */
    public void add(RowMutation rowMutation) throws IOException
    {
        executor.add(new LogRecordAdder(rowMutation));
    }

    /*
     * This is called on Memtable flush to add to the commit log
     * a token indicating that this column family has been flushed.
     * The bit flag associated with this column family is set in the
     * header and this is used to decide if the log file can be deleted.
    */
    public void discardCompletedSegments(final Integer cfId, final ReplayPosition context) throws IOException
    {
        Callable task = new Callable()
        {
            public Object call() throws IOException
            {
                discardCompletedSegmentsInternal(context, cfId);
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
     * Delete log segments whose contents have been turned into SSTables. NOT threadsafe.
     *
     * param @ context The commitLog context .
     * param @ id id of the columnFamily being flushed to disk.
     *
    */
    private void discardCompletedSegmentsInternal(ReplayPosition context, Integer id) throws IOException
    {
        if (logger.isDebugEnabled())
            logger.debug("discard completed log segments for " + context + ", column family " + id + ".");

        /*
         * Loop through all the commit log files in the history. Now process
         * all files that are older than the one in the context. For each of
         * these files the header needs to modified by resetting the dirty
         * bit corresponding to the flushed CF.
        */
        Iterator<CommitLogSegment> iter = segments.iterator();
        while (iter.hasNext())
        {
            CommitLogSegment segment = iter.next();
            if (segment.id == context.segment)
            {
                // Only unmark this segment if there were not write since the
                // ReplayPosition was grabbed.
                segment.turnOffIfNotWritten(id, context.position);
                maybeDiscardSegment(segment, iter);
                break;
            }

            segment.turnOff(id);
            maybeDiscardSegment(segment, iter);
        }
    }

    private void maybeDiscardSegment(CommitLogSegment segment, Iterator<CommitLogSegment> iter)
    {
        if (segment.isSafeToDelete() && iter.hasNext())
        {
            logger.info("Discarding obsolete commit log:" + segment);
            FileUtils.deleteAsync(segment.getPath());
            // usually this will be the first (remaining) segment, but not always, if segment A contains
            // writes to a CF that is unflushed but is followed by segment B whose CFs are all flushed.
            iter.remove();
        }
        else
        {
            if (logger.isDebugEnabled())
                logger.debug("Not safe to delete commit log " + segment + "; dirty is " + segment.dirtyString() + "; hasNext: " + iter.hasNext());
        }
    }

    void sync() throws IOException
    {
        currentSegment().sync();
    }

    /**
     * @return the total size occupied by the commitlog segments expressed in bytes.
     */
    public long getSize()
    {
        long commitlogTotalSize = 0;

        for (CommitLogSegment segment : segments)
        {
            commitlogTotalSize += segment.length();
        }

        return commitlogTotalSize;
    }

    public long getCompletedTasks()
    {
        return executor.getCompletedTasks();
    }

    public long getPendingTasks()
    {
        return executor.getPendingTasks();
    }

    public long getTotalCommitlogSize()
    {
        return getSize();
    }

    public void forceNewSegment() throws ExecutionException, InterruptedException
    {
        Callable<?> task = new Callable()
        {
            public Object call() throws IOException
            {
                if (currentSegment().length() > 0)
                    createNewSegment();
                return null;
            }
        };

        executor.submit(task).get();
    }

    private void createNewSegment() throws IOException
    {
        assert !segments.isEmpty();
        sync();
        segments.getLast().close();
        segments.add(new CommitLogSegment());

        // Maintain desired CL size cap
        if (getSize() >= DatabaseDescriptor.getTotalCommitlogSpaceInMB() * 1024 * 1024)
        {
            // Force a flush on all CFs keeping the oldest segment from being removed
            CommitLogSegment oldestSegment = segments.peek();
            assert oldestSegment != null; // has to be at least the one we just added
            for (Integer dirtyCFId : oldestSegment.cfLastWrite.keySet())
            {
                String keypace = Schema.instance.getCF(dirtyCFId).left;
                final ColumnFamilyStore cfs = Table.open(keypace).getColumnFamilyStore(dirtyCFId);
                // flush shouldn't run on the commitlog executor, since it acquires Table.switchLock,
                // which may already be held by a thread waiting for the CL executor (via getContext),
                // causing deadlock
                Runnable runnable = new Runnable()
                {
                    public void run()
                    {
                        cfs.forceFlush();
                    }
                };
                StorageService.optionalTasks.execute(runnable);
            }
        }
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
                currentSegment().write(rowMutation);
                // roll log if necessary
                if (currentSegment().length() >= SEGMENT_SIZE)
                    createNewSegment();
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

    public void shutdownBlocking() throws InterruptedException
    {
        executor.shutdown();
        executor.awaitTermination();
    }
}
