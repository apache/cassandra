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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.io.util.BufferedRandomAccessFile;
import org.apache.cassandra.io.DeletionService;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.WrappedRunnable;
import org.apache.cassandra.concurrent.StageManager;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.Checksum;
import java.util.zip.CRC32;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

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
public class CommitLog
{
    private static volatile int SEGMENT_SIZE = 128*1024*1024; // roll after log gets this big

    static final Logger logger = LoggerFactory.getLogger(CommitLog.class);

    public static CommitLog instance()
    {
        return CLHandle.instance;
    }

    private static class CLHandle
    {
        public static final CommitLog instance = new CommitLog();
    }

    private final Deque<CommitLogSegment> segments = new ArrayDeque<CommitLogSegment>();

    public static void setSegmentSize(int size)
    {
        SEGMENT_SIZE = size;
    }

    public int getSegmentCount()
    {
        return segments.size();
    }

    private final ICommitLogExecutorService executor;

    /**
     * param @ table - name of table for which we are maintaining
     *                 this commit log.
     * param @ recoverymode - is commit log being instantiated in
     *                        in recovery mode.
    */
    private CommitLog()
    {
        // all old segments are recovered and deleted before CommitLog is instantiated.
        // All we need to do is create a new one.
        segments.add(new CommitLogSegment());
        
        if (DatabaseDescriptor.getCommitLogSync() == Config.CommitLogSync.periodic)
        {
            executor = new PeriodicCommitLogExecutorService();
            final Callable syncer = new Callable()
            {
                public Object call() throws Exception
                {
                    sync();
                    return null;
                }
            };

            new Thread(new Runnable()
            {
                public void run()
                {
                    while (true)
                    {
                        try
                        {
                            executor.submit(syncer).get();
                            Thread.sleep(DatabaseDescriptor.getCommitLogSyncPeriod());
                        }
                        catch (InterruptedException e)
                        {
                            throw new AssertionError(e);
                        }
                        catch (ExecutionException e)
                        {
                            throw new RuntimeException(e);
                        }
                    }
                }
            }, "PERIODIC-COMMIT-LOG-SYNCER").start();
        }
        else
        {
            executor = new BatchCommitLogExecutorService();
        }
    }

    public static void recover() throws IOException
    {
        String directory = DatabaseDescriptor.getLogFileLocation();
        File file = new File(directory);
        File[] files = file.listFiles(new FilenameFilter()
        {
            public boolean accept(File dir, String name)
            {
                return CommitLogSegment.possibleCommitLogFile(name);
            }
        });
        if (files.length == 0)
            return;

        Arrays.sort(files, new FileUtils.FileComparator());
        logger.info("Replaying " + StringUtils.join(files, ", "));
        recover(files);
        FileUtils.delete(files);
        logger.info("Log replay complete");
    }

    public static void recover(File[] clogs) throws IOException
    {
        Set<Table> tablesRecovered = new HashSet<Table>();
        final AtomicInteger counter = new AtomicInteger(0);
        for (File file : clogs)
        {
            int bufferSize = (int)Math.min(file.length(), 32 * 1024 * 1024);
            BufferedRandomAccessFile reader = new BufferedRandomAccessFile(file.getAbsolutePath(), "r", bufferSize);
            final CommitLogHeader clHeader = CommitLogHeader.readCommitLogHeader(reader);
            /* seek to the lowest position where any CF has non-flushed data */
            int lowPos = CommitLogHeader.getLowestPosition(clHeader);
            if (lowPos == 0)
                break;

            reader.seek(lowPos);
            if (logger.isDebugEnabled())
                logger.debug("Replaying " + file + " starting at " + lowPos);

            /* read the logs populate RowMutation and apply */
            while (!reader.isEOF())
            {
                if (logger.isDebugEnabled())
                    logger.debug("Reading mutation at " + reader.getFilePointer());

                long claimedCRC32;
                byte[] bytes;
                try
                {
                    bytes = new byte[reader.readInt()]; // readInt can throw EOFException too
                    reader.readFully(bytes);
                    claimedCRC32 = reader.readLong();
                }
                catch (EOFException e)
                {
                    // last CL entry didn't get completely written.  that's ok.
                    break;
                }

                ByteArrayInputStream bufIn = new ByteArrayInputStream(bytes);
                Checksum checksum = new CRC32();
                checksum.update(bytes, 0, bytes.length);
                if (claimedCRC32 != checksum.getValue())
                {
                    // this part of the log must not have been fsynced.  probably the rest is bad too,
                    // but just in case there is no harm in trying them.
                    continue;
                }

                /* deserialize the commit log entry */
                final RowMutation rm = RowMutation.serializer().deserialize(new DataInputStream(bufIn));
                if (logger.isDebugEnabled())
                    logger.debug(String.format("replaying mutation for %s.%s: %s",
                                                rm.getTable(),
                                                rm.key(),
                                                "{" + StringUtils.join(rm.getColumnFamilies(), ", ") + "}"));
                final Table table = Table.open(rm.getTable());
                tablesRecovered.add(table);
                final Collection<ColumnFamily> columnFamilies = new ArrayList<ColumnFamily>(rm.getColumnFamilies());
                final long entryLocation = reader.getFilePointer();
                Runnable runnable = new WrappedRunnable()
                {
                    public void runMayThrow() throws IOException
                    {
                        RowMutation newRm = new RowMutation(rm.getTable(), rm.key());
                        
                        // Rebuild the row mutation, omitting column families that a) have already been flushed,
                        // b) are part of a cf that was dropped. Keep in mind that the cf.name() is suspect. do every
                        // thing based on the cfid instead.
                        for (ColumnFamily columnFamily : columnFamilies)
                        {
                            // make sure to use the current name of the cf (null means its been dropped).
                            String currentName = CFMetaData.getCurrentName(columnFamily.id());
                            if (currentName == null)
                                continue;
                            else if (!currentName.equals(columnFamily.name()))
                                columnFamily.rename(currentName);
                            
                            if (clHeader.isDirty(columnFamily.id()) && entryLocation >= clHeader.getPosition(columnFamily.id()))
                                newRm.add(columnFamily);
                        }
                        if (!newRm.isEmpty())
                        {
                            Table.open(newRm.getTable()).apply(newRm, null, false);
                        }
                        counter.decrementAndGet();
                    }
                };
                counter.incrementAndGet();
                StageManager.getStage(StageManager.MUTATION_STAGE).submit(runnable);
            }
            reader.close();
        }

        // wait for all the writes to finish on the mutation stage
        while (counter.get() > 0)
        {
            try
            {
                Thread.sleep(10);
            }
            catch (InterruptedException e)
            {
                throw new AssertionError(e);
            }
        }

        // flush replayed tables
        List<Future<?>> futures = new ArrayList<Future<?>>();
        for (Table table : tablesRecovered)
        {
            futures.addAll(table.flush());
        }
        // wait for flushes to finish
        for (Future<?> future : futures)
        {
            try
            {
                future.get();
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }
    }

    private CommitLogSegment currentSegment()
    {
        return segments.getLast();
    }
    
    public CommitLogSegment.CommitLogContext getContext()
    {
        Callable<CommitLogSegment.CommitLogContext> task = new Callable<CommitLogSegment.CommitLogContext>()
        {
            public CommitLogSegment.CommitLogContext call() throws Exception
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

    /*
     * Adds the specified row to the commit log. This method will reset the
     * file offset to what it is before the start of the operation in case
     * of any problems. This way we can assume that the subsequent commit log
     * entry will override the garbage left over by the previous write.
    */
    public void add(RowMutation rowMutation, Object serializedRow) throws IOException
    {
        executor.add(new LogRecordAdder(rowMutation, serializedRow));
    }

    /*
     * This is called on Memtable flush to add to the commit log
     * a token indicating that this column family has been flushed.
     * The bit flag associated with this column family is set in the
     * header and this is used to decide if the log file can be deleted.
    */
    public void discardCompletedSegments(final int cfId, final CommitLogSegment.CommitLogContext context) throws IOException
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
    private void discardCompletedSegmentsInternal(CommitLogSegment.CommitLogContext context, int id) throws IOException
    {
        if (logger.isDebugEnabled())
            logger.debug("discard completed log segments for " + context + ", column family " + id + ".");

        /*
         * log replay assumes that we only have to look at entries past the last
         * flush position, so verify that this flush happens after the last. See CASSANDRA-936
        */
        assert context.position >= context.getSegment().getHeader().getPosition(id)
               : "discard at " + context + " is not after last flush at " + context.getSegment().getHeader().getPosition(id);
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
            CommitLogHeader header = segment.getHeader();
            if (segment.equals(context.getSegment()))
            {
                // we can't just mark the segment where the flush happened clean,
                // since there may have been writes to it between when the flush
                // started and when it finished. so mark the flush position as
                // the replay point for this CF, instead.
                if (logger.isDebugEnabled())
                    logger.debug("Marking replay position " + context.position + " on commit log " + segment);
                header.turnOn(id, context.position);
                segment.writeHeader();
                break;
            }

            header.turnOff(id);
            if (header.isSafeToDelete())
            {
                logger.info("Discarding obsolete commit log:" + segment);
                segment.close();
                DeletionService.submitDelete(segment.getPath());
                // usually this will be the first (remaining) segment, but not always, if segment A contains
                // writes to a CF that is unflushed but is followed by segment B whose CFs are all flushed.
                iter.remove();
            }
            else
            {
                if (logger.isDebugEnabled())
                    logger.debug("Not safe to delete commit log " + segment + "; dirty is " + header.dirtyString());
                segment.writeHeader();
            }
        }
    }
    
    public void forceNewSegment()
    {
        Callable task = new Callable()
        {
            public Object call() throws Exception
            {
                sync();
                segments.add(new CommitLogSegment());
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

    void sync() throws IOException
    {
        currentSegment().sync();
    }

    // TODO this should be a Runnable since it doesn't actually return anything, but it's difficult to do that
    // without breaking the fragile CheaterFutureTask in BatchCLES.
    class LogRecordAdder implements Callable, Runnable
    {
        final RowMutation rowMutation;
        final Object serializedRow;

        LogRecordAdder(RowMutation rm, Object serializedRow)
        {
            this.rowMutation = rm;
            this.serializedRow = serializedRow;
        }

        public void run()
        {
            try
            {
                currentSegment().write(rowMutation, serializedRow);
                // roll log if necessary
                if (currentSegment().length() >= SEGMENT_SIZE)
                {
                    sync();
                    segments.add(new CommitLogSegment());
                }
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
