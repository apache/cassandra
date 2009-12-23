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

package org.apache.cassandra.db;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.BufferedRandomAccessFile;
import org.apache.cassandra.io.DataInputBuffer;
import org.apache.cassandra.io.DataOutputBuffer;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.FileUtils;
import org.apache.cassandra.concurrent.StageManager;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.*;
import java.util.zip.Checksum;
import java.util.zip.CRC32;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

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
 *
 * The CommitLog class itself is "mostly a singleton."  open() always returns one
 * instance, but log replay will bypass that.
 */
public class CommitLog
{
    private static volatile int SEGMENT_SIZE = 128*1024*1024; // roll after log gets this big
    private static volatile CommitLog instance_;
    private static final Lock lock_ = new ReentrantLock();
    private static final Logger logger_ = Logger.getLogger(CommitLog.class);
    private static final Map<String, CommitLogHeader> clHeaders_ = new HashMap<String, CommitLogHeader>();

    public static final class CommitLogContext
    {
        /* Commit Log associated with this operation */
        public final String file;
        /* Offset within the Commit Log where this row as added */
        public final long position;

        public CommitLogContext(String file, long position)
        {
            this.file = file;
            this.position = position;
        }

        boolean isValidContext()
        {
            return (position != -1L);
        }

        @Override
        public String toString()
        {
            return "CommitLogContext(" +
                   "file='" + file + '\'' +
                   ", position=" + position +
                   ')';
        }
    }

    public static class CommitLogFileComparator implements Comparator<String>
    {
        public int compare(String f, String f2)
        {
            return (int)(getCreationTime(f) - getCreationTime(f2));
        }
    }

    public static void setSegmentSize(int size)
    {
        SEGMENT_SIZE = size;
    }

    static int getSegmentCount()
    {
        return clHeaders_.size();
    }

    static long getCreationTime(String file)
    {
        String[] entries = FBUtilities.strip(file, "-.");
        return Long.parseLong(entries[entries.length - 2]);
    }

    private static BufferedRandomAccessFile createWriter(String file) throws IOException
    {        
        return new BufferedRandomAccessFile(file, "rw");
    }

    static CommitLog open() throws IOException
    {
        if ( instance_ == null )
        {
            CommitLog.lock_.lock();
            try
            {

                if ( instance_ == null )
                {
                    instance_ = new CommitLog();
                }
            }
            finally
            {
                CommitLog.lock_.unlock();
            }
        }
        return instance_;
    }

    /* Current commit log file */
    private String logFile_;
    /* header for current commit log */
    private CommitLogHeader clHeader_;
    private BufferedRandomAccessFile logWriter_;
    private final ExecutorService executor = new CommitLogExecutorService();

    /*
     * Generates a file name of the format CommitLog-<table>-<timestamp>.log in the
     * directory specified by the Database Descriptor.
    */
    private void setNextFileName()
    {
        logFile_ = DatabaseDescriptor.getLogFileLocation() + File.separator +
                   "CommitLog-" + System.currentTimeMillis() + ".log";
    }

    /*
     * param @ table - name of table for which we are maintaining
     *                 this commit log.
     * param @ recoverymode - is commit log being instantiated in
     *                        in recovery mode.
    */
    private CommitLog() throws IOException
    {
        setNextFileName();
        logWriter_ = CommitLog.createWriter(logFile_);
        writeCommitLogHeader();

        if (DatabaseDescriptor.getCommitLogSync() == DatabaseDescriptor.CommitLogSync.periodic)
        {
            final Runnable syncer = new Runnable()
            {
                public void run()
                {
                    try
                    {
                        sync();
                    }
                    catch (IOException e)
                    {
                        throw new RuntimeException(e);
                    }
                }
            };

            new Thread(new Runnable()
            {
                public void run()
                {
                    while (true)
                    {
                        executor.submit(syncer);
                        try
                        {
                            Thread.sleep(DatabaseDescriptor.getCommitLogSyncPeriod());
                        }
                        catch (InterruptedException e)
                        {
                            throw new RuntimeException(e);
                        }
                    }
                }
            }, "PERIODIC-COMMIT-LOG-SYNCER").start();
        }
    }

    /*
     * This ctor is currently used only for debugging. We
     * are now using it to modify the header so that recovery
     * can be tested in as many scenarios as we could imagine.
     *
     * param @ logFile - logfile which we wish to modify.
    */
    CommitLog(File logFile) throws IOException
    {
        logFile_ = logFile.getAbsolutePath();
        logWriter_ = CommitLog.createWriter(logFile_);
    }

    String getLogFile()
    {
        return logFile_;
    }
    
    private CommitLogHeader readCommitLogHeader(BufferedRandomAccessFile logReader) throws IOException
    {
        int size = (int)logReader.readLong();
        byte[] bytes = new byte[size];
        logReader.read(bytes);
        ByteArrayInputStream byteStream = new ByteArrayInputStream(bytes);
        return CommitLogHeader.serializer().deserialize(new DataInputStream(byteStream));
    }

    /*
     * This is invoked on startup via the ctor. It basically
     * writes a header with all bits set to zero.
    */
    private void writeCommitLogHeader() throws IOException
    {
        int cfSize = Table.TableMetadata.getColumnFamilyCount();
        clHeader_ = new CommitLogHeader(cfSize);
        writeCommitLogHeader(logWriter_, clHeader_.toByteArray());
    }

    /** writes header at the beginning of the file, then seeks back to current position */
    private void seekAndWriteCommitLogHeader(byte[] bytes) throws IOException
    {
        long currentPos = logWriter_.getFilePointer();
        logWriter_.seek(0);

        writeCommitLogHeader(logWriter_, bytes);

        logWriter_.seek(currentPos);
    }

    private static void writeCommitLogHeader(BufferedRandomAccessFile logWriter, byte[] bytes) throws IOException
    {
        logWriter.writeLong(bytes.length);
        logWriter.write(bytes);
        logWriter.sync();
    }

    void recover(File[] clogs) throws IOException
    {
        Set<Table> tablesRecovered = new HashSet<Table>();
        assert StageManager.getStage(StageManager.mutationStage_).getCompletedTasks() == 0;
        int rows = 0;

        DataInputBuffer bufIn = new DataInputBuffer();
        for (File file : clogs)
        {
            int bufferSize = (int)Math.min(file.length(), 32 * 1024 * 1024);
            BufferedRandomAccessFile reader = new BufferedRandomAccessFile(file.getAbsolutePath(), "r", bufferSize);
            final CommitLogHeader clHeader = readCommitLogHeader(reader);
            /* seek to the lowest position where any CF has non-flushed data */
            int lowPos = CommitLogHeader.getLowestPosition(clHeader);
            if (lowPos == 0)
                break;

            reader.seek(lowPos);
            if (logger_.isDebugEnabled())
                logger_.debug("Replaying " + file + " starting at " + lowPos);

            /* read the logs populate RowMutation and apply */
            while (!reader.isEOF())
            {
                if (logger_.isDebugEnabled())
                    logger_.debug("Reading mutation at " + reader.getFilePointer());

                long claimedCRC32;
                byte[] bytes;
                try
                {
                    bytes = new byte[(int) reader.readLong()]; // readlong can throw EOFException too
                    reader.readFully(bytes);
                    claimedCRC32 = reader.readLong();
                }
                catch (EOFException e)
                {
                    // last CL entry didn't get completely written.  that's ok.
                    break;
                }
                bufIn.reset(bytes, bytes.length);
                Checksum checksum = new CRC32();
                checksum.update(bytes, 0, bytes.length);
                if (claimedCRC32 != checksum.getValue())
                {
                    // this part of the log must not have been fsynced.  probably the rest is bad too,
                    // but just in case there is no harm in trying them.
                    continue;
                }

                /* deserialize the commit log entry */
                final RowMutation rm = RowMutation.serializer().deserialize(bufIn);
                if (logger_.isDebugEnabled())
                    logger_.debug(String.format("replaying mutation for %s.%s: %s",
                                                rm.getTable(),
                                                rm.key(),
                                                "{" + StringUtils.join(rm.getColumnFamilies(), ", ") + "}"));
                final Table table = Table.open(rm.getTable());
                tablesRecovered.add(table);
                final Collection<ColumnFamily> columnFamilies = new ArrayList<ColumnFamily>(rm.getColumnFamilies());
                final long entryLocation = reader.getFilePointer();
                Runnable runnable = new Runnable()
                {
                    public void run()
                    {
                        /* remove column families that have already been flushed before applying the rest */
                        for (ColumnFamily columnFamily : columnFamilies)
                        {
                            int id = table.getColumnFamilyId(columnFamily.name());
                            if (!clHeader.isDirty(id) || entryLocation < clHeader.getPosition(id))
                            {
                                rm.removeColumnFamily(columnFamily);
                            }
                        }
                        if (!rm.isEmpty())
                        {
                            try
                            {
                                Table.open(rm.getTable()).apply(rm, null, false);
                            }
                            catch (IOException e)
                            {
                                throw new IOError(e);
                            }
                        }
                    }
                };
                StageManager.getStage(StageManager.mutationStage_).execute(runnable);
                rows++;
            }
            reader.close();
        }

        // wait for all the writes to finish on the mutation stage
        while (StageManager.getStage(StageManager.mutationStage_).getCompletedTasks() < rows)
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

        // flush replayed tables, allowing commitlog segments to be removed
        List<Future<?>> futures = new ArrayList<Future<?>>();
        for (Table table : tablesRecovered)
        {
            futures.addAll(table.flush());
        }
        // wait for flushes to finish before continuing with startup
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

    /*
     * Update the header of the commit log if a new column family
     * is encountered for the first time.
    */
    private void maybeUpdateHeader(RowMutation rm) throws IOException
    {
        Table table = Table.open(rm.getTable());
        for (ColumnFamily columnFamily : rm.getColumnFamilies())
        {
            int id = table.getColumnFamilyId(columnFamily.name());
            if (!clHeader_.isDirty(id))
            {
                clHeader_.turnOn(id, logWriter_.getFilePointer());
                seekAndWriteCommitLogHeader(clHeader_.toByteArray());
            }
        }
    }
    
    CommitLogContext getContext() throws IOException
    {
        Callable<CommitLogContext> task = new Callable<CommitLogContext>()
        {
            public CommitLogContext call() throws Exception
            {
                return new CommitLogContext(logFile_, logWriter_.getFilePointer());
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
    void add(RowMutation rowMutation, Object serializedRow) throws IOException
    {
        Callable<CommitLogContext> task = new LogRecordAdder(rowMutation, serializedRow);

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

    /*
     * This is called on Memtable flush to add to the commit log
     * a token indicating that this column family has been flushed.
     * The bit flag associated with this column family is set in the
     * header and this is used to decide if the log file can be deleted.
    */
    void onMemtableFlush(final String tableName, final String cf, final CommitLog.CommitLogContext cLogCtx) throws IOException
    {
        Callable task = new Callable()
        {
            public Object call() throws IOException
            {
                Table table = Table.open(tableName);
                int id = table.getColumnFamilyId(cf);
                discardCompletedSegments(cLogCtx, id);
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

    /*
     * Delete log segments whose contents have been turned into SSTables.
     *
     * param @ cLogCtx The commitLog context .
     * param @ id id of the columnFamily being flushed to disk.
     *
    */
    private void discardCompletedSegments(CommitLog.CommitLogContext cLogCtx, int id) throws IOException
    {
        if (logger_.isDebugEnabled())
            logger_.debug("discard completed log segments for " + cLogCtx + ", column family " + id + ". CFIDs are " + Table.TableMetadata.getColumnFamilyIDString());
        /* retrieve the commit log header associated with the file in the context */
        if (clHeaders_.get(cLogCtx.file) == null)
        {
            if (logFile_.equals(cLogCtx.file))
            {
                /* this means we are dealing with the current commit log. */
                clHeaders_.put(cLogCtx.file, clHeader_);
            }
            else
            {
                logger_.error("Unknown commitlog file " + cLogCtx.file);
                return;
            }
        }

        /*
         * log replay assumes that we only have to look at entries past the last
         * flush position, so verify that this flush happens after the last.
        */
        assert cLogCtx.position >= clHeaders_.get(cLogCtx.file).getPosition(id);

        /* Sort the commit logs based on creation time */
        List<String> oldFiles = new ArrayList<String>(clHeaders_.keySet());
        Collections.sort(oldFiles, new CommitLogFileComparator());

        /*
         * Loop through all the commit log files in the history. Now process
         * all files that are older than the one in the context. For each of
         * these files the header needs to modified by resetting the dirty
         * bit corresponding to the flushed CF.
        */
        for (String oldFile : oldFiles)
        {
            CommitLogHeader header = clHeaders_.get(oldFile);
            if (oldFile.equals(cLogCtx.file))
            {
                // we can't just mark the segment where the flush happened clean,
                // since there may have been writes to it between when the flush
                // started and when it finished. so mark the flush position as
                // the replay point for this CF, instead.
                if (logger_.isDebugEnabled())
                    logger_.debug("Marking replay position " + cLogCtx.position + " on commit log " + oldFile);
                header.turnOn(id, cLogCtx.position);
                if (oldFile.equals(logFile_))
                {
                    seekAndWriteCommitLogHeader(header.toByteArray());
                }
                else
                {
                    writeOldCommitLogHeader(oldFile, header);
                }
                break;
            }

            header.turnOff(id);
            if (header.isSafeToDelete())
            {
                logger_.info("Deleting obsolete commit log:" + oldFile);
                FileUtils.deleteAsync(oldFile);
                clHeaders_.remove(oldFile);
            }
            else
            {
                if (logger_.isDebugEnabled())
                    logger_.debug("Not safe to delete commit log " + oldFile + "; dirty is " + header.dirtyString());
                writeOldCommitLogHeader(oldFile, header);
            }
        }
    }

    private void writeOldCommitLogHeader(String oldFile, CommitLogHeader header) throws IOException
    {
        BufferedRandomAccessFile logWriter = CommitLog.createWriter(oldFile);
        writeCommitLogHeader(logWriter, header.toByteArray());
        logWriter.close();
    }

    private boolean maybeRollLog() throws IOException
    {
        if (logWriter_.length() >= SEGMENT_SIZE)
        {
            /* Rolls the current log file over to a new one. */
            setNextFileName();
            String oldLogFile = logWriter_.getPath();
            logWriter_.close();

            /* point reader/writer to a new commit log file. */
            logWriter_ = CommitLog.createWriter(logFile_);
            /* squirrel away the old commit log header */
            clHeaders_.put(oldLogFile, new CommitLogHeader(clHeader_));
            clHeader_.clear();
            writeCommitLogHeader(logWriter_, clHeader_.toByteArray());
            return true;
        }
        return false;
    }

    void sync() throws IOException
    {
        logWriter_.sync();
    }

    class LogRecordAdder implements Callable<CommitLog.CommitLogContext>
    {
        final RowMutation rowMutation;
        final Object serializedRow;

        LogRecordAdder(RowMutation rm, Object serializedRow)
        {
            this.rowMutation = rm;
            this.serializedRow = serializedRow;
        }

        public CommitLog.CommitLogContext call() throws Exception
        {
            long currentPosition = -1L;
            try
            {
                currentPosition = logWriter_.getFilePointer();
                CommitLogContext cLogCtx = new CommitLogContext(logFile_, currentPosition);
                maybeUpdateHeader(rowMutation);
                Checksum checkum = new CRC32();
                if (serializedRow instanceof DataOutputBuffer)
                {
                    DataOutputBuffer buffer = (DataOutputBuffer) serializedRow;
                    logWriter_.writeLong(buffer.getLength());
                    logWriter_.write(buffer.getData(), 0, buffer.getLength());
                    checkum.update(buffer.getData(), 0, buffer.getLength());
                }
                else
                {
                    assert serializedRow instanceof byte[];
                    byte[] bytes = (byte[]) serializedRow;
                    logWriter_.writeLong(bytes.length);
                    logWriter_.write(bytes);
                    checkum.update(bytes, 0, bytes.length);
                }
                logWriter_.writeLong(checkum.getValue());
                maybeRollLog();
                return cLogCtx;
            }
            catch (IOException e)
            {
                if ( currentPosition != -1 )
                    logWriter_.seek(currentPosition);
                throw e;
            }
        }
    }
}
