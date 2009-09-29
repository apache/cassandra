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
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
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
    private static Lock lock_ = new ReentrantLock();
    private static Logger logger_ = Logger.getLogger(CommitLog.class);
    private static Map<String, CommitLogHeader> clHeaders_ = new HashMap<String, CommitLogHeader>();

    private ExecutorService executor;


    public static final class CommitLogContext
    {
        static CommitLogContext NULL = new CommitLogContext(null, -1L);
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
                    instance_ = new CommitLog(false);
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
    CommitLog(boolean recoveryMode) throws IOException
    {
        if (!recoveryMode)
        {
            executor = new CommitLogExecutorService();
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

    private static void writeCommitLogHeader(RandomAccessFile logWriter, byte[] bytes) throws IOException
    {
        logWriter.writeLong(bytes.length);
        logWriter.write(bytes);
    }

    void recover(File[] clogs) throws IOException
    {
        DataInputBuffer bufIn = new DataInputBuffer();

        for (File file : clogs)
        {
            int bufferSize = (int)Math.min(file.length(), 32 * 1024 * 1024);
            BufferedRandomAccessFile reader = new BufferedRandomAccessFile(file.getAbsolutePath(), "r", bufferSize);
            CommitLogHeader clHeader = readCommitLogHeader(reader);
            /* seek to the lowest position where any CF has non-flushed data */
            int lowPos = CommitLogHeader.getLowestPosition(clHeader);
            if (lowPos == 0)
                break;

            reader.seek(lowPos);
            if (logger_.isDebugEnabled())
                logger_.debug("Replaying " + file + " starting at " + lowPos);

            Set<Table> tablesRecovered = new HashSet<Table>();

            /* read the logs populate RowMutation and apply */
            while (!reader.isEOF())
            {
                if (logger_.isDebugEnabled())
                    logger_.debug("Reading mutation at " + reader.getFilePointer());

                byte[] bytes;
                try
                {
                    bytes = new byte[(int) reader.readLong()]; // readlong can throw EOFException too
                    if (reader.read(bytes) < bytes.length)
                    {
                        throw new EOFException();
                    }
                }
                catch (EOFException e)
                {
                    // last CL entry didn't get completely written.  that's ok.
                    break;
                }
                bufIn.reset(bytes, bytes.length);

                /* read the commit log entry */
                Row row = Row.serializer().deserialize(bufIn);
                if (logger_.isDebugEnabled())
                    logger_.debug(String.format("replaying mutation for %s.%s: %s",
                                                row.getTable(),
                                                row.key(),
                                                "{" + StringUtils.join(row.getColumnFamilies(), ", ") + "}"));
                Table table = Table.open(row.getTable());
                tablesRecovered.add(table);
                Collection<ColumnFamily> columnFamilies = new ArrayList<ColumnFamily>(row.getColumnFamilies());
                /* remove column families that have already been flushed */
                for (ColumnFamily columnFamily : columnFamilies)
                {
                    int id = table.getColumnFamilyId(columnFamily.name());
                    if (!clHeader.isDirty(id) || reader.getFilePointer() < clHeader.getPosition(id))
                    {
                        row.removeColumnFamily(columnFamily);
                    }
                }
                if (!row.isEmpty())
                {
                    table.applyNow(row);
                }
            }
            reader.close();
            /* apply the rows read -- success will result in the CL file being discarded */
            for (Table table : tablesRecovered)
            {
                table.flush(true);
            }
        }
    }

    /*
     * Update the header of the commit log if a new column family
     * is encountered for the first time.
    */
    private void maybeUpdateHeader(Row row) throws IOException
    {
        Table table = Table.open(row.getTable());
        for (ColumnFamily columnFamily : row.getColumnFamilies())
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
    CommitLogContext add(final Row row) throws IOException
    {
        Callable<CommitLogContext> task = new LogRecordAdder(row);

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
        CommitLogHeader commitLogHeader = clHeaders_.get(cLogCtx.file);
        if (commitLogHeader == null)
        {
            if (logFile_.equals(cLogCtx.file))
            {
                /* this means we are dealing with the current commit log. */
                commitLogHeader = clHeader_;
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
         * (Currently Memtables are flushed on a single thread so this should be fine.)
        */
        assert cLogCtx.position >= commitLogHeader.getPosition(id);

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
            if (oldFile.equals(cLogCtx.file))
            {
                // we can't just mark the segment where the flush happened clean,
                // since there may have been writes to it between when the flush
                // started and when it finished. so mark the flush position as
                // the replay point for this CF, instead.
                if (logger_.isDebugEnabled())
                    logger_.debug("Marking replay position on current commit log " + oldFile);
                commitLogHeader.turnOn(id, cLogCtx.position);
                seekAndWriteCommitLogHeader(commitLogHeader.toByteArray());
                break;
            }

	    CommitLogHeader oldCommitLogHeader = clHeaders_.get(oldFile);
	    oldCommitLogHeader.turnOff(id);
	    if (oldCommitLogHeader.isSafeToDelete())
	    {
		if (logger_.isDebugEnabled())
		  logger_.debug("Deleting commit log:" + oldFile);
		FileUtils.deleteAsync(oldFile);
		clHeaders_.remove(oldFile);
	    }
	    else
	    {
		if (logger_.isDebugEnabled())
		    logger_.debug("Not safe to delete commit log " + oldFile + "; dirty is " + oldCommitLogHeader.dirtyString());
		RandomAccessFile logWriter = CommitLog.createWriter(oldFile);
		writeCommitLogHeader(logWriter, oldCommitLogHeader.toByteArray());
		logWriter.close();
	    }
        }
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
        Row row;

        LogRecordAdder(Row row)
        {
            this.row = row;
        }

        public CommitLog.CommitLogContext call() throws Exception
        {
            long currentPosition = -1L;
            DataOutputBuffer cfBuffer = new DataOutputBuffer();
            try
            {
                /* serialize the row */
                Row.serializer().serialize(row, cfBuffer);
                currentPosition = logWriter_.getFilePointer();
                CommitLogContext cLogCtx = new CommitLogContext(logFile_, currentPosition);
                /* Update the header */
                maybeUpdateHeader(row);
                logWriter_.writeLong(cfBuffer.getLength());
                logWriter_.write(cfBuffer.getData(), 0, cfBuffer.getLength());
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
