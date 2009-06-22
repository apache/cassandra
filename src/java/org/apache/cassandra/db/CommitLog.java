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

import java.io.*;
import java.util.*;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.DataInputBuffer;
import org.apache.cassandra.io.DataOutputBuffer;
import org.apache.cassandra.io.IFileReader;
import org.apache.cassandra.io.IFileWriter;
import org.apache.cassandra.io.SequenceFile;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.FileUtils;

import org.apache.log4j.Logger;
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
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */
public class CommitLog
{
    private static volatile int SEGMENT_SIZE = 128*1024*1024; // roll after log gets this big
    private static Map<String, CommitLog> instances_ = new HashMap<String, CommitLog>();
    private static Lock lock_ = new ReentrantLock();
    private static Logger logger_ = Logger.getLogger(CommitLog.class);
    private static Map<String, CommitLogHeader> clHeaders_ = new HashMap<String, CommitLogHeader>();

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
    }

    public static class CommitLogFileComparator implements Comparator<String>
    {
        public int compare(String f, String f2)
        {
            return (int)(getCreationTime(f) - getCreationTime(f2));
        }

        public boolean equals(Object o)
        {
            if ( !(o instanceof CommitLogFileComparator) )
                return false;
            return true;
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

    /*
     * Write the serialized commit log header into the specified commit log.
    */
    private static void writeCommitLogHeader(String commitLogFileName, byte[] bytes) throws IOException
    {     
        IFileWriter logWriter = CommitLog.createWriter(commitLogFileName);
        logWriter.seek(0L);
        /* write the commit log header */
        logWriter.writeDirect(bytes);
        logWriter.close();
    }

    private static IFileWriter createWriter(String file) throws IOException
    {        
        if ( DatabaseDescriptor.isFastSync() )
        {
            return SequenceFile.fastWriter(file, 4*1024*1024);
        }
        else
            return SequenceFile.writer(file);
    }

    static CommitLog open(String table) throws IOException
    {
        CommitLog commitLog = instances_.get(table);
        if ( commitLog == null )
        {
            CommitLog.lock_.lock();
            try
            {
                commitLog = instances_.get(table);
                if ( commitLog == null )
                {
                    commitLog = new CommitLog(table, false);
                    instances_.put(table, commitLog);
                }
            }
            finally
            {
                CommitLog.lock_.unlock();
            }
        }
        return commitLog;
    }

    static String getTableName(String file)
    {
        String[] values = file.split("-");
        return values[1];
    }

    private String table_;
    /* Current commit log file */
    private String logFile_;
    /* header for current commit log */
    private CommitLogHeader clHeader_;
    private IFileWriter logWriter_;

    /*
     * Generates a file name of the format CommitLog-<table>-<timestamp>.log in the
     * directory specified by the Database Descriptor.
    */
    private void setNextFileName()
    {
        logFile_ = DatabaseDescriptor.getLogFileLocation() + System.getProperty("file.separator") +
                   "CommitLog-" + table_ + "-" + System.currentTimeMillis() + ".log";
    }

    /*
     * param @ table - name of table for which we are maintaining
     *                 this commit log.
     * param @ recoverymode - is commit log being instantiated in
     *                        in recovery mode.
    */
    CommitLog(String table, boolean recoveryMode) throws IOException
    {
        table_ = table;
        if ( !recoveryMode )
        {
            setNextFileName();            
            logWriter_ = CommitLog.createWriter(logFile_);
            writeCommitLogHeader();
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
        table_ = CommitLog.getTableName(logFile.getName());
        logFile_ = logFile.getAbsolutePath();
        logWriter_ = CommitLog.createWriter(logFile_);
    }

    String getLogFile()
    {
        return logFile_;
    }
    
    private CommitLogHeader readCommitLogHeader(IFileReader logReader) throws IOException
    {
        int size = (int)logReader.readLong();
        byte[] bytes = new byte[size];
        logReader.readDirect(bytes);
        ByteArrayInputStream byteStream = new ByteArrayInputStream(bytes);
        return CommitLogHeader.serializer().deserialize(new DataInputStream(byteStream));
    }

    /*
     * This is invoked on startup via the ctor. It basically
     * writes a header with all bits set to zero.
    */
    private void writeCommitLogHeader() throws IOException
    {
        Table table = Table.open(table_);
        int cfSize = table.getNumberOfColumnFamilies();
        /* record the beginning of the commit header */
        /* write the commit log header */
        clHeader_ = new CommitLogHeader(cfSize);
        writeCommitLogHeader(clHeader_.toByteArray(), false);
    }

    private void writeCommitLogHeader(byte[] bytes, boolean reset) throws IOException
    {
        /* record the current position */
        long currentPos = logWriter_.getCurrentPosition();
        logWriter_.seek(0);
        /* write the commit log header */
        logWriter_.writeLong(bytes.length);
        logWriter_.writeDirect(bytes);
        if (reset)
        {
            /* seek back to the old position */
            logWriter_.seek(currentPos);
        }
    }

    void recover(File[] clogs) throws IOException
    {
        DataInputBuffer bufIn = new DataInputBuffer();

        for (File file : clogs)
        {
            // IFileReader reader = SequenceFile.bufferedReader(file.getAbsolutePath(), DatabaseDescriptor.getLogFileSizeThreshold());
            IFileReader reader = SequenceFile.reader(table_, file.getAbsolutePath());
            try
            {
                CommitLogHeader clHeader = readCommitLogHeader(reader);
                /* seek to the lowest position */
                int lowPos = CommitLogHeader.getLowestPosition(clHeader);
                /*
                 * If lowPos == 0 then we need to skip the processing of this
                 * file.
                */
                if (lowPos == 0)
                    break;
                else
                    reader.seek(lowPos);

                Set<Table> tablesRecovered = new HashSet<Table>();

                /* read the logs populate RowMutation and apply */
                while ( !reader.isEOF() )
                {
                    byte[] bytes = new byte[(int)reader.readLong()];
                    reader.readDirect(bytes);
                    bufIn.reset(bytes, bytes.length);

                    /* read the commit log entry */
                    try
                    {                        
                        Row row = Row.serializer(table_).deserialize(bufIn);
                        Table table = Table.open(table_);
                        tablesRecovered.add(table);
                        Collection<ColumnFamily> columnFamilies = new ArrayList<ColumnFamily>(row.getColumnFamilies());
                        /* remove column families that have already been flushed */
                        for (ColumnFamily columnFamily : columnFamilies)
                        {
                        	/* TODO: Remove this to not process Hints */
                        	if ( !DatabaseDescriptor.isApplicationColumnFamily(columnFamily.name()) )
                        	{
                        		row.removeColumnFamily(columnFamily);
                        		continue;
                        	}	
                            int id = table.getColumnFamilyId(columnFamily.name());
                            if ( !clHeader.isDirty(id) || reader.getCurrentPosition() < clHeader.getPosition(id) )
                                row.removeColumnFamily(columnFamily);
                        }
                        if ( !row.isEmpty() )
                        {                            
                        	table.applyNow(row);
                        }
                    }
                    catch ( IOException e )
                    {
                        logger_.error("Unexpected error reading " + file.getName() + "; attempting to continue with the next entry", e);
                    }
                }
                reader.close();
                /* apply the rows read -- success will result in the CL file being discarded */
                for (Table table : tablesRecovered)
                {
                    table.flush(true);
                }
            }
            catch (Throwable th)
            {
                logger_.error("Fatal error reading " + file.getName(), th);
                /* close the reader and delete this commit log. */
                reader.close();
                FileUtils.delete(new File[]{ file });
            }
        }
    }

    /*
     * Update the header of the commit log if a new column family
     * is encountered for the first time.
    */
    private void maybeUpdateHeader(Row row) throws IOException
    {
        Table table = Table.open(table_);
        for (ColumnFamily columnFamily : row.getColumnFamilies())
        {
            int id = table.getColumnFamilyId(columnFamily.name());
            if (!clHeader_.isDirty(id) || (clHeader_.isDirty(id) && clHeader_.getPosition(id) == 0))
            {
                clHeader_.turnOn(id, logWriter_.getCurrentPosition());
                writeCommitLogHeader(clHeader_.toByteArray(), true);
            }
        }
    }
    
    CommitLogContext getContext() throws IOException
    {
        return new CommitLogContext(logFile_, logWriter_.getCurrentPosition());
    }

    /*
     * Adds the specified row to the commit log. This method will reset the
     * file offset to what it is before the start of the operation in case
     * of any problems. This way we can assume that the subsequent commit log
     * entry will override the garbage left over by the previous write.
    */
    synchronized CommitLogContext add(Row row) throws IOException
    {
        long currentPosition = -1L;
        CommitLogContext cLogCtx = null;
        DataOutputBuffer cfBuffer = new DataOutputBuffer();

        try
        {
            /* serialize the row */
            cfBuffer.reset();
            Row.serializer(table_).serialize(row, cfBuffer);
            currentPosition = logWriter_.getCurrentPosition();
            cLogCtx = new CommitLogContext(logFile_, currentPosition);
            /* Update the header */
            maybeUpdateHeader(row);
            logWriter_.writeLong(cfBuffer.getLength());
            logWriter_.append(cfBuffer);
            checkThresholdAndRollLog();
        }
        catch (IOException e)
        {
            if ( currentPosition != -1 )
                logWriter_.seek(currentPosition);
            throw e;
        }
        finally
        {                  	
            cfBuffer.close();            
        }
        return cLogCtx;
    }

    /*
     * This is called on Memtable flush to add to the commit log
     * a token indicating that this column family has been flushed.
     * The bit flag associated with this column family is set in the
     * header and this is used to decide if the log file can be deleted.
    */
    synchronized void onMemtableFlush(String cf, CommitLog.CommitLogContext cLogCtx) throws IOException
    {
        Table table = Table.open(table_);
        int id = table.getColumnFamilyId(cf);
        /* trying discarding old commit log files */
        discard(cLogCtx, id);
    }


    /*
     * Check if old commit logs can be deleted. However we cannot
     * do this anymore in the Fast Sync mode and hence I think we
     * should get rid of Fast Sync mode altogether.
     *
     * param @ cLogCtx The commitLog context .
     * param @ id id of the columnFamily being flushed to disk.
     *
    */
    private void discard(CommitLog.CommitLogContext cLogCtx, int id) throws IOException
    {
        /* retrieve the commit log header associated with the file in the context */
        CommitLogHeader commitLogHeader = clHeaders_.get(cLogCtx.file);
        if(commitLogHeader == null )
        {
            if( logFile_.equals(cLogCtx.file) )
            {
                /* this means we are dealing with the current commit log. */
                commitLogHeader = clHeader_;
                clHeaders_.put(cLogCtx.file, clHeader_);
            }
            else
                return;
        }

        /*
         * log replay assumes that we only have to look at entries past the last
         * flush position, so verify that this flush happens after the last.
         * (Currently Memtables are flushed on a single thread so this should be fine.)
        */
        assert cLogCtx.position >= commitLogHeader.getPosition(id);

        commitLogHeader.turnOff(id);
        /* Sort the commit logs based on creation time */
        List<String> oldFiles = new ArrayList<String>(clHeaders_.keySet());
        Collections.sort(oldFiles, new CommitLogFileComparator());
        List<String> listOfDeletedFiles = new ArrayList<String>();
        /*
         * Loop through all the commit log files in the history. Now process
         * all files that are older than the one in the context. For each of
         * these files the header needs to modified by performing a bitwise &
         * of the header with the header of the file in the context. If we
         * encounter the file in the context in our list of old commit log files
         * then we update the header and write it back to the commit log.
        */
        for(String oldFile : oldFiles)
        {
            if(oldFile.equals(cLogCtx.file))
            {
                /*
                 * We need to turn on again. This is because we always keep
                 * the bit turned on and the position indicates from where the
                 * commit log needs to be read. When a flush occurs we turn off
                 * perform & operation and then turn on with the new position.
                */
                commitLogHeader.turnOn(id, cLogCtx.position);
                writeCommitLogHeader(cLogCtx.file, commitLogHeader.toByteArray());
                break;
            }
            else
            {
                CommitLogHeader oldCommitLogHeader = clHeaders_.get(oldFile);
                oldCommitLogHeader.and(commitLogHeader);
                if(oldCommitLogHeader.isSafeToDelete())
                {
                	logger_.debug("Deleting commit log:"+ oldFile);
                    FileUtils.deleteAsync(oldFile);
                    listOfDeletedFiles.add(oldFile);
                }
                else
                {
                    writeCommitLogHeader(oldFile, oldCommitLogHeader.toByteArray());
                }
            }
        }

        for ( String deletedFile : listOfDeletedFiles)
        {
            clHeaders_.remove(deletedFile);
        }
    }

    private void checkThresholdAndRollLog() throws IOException
    {
        if (logWriter_.getFileSize() >= SEGMENT_SIZE)
        {
            /* Rolls the current log file over to a new one. */
            setNextFileName();
            String oldLogFile = logWriter_.getFileName();
            logWriter_.close();

            /* point reader/writer to a new commit log file. */
            logWriter_ = CommitLog.createWriter(logFile_);
            /* squirrel away the old commit log header */
            clHeaders_.put(oldLogFile, new CommitLogHeader(clHeader_));
            // we leave the old 'dirty' bits alone, so we can test for
            // whether it's safe to remove a given log segment by and-ing its dirty
            // with the current one.
            clHeader_.zeroPositions();
            writeCommitLogHeader(clHeader_.toByteArray(), false);
        }
    }
}