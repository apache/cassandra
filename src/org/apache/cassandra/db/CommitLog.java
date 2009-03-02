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
import java.nio.file.Path;
import java.util.*;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.DataInputBuffer;
import org.apache.cassandra.io.DataOutputBuffer;
import org.apache.cassandra.io.IFileReader;
import org.apache.cassandra.io.IFileWriter;
import org.apache.cassandra.io.SequenceFile;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.FileUtils;
import org.apache.cassandra.utils.LogUtil;
import org.apache.log4j.Logger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.cassandra.io.*;
import org.apache.cassandra.utils.*;

/*
 * Commit Log tracks every write operation into the system. The aim
 * of the commit log is to be able to successfully recover data that was
 * not stored to disk via the Memtable. Every Commit Log maintains a
 * header represented by the abstraction CommitLogHeader. The header
 * contains a bit array and an array of longs and both the arrays are
 * of size, #column families for the Table, the Commit Log represents.
 * Whenever a ColumnFamily is written to, for the first time its bit flag
 * is set to one in the CommitLogHeader. When it is flushed to disk by the
 * Memtable its corresponding bit in the header is set to zero. This helps
 * track which CommitLogs can be thrown away as a result of Memtable flushes.
 * However if a ColumnFamily is flushed and again written to disk then its
 * entry in the array of longs is updated with the offset in the Commit Log
 * file where it was written. This helps speed up recovery since we can seek
 * to these offsets and start processing the commit log.
 * Every Commit Log is rolled over everytime it reaches its threshold in size.
 * Over time there could be a number of commit logs that would be generated.
 * Hovever whenever we flush a column family disk and update its bit flag we
 * take this bit array and bitwise & it with the headers of the other commit
 * logs that are older.
 *
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */
class CommitLog
{
    private static final int bufSize_ = 128*1024*1024;
    private static Map<String, CommitLog> instances_ = new HashMap<String, CommitLog>();
    private static Lock lock_ = new ReentrantLock();
    private static Logger logger_ = Logger.getLogger(CommitLog.class);
    private static Map<String, CommitLogHeader> clHeaders_ = new HashMap<String, CommitLogHeader>();
    
    public static final class CommitLogContext
    {
        static CommitLogContext NULL = new CommitLogContext(null, -1L);
        /* Commit Log associated with this operation */
        private String file_;
        /* Offset within the Commit Log where this row as added */
        private long position_;

        public CommitLogContext(String file, long position)
        {
            file_ = file;
            position_ = position;
        }

        boolean isValidContext()
        {
            return (position_ != -1L);
        }

        String file()
        {
            return file_;
        }

        long position()
        {
            return position_;
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
            /* Add this to the threshold */
            int bufSize = 4*1024*1024;
            return SequenceFile.fastWriter(file, CommitLog.bufSize_ + bufSize);
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
    private long commitHeaderStartPos_;
    /* Force rollover the commit log on the next insert */
    private boolean forcedRollOver_ = false;


    /*
     * Generates a file name of the format CommitLog-<table>-<timestamp>.log in the
     * directory specified by the Database Descriptor.
    */
    private void setNextFileName()
    {
        logFile_ = DatabaseDescriptor.getLogFileLocation() +
                            System.getProperty("file.separator") +
                            "CommitLog-" +
                            table_ +
                            "-" +
                            System.currentTimeMillis() +
                            ".log";
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
        commitHeaderStartPos_ = 0L;
    }

    String getLogFile()
    {
        return logFile_;
    }

    void readCommitLogHeader(String logFile, byte[] bytes) throws IOException
    {
        IFileReader logReader = SequenceFile.reader(logFile);
        try
        {
            logReader.readDirect(bytes);
        }
        finally
        {
            logReader.close();
        }
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
        commitHeaderStartPos_ = logWriter_.getCurrentPosition();
        /* write the commit log header */
        clHeader_ = new CommitLogHeader(cfSize);
        writeCommitLogHeader(clHeader_.toByteArray(), false);
    }

    private void writeCommitLogHeader(byte[] bytes, boolean reset) throws IOException
    {
        /* record the current position */
        long currentPos = logWriter_.getCurrentPosition();
        logWriter_.seek(commitHeaderStartPos_);
        /* write the commit log header */
        logWriter_.writeDirect(bytes);
        if ( reset )
        {
            /* seek back to the old position */
            logWriter_.seek(currentPos);
        }
    }

    void recover(List<File> clogs) throws IOException
    {
        Table table = Table.open(table_);
        int cfSize = table.getNumberOfColumnFamilies();
        int size = CommitLogHeader.size(cfSize);
        byte[] header = new byte[size];
        byte[] header2 = new byte[size];
        int index = clogs.size() - 1;

        File file = clogs.get(index);
        readCommitLogHeader(file.getAbsolutePath(), header);

        Stack<File> filesNeeded = new Stack<File>();
        filesNeeded.push(file);

        /*
         * Identify files that we need for processing. This can be done
         * using the information in the header of each file. Simply and
         * the byte[] (which are the headers) and stop at the file where
         * the result is a zero.
        */
        for ( int i = (index - 1); i >= 0; --i )
        {
            file = clogs.get(i);
            readCommitLogHeader(file.getAbsolutePath(), header2);
            byte[] result = CommitLogHeader.and(header, header2);
            if ( !CommitLogHeader.isZero(result) )
            {
                filesNeeded.push(file);
            }
            else
            {
                break;
            }
        }

        doRecovery(filesNeeded, header);
    }

    private void printHeader(byte[] header)
    {
        StringBuilder sb = new StringBuilder("");
        for ( byte b : header )
        {
            sb.append(b);
            sb.append(" ");
        }
        logger_.debug(sb.toString());
    }

    private void doRecovery(Stack<File> filesNeeded, byte[] header) throws IOException
    {
        Table table = Table.open(table_);

        DataInputBuffer bufIn = new DataInputBuffer();
        DataOutputBuffer bufOut = new DataOutputBuffer();        

        while ( !filesNeeded.isEmpty() )
        {
            File file = filesNeeded.pop();
            // IFileReader reader = SequenceFile.bufferedReader(file.getAbsolutePath(), DatabaseDescriptor.getLogFileSizeThreshold());
            IFileReader reader = SequenceFile.reader(file.getAbsolutePath());
            try
            {
                Map<String, Row> rows = new HashMap<String, Row>();
                reader.readDirect(header);
                /* deserialize the commit log header */
                bufIn.reset(header, 0, header.length);
                CommitLogHeader clHeader = CommitLogHeader.serializer().deserialize(bufIn);
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

                /* read the logs populate RowMutation and apply */
                while ( !reader.isEOF() )
                {
                    bufOut.reset();
                    long bytesRead = reader.next(bufOut);
                    if ( bytesRead == -1 )
                        break;

                    bufIn.reset(bufOut.getData(), bufOut.getLength());
                    /* Skip over the commit log key portion */
                    bufIn.readUTF();
                    /* Skip over data size */
                    bufIn.readInt();
                    
                    /* read the commit log entry */
                    try
                    {                        
                        Row row = Row.serializer().deserialize(bufIn);
                        Map<String, ColumnFamily> columnFamilies = new HashMap<String, ColumnFamily>(row.getColumnFamilies());
                        /* remove column families that have already been flushed */
                    	Set<String> cNames = columnFamilies.keySet();

                        for ( String cName : cNames )
                        {
                        	ColumnFamily columnFamily = columnFamilies.get(cName);
                        	/* TODO: Remove this to not process Hints */
                        	if ( !DatabaseDescriptor.isApplicationColumnFamily(cName) )
                        	{
                        		row.removeColumnFamily(columnFamily);
                        		continue;
                        	}	
                            int id = table.getColumnFamilyId(columnFamily.name());
                            if ( clHeader.get(id) == 0 || reader.getCurrentPosition() < clHeader.getPosition(id) )
                                row.removeColumnFamily(columnFamily);
                        }
                        if ( !row.isEmpty() )
                        {                            
                        	table.applyNow(row);
                        }
                    }
                    catch ( IOException e )
                    {
                        logger_.debug( LogUtil.throwableToString(e) );
                    }
                }
                reader.close();
                /* apply the rows read */
                table.flush(true);
            }
            catch ( Throwable th )
            {
                logger_.info( LogUtil.throwableToString(th) );
                /* close the reader and delete this commit log. */
                reader.close();
                FileUtils.delete( new File[]{file} );
            }
        }
    }

    /*
     * Update the header of the commit log if a new column family
     * is encountered for the first time.
    */
    private void updateHeader(Row row) throws IOException
    {
    	Map<String, ColumnFamily> columnFamilies = row.getColumnFamilies();
        Table table = Table.open(table_);
        Set<String> cNames = columnFamilies.keySet();
        for ( String cName : cNames )
        {
        	ColumnFamily columnFamily = columnFamilies.get(cName);
        	int id = table.getColumnFamilyId(columnFamily.name());
        	if ( clHeader_.get(id) == 0 || ( clHeader_.get(id) == 1 && clHeader_.getPosition(id) == 0 ) )
        	{
            	if ( clHeader_.get(id) == 0 || ( clHeader_.get(id) == 1 && clHeader_.getPosition(id) == 0 ) )
            	{
	        		clHeader_.turnOn( id, logWriter_.getCurrentPosition() );
	        		writeCommitLogHeader(clHeader_.toByteArray(), true);
            	}
        	}
        }
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
        long fileSize = 0L;
        
        try
        {
            /* serialize the row */
            cfBuffer.reset();
            Row.serializer().serialize(row, cfBuffer);
            currentPosition = logWriter_.getCurrentPosition();
            cLogCtx = new CommitLogContext(logFile_, currentPosition);
            /* Update the header */
            updateHeader(row);
            logWriter_.append(table_, cfBuffer);
            fileSize = logWriter_.getFileSize();                       
            checkThresholdAndRollLog(fileSize);            
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
     * should get rid of Fast Sync mode altogether. If there is
     * a pathological event where few CF's are rarely being updated
     * then their Memtable never gets flushed.
     * This will prevent commit logs from being deleted. WE NEED to
     * fix this using some hueristic and force flushing such Memtables.
     *
     * param @ cLogCtx The commitLog context .
     * param @ id id of the columnFamily being flushed to disk.
     *
    */
    private void discard(CommitLog.CommitLogContext cLogCtx, int id) throws IOException
    {
        /* retrieve the commit log header associated with the file in the context */
        CommitLogHeader commitLogHeader = clHeaders_.get(cLogCtx.file());
        if(commitLogHeader == null )
        {
            if( logFile_.equals(cLogCtx.file()) )
            {
                /* this means we are dealing with the current commit log. */
                commitLogHeader = clHeader_;
                clHeaders_.put(cLogCtx.file(), clHeader_);
            }
            else
                return;
        }
        /*
         * We do any processing only if there is a change in the position in the context.
         * This can happen if an older Memtable's flush comes in after a newer Memtable's
         * flush. Right now this cannot happen since Memtables are flushed on a single
         * thread.
        */
        if ( cLogCtx.position() < commitLogHeader.getPosition(id) )
            return;
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
            if(oldFile.equals(cLogCtx.file()))
            {
                /*
                 * We need to turn on again. This is because we always keep
                 * the bit turned on and the position indicates from where the
                 * commit log needs to be read. When a flush occurs we turn off
                 * perform & operation and then turn on with the new position.
                */
                commitLogHeader.turnOn(id, cLogCtx.position());
                writeCommitLogHeader(cLogCtx.file(), commitLogHeader.toByteArray());
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

    private void checkThresholdAndRollLog( long fileSize )
    {
        try
        {
            if ( fileSize >= DatabaseDescriptor.getLogFileSizeThreshold() || forcedRollOver_ )
            {
                if ( logWriter_.getFileSize() >= DatabaseDescriptor.getLogFileSizeThreshold() || forcedRollOver_ )
                {
	                /* Rolls the current log file over to a new one. */
	                setNextFileName();
	                String oldLogFile = logWriter_.getFileName();
	                //history_.add(oldLogFile);
	                logWriter_.close();
	
	                /* point reader/writer to a new commit log file. */
	                // logWriter_ = SequenceFile.writer(logFile_);
	                logWriter_ = CommitLog.createWriter(logFile_);
	                /* squirrel away the old commit log header */
	                clHeaders_.put(oldLogFile, new CommitLogHeader( clHeader_ ));
	                /*
	                 * We need to zero out positions because the positions in
	                 * the old file do not make sense in the new one.
	                */
	                clHeader_.zeroPositions();
	                writeCommitLogHeader(clHeader_.toByteArray(), false);
	                // Get the list of files in commit log directory if it is greater than a certain number  
	                // Force flush all the column families that way we ensure that a slowly populated column family is not screwing up 
	                // by accumulating the commit logs .
                }
            }
        }
        catch ( IOException e )
        {
            logger_.info(LogUtil.throwableToString(e));
        }
        finally
        {
        	forcedRollOver_ = false;
        }
    }

    public void setForcedRollOver()
    {
    	forcedRollOver_ = true;
    }

    synchronized  public void snapshot( String snapshotDirectory ) throws IOException
    {
        Map<String, List<File>> tableToCommitLogs = RecoveryManager.getListOFCommitLogsPerTable();
        List<File> clogs = tableToCommitLogs.get(table_);
        
        if( clogs != null )
        {
        	File snapshotDir = new File(snapshotDirectory);
        	if( !snapshotDir.exists() )
        		snapshotDir.mkdir();
        	File commitLogSnapshotDir = new File(snapshotDirectory + System.getProperty("file.separator") + "CommitLogs");
        	if( !commitLogSnapshotDir.exists() )
        		commitLogSnapshotDir.mkdir();
            for (File file : clogs)
            {
            	Path existingLink = file.toPath();
            	File hardLinkFile = new File(commitLogSnapshotDir.getAbsolutePath() + System.getProperty("file.separator") + file.getName());
            	Path hardLink = hardLinkFile.toPath();
            	hardLink.createLink(existingLink);
            }
        }
    }
    public static void main(String[] args) throws Throwable
    {
        LogUtil.init();

        // the return value is not used in this case
        DatabaseDescriptor.init();
        
        File logDir = new File(DatabaseDescriptor.getLogFileLocation());
        File[] files = logDir.listFiles();
        Arrays.sort( files, new FileUtils.FileComparator() );

        byte[] bytes = new byte[CommitLogHeader.size(Integer.parseInt(args[0]))];
        for ( File file : files )
        {
            CommitLog clog = new CommitLog( file );
            clog.readCommitLogHeader(file.getAbsolutePath(), bytes);
            DataInputBuffer bufIn = new DataInputBuffer();
            bufIn.reset(bytes, 0, bytes.length);
            CommitLogHeader clHeader = CommitLogHeader.serializer().deserialize(bufIn);
            /*
            StringBuilder sb = new StringBuilder("");
            for ( byte b : bytes )
            {
                sb.append(b);
                sb.append(" ");
            }
            */
            System.out.println("FILE:" + file);
            System.out.println(clHeader.toString());
        }
    }
}