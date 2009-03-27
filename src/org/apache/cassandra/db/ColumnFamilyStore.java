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

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.log4j.Logger;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.io.DataInputBuffer;
import org.apache.cassandra.io.DataOutputBuffer;
import org.apache.cassandra.io.IndexHelper;
import org.apache.cassandra.io.SSTable;
import org.apache.cassandra.io.SequenceFile;
import org.apache.cassandra.net.EndPoint;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.BloomFilter;
import org.apache.cassandra.utils.FileUtils;
import org.apache.cassandra.utils.LogUtil;

/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public class ColumnFamilyStore
{
    private static int threshHold_ = 4;
    private static final int bufSize_ = 128*1024*1024;
    private static int compactionMemoryThreshold_ = 1 << 30;
    private static Logger logger_ = Logger.getLogger(ColumnFamilyStore.class);

    private String table_;
    public String columnFamily_;

    /* This is used to generate the next index for a SSTable */
    private AtomicInteger fileIndexGenerator_ = new AtomicInteger(0);

    /* memtable associated with this ColumnFamilyStore. */
    private AtomicReference<Memtable> memtable_;
    private AtomicReference<BinaryMemtable> binaryMemtable_;

    /* SSTables on disk for this column family */
    private Set<String> ssTables_ = new HashSet<String>();

    /* Modification lock used for protecting reads from compactions. */
    private ReentrantReadWriteLock lock_ = new ReentrantReadWriteLock(true);

    /* Flag indicates if a compaction is in process */
    private AtomicBoolean isCompacting_ = new AtomicBoolean(false);

    ColumnFamilyStore(String table, String columnFamily) throws IOException
    {
        table_ = table;
        columnFamily_ = columnFamily;
        /*
         * Get all data files associated with old Memtables for this table.
         * These files are named as follows <Table>-1.db, ..., <Table>-n.db. Get
         * the max which in this case is n and increment it to use it for next
         * index.
         */
        List<Integer> indices = new ArrayList<Integer>();
        String[] dataFileDirectories = DatabaseDescriptor.getAllDataFileLocations();
        for ( String directory : dataFileDirectories )
        {
            File fileDir = new File(directory);
            File[] files = fileDir.listFiles();
            for (File file : files)
            {
                String filename = file.getName();
                String[] tblCfName = getTableAndColumnFamilyName(filename);
                
                if (tblCfName[0].equals(table_)
                        && tblCfName[1].equals(columnFamily))
                {
                    int index = getIndexFromFileName(filename);
                    indices.add(index);
                }
            }
        }
        Collections.sort(indices);
        int value = (indices.size() > 0) ? (indices.get(indices.size() - 1)) : 0;
        fileIndexGenerator_.set(value);
        memtable_ = new AtomicReference<Memtable>( new Memtable(table_, columnFamily_) );
        binaryMemtable_ = new AtomicReference<BinaryMemtable>( new BinaryMemtable(table_, columnFamily_) );
    }

    void onStart() throws IOException
    {
        /* Do major compaction */
        List<File> ssTables = new ArrayList<File>();
        String[] dataFileDirectories = DatabaseDescriptor.getAllDataFileLocations();
        for ( String directory : dataFileDirectories )
        {
            File fileDir = new File(directory);
            File[] files = fileDir.listFiles();
            for (File file : files)
            {
                String filename = file.getName();
                if(((file.length() == 0) || (filename.contains("-" + SSTable.temporaryFile_)) ) && (filename.contains(columnFamily_)))
                {
                	file.delete();
                	continue;
                }
                
                String[] tblCfName = getTableAndColumnFamilyName(filename);
                if (tblCfName[0].equals(table_)
                        && tblCfName[1].equals(columnFamily_)
                        && filename.contains("-Data.db"))
                {
                    ssTables.add(file.getAbsoluteFile());
                }
            }
        }
        Collections.sort(ssTables, new FileUtils.FileComparator());
        List<String> filenames = new ArrayList<String>();
        for (File ssTable : ssTables)
        {
            filenames.add(ssTable.getAbsolutePath());
        }

        /* There are no files to compact just add to the list of SSTables */
        ssTables_.addAll(filenames);
        /* Load the index files and the Bloom Filters associated with them. */
        SSTable.onStart(filenames);
        logger_.debug("Submitting a major compaction task ...");
        MinorCompactionManager.instance().submit(ColumnFamilyStore.this);
        if(columnFamily_.equals(Table.hints_))
        {
        	HintedHandOffManager.instance().submit(this);
        }
        MinorCompactionManager.instance().submitPeriodicCompaction(this);
    }

    List<String> getAllSSTablesOnDisk()
    {
        return new ArrayList<String>(ssTables_);
    }

    /*
     * This method is called to obtain statistics about
     * the Column Family represented by this Column Family
     * Store. It will report the total number of files on
     * disk and the total space oocupied by the data files
     * associated with this Column Family.
    */
    public String cfStats(String newLineSeparator)
    {
        StringBuilder sb = new StringBuilder();
        /*
         * We want to do this so that if there are
         * no files on disk we do not want to display
         * something ugly on the admin page.
        */
        if ( ssTables_.size() == 0 )
        {
            return sb.toString();
        }
        sb.append(columnFamily_ + " statistics :");
        sb.append(newLineSeparator);
        sb.append("Number of files on disk : " + ssTables_.size());
        sb.append(newLineSeparator);
        double totalSpace = 0d;
        for ( String file : ssTables_ )
        {
            File f = new File(file);
            totalSpace += f.length();
        }
        String diskSpace = FileUtils.stringifyFileSize(totalSpace);
        sb.append("Total disk space : " + diskSpace);
        sb.append(newLineSeparator);
        sb.append("--------------------------------------");
        sb.append(newLineSeparator);
        return sb.toString();
    }

    /*
     * This is called after bootstrap to add the files
     * to the list of files maintained.
    */
    void addToList(String file)
    {
    	lock_.writeLock().lock();
        try
        {
            ssTables_.add(file);
        }
        finally
        {
        	lock_.writeLock().unlock();
        }
    }

    void touch(String key, boolean fData) throws IOException
    {
        /* Scan the SSTables on disk first */
        lock_.readLock().lock();
        try
        {
            List<String> files = new ArrayList<String>(ssTables_);
            for (String file : files)
            {
                /*
                 * Get the BloomFilter associated with this file. Check if the key
                 * is present in the BloomFilter. If not continue to the next file.
                */
                boolean bVal = SSTable.isKeyInFile(key, file);
                if ( !bVal )
                    continue;
                SSTable ssTable = new SSTable(file);
                ssTable.touch(key, fData);
            }
        }
        finally
        {
            lock_.readLock().unlock();
        }
    }

    /*
     * This method forces a compaction of the SSTables on disk. We wait
     * for the process to complete by waiting on a future pointer.
    */
    boolean forceCompaction(List<Range> ranges, EndPoint target, long skip, List<String> fileList)
    {        
    	Future<Boolean> futurePtr = null;
    	if( ranges != null)
    		futurePtr = MinorCompactionManager.instance().submit(ColumnFamilyStore.this, ranges, target, fileList);
    	else
    		MinorCompactionManager.instance().submitMajor(ColumnFamilyStore.this, skip);
    	
        boolean result = true;
        try
        {
            /* Waiting for the compaction to complete. */
        	if(futurePtr != null)
        		result = futurePtr.get();
            logger_.debug("Done forcing compaction ...");
        }
        catch (ExecutionException ex)
        {
            logger_.debug(LogUtil.throwableToString(ex));
        }
        catch ( InterruptedException ex2 )
        {
            logger_.debug(LogUtil.throwableToString(ex2));
        }
        return result;
    }

    String getColumnFamilyName()
    {
        return columnFamily_;
    }

    private String[] getTableAndColumnFamilyName(String filename)
    {
        StringTokenizer st = new StringTokenizer(filename, "-");
        String[] values = new String[2];
        int i = 0;
        while (st.hasMoreElements())
        {
            if (i == 0)
                values[i] = (String) st.nextElement();
            else if (i == 1)
            {
                values[i] = (String) st.nextElement();
                break;
            }
            ++i;
        }
        return values;
    }

    protected static int getIndexFromFileName(String filename)
    {
        /*
         * File name is of the form <table>-<column family>-<index>-Data.db.
         * This tokenizer will strip the .db portion.
         */
        StringTokenizer st = new StringTokenizer(filename, "-");
        /*
         * Now I want to get the index portion of the filename. We accumulate
         * the indices and then sort them to get the max index.
         */
        int count = st.countTokens();
        int i = 0;
        String index = null;
        while (st.hasMoreElements())
        {
            index = (String) st.nextElement();
            if (i == (count - 2))
                break;
            ++i;
        }
        return Integer.parseInt(index);
    }

    String getNextFileName()
    {
    	// Psuedo increment so that we do not generate consecutive numbers 
    	fileIndexGenerator_.incrementAndGet();
        return table_ + "-" + columnFamily_ + "-" + fileIndexGenerator_.incrementAndGet();
    }

    /*
     * Return a temporary file name.
     */
    String getTempFileName()
    {
    	// Psuedo increment so that we do not generate consecutive numbers 
    	fileIndexGenerator_.incrementAndGet();
        return table_ + "-" + columnFamily_ + "-" + SSTable.temporaryFile_ + "-" + fileIndexGenerator_.incrementAndGet();
    }

    /*
     * Return a temporary file name. Based on the list of files input 
     * This fn sorts the list and generates a number between he 2 lowest filenames 
     * ensuring uniqueness.
     * Since we do not generate consecutive numbers hence the lowest file number
     * can just be incremented to generate the next file. 
     */
    String getTempFileName( List<String> files)
    {
    	int lowestIndex = 0 ;
    	int index = 0;
    	Collections.sort(files, new FileNameComparator(FileNameComparator.Ascending));
    	
    	if( files.size() <= 1)
    		return null;
    	lowestIndex = getIndexFromFileName(files.get(0));
   		
   		index = lowestIndex + 1 ;

        return table_ + "-" + columnFamily_ + "-" + SSTable.temporaryFile_ + "-" + index;
    }

    
    /*
     * This version is used only on start up when we are recovering from logs.
     * In the future we may want to parellelize the log processing for a table
     * by having a thread per log file present for recovery. Re-visit at that
     * time.
     */
    void switchMemtable(String key, ColumnFamily columnFamily, CommitLog.CommitLogContext cLogCtx) throws IOException
    {
        memtable_.set( new Memtable(table_, columnFamily_) );
        if(!key.equals(Memtable.flushKey_))
        	memtable_.get().put(key, columnFamily, cLogCtx);
    }

    /*
     * This version is used only on start up when we are recovering from logs.
     * In the future we may want to parellelize the log processing for a table
     * by having a thread per log file present for recovery. Re-visit at that
     * time.
     */
    void switchBinaryMemtable(String key, byte[] buffer) throws IOException
    {
        binaryMemtable_.set( new BinaryMemtable(table_, columnFamily_) );
        binaryMemtable_.get().put(key, buffer);
    }

    void forceFlush() throws IOException
    {
        //MemtableManager.instance().submit(getColumnFamilyName(), memtable_.get() , CommitLog.CommitLogContext.NULL);
        //memtable_.get().flush(true, CommitLog.CommitLogContext.NULL);
        memtable_.get().forceflush(this);
    }

    void forceFlushBinary()
    {
        BinaryMemtableManager.instance().submit(getColumnFamilyName(), binaryMemtable_.get());
        //binaryMemtable_.get().flush(true);
    }

    /**
     * Insert/Update the column family for this key. 
     * param @ lock - lock that needs to be used. 
     * param @ key - key for update/insert 
     * param @ columnFamily - columnFamily changes
    */
    void apply(String key, ColumnFamily columnFamily, CommitLog.CommitLogContext cLogCtx)
            throws IOException
    {
        memtable_.get().put(key, columnFamily, cLogCtx);
    }

    /*
     * Insert/Update the column family for this key. param @ lock - lock that
     * needs to be used. param @ key - key for update/insert param @
     * columnFamily - columnFamily changes
     */
    void applyBinary(String key, byte[] buffer)
            throws IOException
    {
        binaryMemtable_.get().put(key, buffer);
    }

    public ColumnFamily getColumnFamily(String key, String columnFamilyColumn, IFilter filter) throws IOException
    {
        List<ColumnFamily> columnFamilies = getColumnFamilies(key, columnFamilyColumn, filter);
        return resolveAndRemoveDeleted(columnFamilies);
    }

    /**
     *
     * Get the column family in the most efficient order.
     * 1. Memtable
     * 2. Sorted list of files
     */
    List<ColumnFamily> getColumnFamilies(String key, String columnFamilyColumn, IFilter filter) throws IOException
    {
        List<ColumnFamily> columnFamilies1 = new ArrayList<ColumnFamily>();
        /* Get the ColumnFamily from Memtable */
        getColumnFamilyFromCurrentMemtable(key, columnFamilyColumn, filter, columnFamilies1);
        if (columnFamilies1.size() == 0 || !filter.isDone())
        {
            /* Check if MemtableManager has any historical information */
            MemtableManager.instance().getColumnFamily(key, columnFamily_, columnFamilyColumn, filter, columnFamilies1);
        }
        List<ColumnFamily> columnFamilies = columnFamilies1;
        if (columnFamilies.size() == 0 || !filter.isDone())
        {
            long start = System.currentTimeMillis();
            getColumnFamilyFromDisk(key, columnFamilyColumn, columnFamilies, filter);
            logger_.debug("DISK TIME: " + (System.currentTimeMillis() - start) + " ms.");
        }
        return columnFamilies;
    }

    /**
     * Fetch from disk files and go in sorted order  to be efficient
     * This fn exits as soon as the required data is found.
     * @param key
     * @param cf
     * @param columnFamilies
     * @param filter
     * @throws IOException
     */
    private void getColumnFamilyFromDisk(String key, String cf, List<ColumnFamily> columnFamilies, IFilter filter) throws IOException
    {
        /* Scan the SSTables on disk first */
        List<String> files = new ArrayList<String>();        
    	lock_.readLock().lock();
        try
        {
            files.addAll(ssTables_);
            Collections.sort(files, new FileNameComparator(FileNameComparator.Descending));
        }
        finally
        {
            lock_.readLock().unlock();
        }
    		        	        
        for (String file : files)
        {
            /*
             * Get the BloomFilter associated with this file. Check if the key
             * is present in the BloomFilter. If not continue to the next file.
            */
            boolean bVal = SSTable.isKeyInFile(key, file);
            if ( !bVal )
                continue;
            ColumnFamily columnFamily = fetchColumnFamily(key, cf, filter, file);
            long start = System.currentTimeMillis();
            if (columnFamily != null)
            {
                columnFamilies.add(columnFamily);
                if(filter.isDone())
                {
                	break;
                }
            }
            logger_.debug("DISK Data structure population  TIME: " + (System.currentTimeMillis() - start) + " ms.");
        }
    }


    private ColumnFamily fetchColumnFamily(String key, String cf, IFilter filter, String ssTableFile) throws IOException
	{
		SSTable ssTable = new SSTable(ssTableFile);
		long start = System.currentTimeMillis();
		DataInputBuffer bufIn = null;
		bufIn = filter.next(key, cf, ssTable);
		logger_.debug("DISK ssTable.next TIME: " + (System.currentTimeMillis() - start) + " ms.");
		if (bufIn.getLength() == 0)
			return null;
        start = System.currentTimeMillis();
        ColumnFamily columnFamily = ColumnFamily.serializer().deserialize(bufIn, cf, filter);
		logger_.debug("DISK Deserialize TIME: " + (System.currentTimeMillis() - start) + " ms.");
		if (columnFamily == null)
			return null;
		return columnFamily;
	}

    private void getColumnFamilyFromCurrentMemtable(String key, String cf, IFilter filter, List<ColumnFamily> columnFamilies)
    {
        /* Get the ColumnFamily from Memtable */
        ColumnFamily columnFamily = memtable_.get().get(key, cf, filter);
        if (columnFamily != null)
        {
            columnFamilies.add(columnFamily);
        }
    }
    
    /** merge all columnFamilies into a single instance, with only the newest versions of columns preserved. */
    static ColumnFamily resolve(List<ColumnFamily> columnFamilies)
    {
        int size = columnFamilies.size();
        if (size == 0)
            return null;

        // start from nothing so that we don't include potential deleted columns from the first instance
        String cfname = columnFamilies.get(0).name();
        ColumnFamily cf = new ColumnFamily(cfname);

        // merge
        for (ColumnFamily cf2 : columnFamilies)
        {
            assert cf.name().equals(cf2.name());
            cf.addColumns(cf2);
            cf.delete(Math.max(cf.getMarkedForDeleteAt(), cf2.getMarkedForDeleteAt()));
        }
        return cf;
    }

    /** like resolve, but leaves the resolved CF as the only item in the list */
    private static void merge(List<ColumnFamily> columnFamilies)
    {
        ColumnFamily cf = resolve(columnFamilies);
        columnFamilies.clear();
        columnFamilies.add(cf);
    }

    private static ColumnFamily resolveAndRemoveDeleted(List<ColumnFamily> columnFamilies) {
        ColumnFamily cf = resolve(columnFamilies);
        return removeDeleted(cf);
    }

    static ColumnFamily removeDeleted(ColumnFamily cf) {
        if (cf == null) {
            return null;
        }
        for (String cname : new ArrayList<String>(cf.getColumns().keySet())) {
            IColumn c = cf.getColumns().get(cname);
            if (c instanceof SuperColumn) {
                long min_timestamp = Math.max(c.getMarkedForDeleteAt(), cf.getMarkedForDeleteAt());
                // don't operate directly on the supercolumn, it could be the one in the memtable
                cf.remove(cname);
                IColumn sc = new SuperColumn(cname);
                for (IColumn subColumn : c.getSubColumns()) {
                    if (!subColumn.isMarkedForDelete() && subColumn.timestamp() >= min_timestamp) {
                        sc.addColumn(subColumn.name(), subColumn);
                    }
                }
                if (sc.getSubColumns().size() > 0) {
                    cf.addColumn(sc);
                }
            } else if (c.isMarkedForDelete() || c.timestamp() < cf.getMarkedForDeleteAt()) {
                cf.remove(cname);
            }
        }
        return cf;
    }

    /*
     * This version is used only on start up when we are recovering from logs.
     * Hence no locking is required since we process logs on the main thread. In
     * the future we may want to parellelize the log processing for a table by
     * having a thread per log file present for recovery. Re-visit at that time.
     */
    void applyNow(String key, ColumnFamily columnFamily) throws IOException
    {
         memtable_.get().putOnRecovery(key, columnFamily);
    }

    /*
     * This method is called when the Memtable is frozen and ready to be flushed
     * to disk. This method informs the CommitLog that a particular ColumnFamily
     * is being flushed to disk.
     */
    void onMemtableFlush(CommitLog.CommitLogContext cLogCtx) throws IOException
    {
        if ( cLogCtx.isValidContext() )
            CommitLog.open(table_).onMemtableFlush(columnFamily_, cLogCtx);
    }

    /*
     * Called after the Memtable flushes its in-memory data. This information is
     * cached in the ColumnFamilyStore. This is useful for reads because the
     * ColumnFamilyStore first looks in the in-memory store and the into the
     * disk to find the key. If invoked during recoveryMode the
     * onMemtableFlush() need not be invoked.
     *
     * param @ filename - filename just flushed to disk
     * param @ bf - bloom filter which indicates the keys that are in this file.
    */
    void storeLocation(String filename, BloomFilter bf)
    {
        boolean doCompaction = false;
        int ssTableSize = 0;
    	lock_.writeLock().lock();
        try
        {
            ssTables_.add(filename);
            SSTable.storeBloomFilter(filename, bf);
            ssTableSize = ssTables_.size();
        }
        finally
        {
        	lock_.writeLock().unlock();
        }
        if (ssTableSize >= threshHold_ && !isCompacting_.get())
        {
            doCompaction = true;
        }

        if (isCompacting_.get())
        {
            if ( ssTableSize % threshHold_ == 0 )
            {
                doCompaction = true;
            }
        }
        if ( doCompaction )
        {
            logger_.debug("Submitting for  compaction ...");
            MinorCompactionManager.instance().submit(ColumnFamilyStore.this);
            logger_.debug("Submitted for compaction ...");
        }
    }

    PriorityQueue<FileStruct> initializePriorityQueue(List<String> files, List<Range> ranges, int minBufferSize)
    {
        PriorityQueue<FileStruct> pq = new PriorityQueue<FileStruct>();
        if (files.size() > 1 || (ranges != null &&  files.size() > 0))
        {
            int bufferSize = Math.min( (ColumnFamilyStore.compactionMemoryThreshold_ / files.size()), minBufferSize ) ;
            FileStruct fs = null;
            for (String file : files)
            {
            	try
            	{
            		fs = new FileStruct();
	                fs.bufIn_ = new DataInputBuffer();
	                fs.bufOut_ = new DataOutputBuffer();
	                fs.reader_ = SequenceFile.bufferedReader(file, bufferSize);                    
	                fs.key_ = null;
	                fs = getNextKey(fs);
	                if(fs == null)
	                	continue;
	                pq.add(fs);
            	}
            	catch ( Exception ex)
            	{
            		ex.printStackTrace();
            		try
            		{
            			if(fs != null)
            			{
            				fs.reader_.close();
            			}
            		}
            		catch(Exception e)
            		{
            			logger_.warn("Unable to close file :" + file);
            		}
                }
            }
        }
        return pq;
    }

    /*
     * Stage the compactions , compact similar size files.
     * This fn figures out the files close enough by size and if they
     * are greater than the threshold then compacts.
     */
    Map<Integer, List<String>> stageOrderedCompaction(List<String> files)
    {
        // Sort the files based on the generation ID 
        Collections.sort(files, new FileNameComparator(FileNameComparator.Ascending));
    	Map<Integer, List<String>>  buckets = new HashMap<Integer, List<String>>();
    	int maxBuckets = 1000;
    	long averages[] = new long[maxBuckets];
    	long min = 50L*1024L*1024L;
    	Integer i = 0;
    	for(String file : files)
    	{
    		File f = new File(file);
    		long size = f.length();
			if ( (size > averages[i]/2 && size < 3*averages[i]/2) || ( size < min && averages[i] < min ))
			{
				averages[i] = (averages[i] + size) / 2 ;
				List<String> fileList = buckets.get(i);
				if(fileList == null)
				{
					fileList = new ArrayList<String>();
					buckets.put(i, fileList);
				}
				fileList.add(file);
			}
			else
    		{
				if( i >= maxBuckets )
					break;
				i++;
				List<String> fileList = new ArrayList<String>();
				buckets.put(i, fileList);
				fileList.add(file);
    			averages[i] = size;
    		}
    	}
    	return buckets;
    }
       
    /*
     * Break the files into buckets and then compact.
     */
    void doCompaction()
    {
        isCompacting_.set(true);
        List<String> files = new ArrayList<String>(ssTables_);
        try
        {
	        int count = 0;
	    	Map<Integer, List<String>> buckets = stageOrderedCompaction(files);
	    	Set<Integer> keySet = buckets.keySet();
	    	for(Integer key : keySet)
	    	{
	    		List<String> fileList = buckets.get(key);
	    		Collections.sort( fileList , new FileNameComparator( FileNameComparator.Ascending));
	    		if(fileList.size() >= threshHold_ )
	    		{
	    			files.clear();
	    			count = 0;
	    			for(String file : fileList)
	    			{
	    				files.add(file);
	    				count++;
	    				if( count == threshHold_ )
	    					break;
	    			}
	    	        try
	    	        {
	    	        	// For each bucket if it has crossed the threshhold do the compaction
	    	        	// In case of range  compaction merge the counting bloom filters also.
	    	        	if( count == threshHold_)
	    	        		doFileCompaction(files, bufSize_);
	    	        }
	    	        catch ( Exception ex)
	    	        {
                		logger_.warn(LogUtil.throwableToString(ex));
	    	        }
	    		}
	    	}
        }
        finally
        {
        	isCompacting_.set(false);
        }
    }

    void doMajorCompaction(long skip)  throws IOException
    {
    	doMajorCompactionInternal( skip );
    }

    /*
     * Compact all the files irrespective of the size.
     * skip : is the ammount in Gb of the files to be skipped
     * all files greater than skip GB are skipped for this compaction.
     * Except if skip is 0 , in that case this is ignored and all files are taken.
     */
    void doMajorCompactionInternal(long skip)
    {
        isCompacting_.set(true);
        List<String> filesInternal = new ArrayList<String>(ssTables_);
        List<String> files = null;
        try
        {
        	 if( skip > 0L )
        	 {
        		 files = new ArrayList<String>();
	        	 for ( String file : filesInternal )
	        	 {
	        		 File f = new File(file);
	        		 if( f.length() < skip*1024L*1024L*1024L )
	        		 {
	        			 files.add(file);
	        		 }
	        	 }
        	 }
        	 else
        	 {
        		 files = filesInternal;
        	 }
        	 doFileCompaction(files, bufSize_);
        }
        catch ( Exception ex)
        {
        	ex.printStackTrace();
        }
        finally
        {
        	isCompacting_.set(false);
        }
    }

    /*
     * Add up all the files sizes this is the worst case file
     * size for compaction of all the list of files given.
     */
    long getExpectedCompactedFileSize(List<String> files)
    {
    	long expectedFileSize = 0;
    	for(String file : files)
    	{
    		File f = new File(file);
    		long size = f.length();
    		expectedFileSize = expectedFileSize + size;
    	}
    	return expectedFileSize;
    }

    /*
     *  Find the maximum size file in the list .
     */
    String getMaxSizeFile( List<String> files )
    {
    	long maxSize = 0L;
    	String maxFile = null;
    	for ( String file : files )
    	{
    		File f = new File(file);
    		if(f.length() > maxSize )
    		{
    			maxSize = f.length();
    			maxFile = file;
    		}
    	}
    	return maxFile;
    }

    boolean doAntiCompaction(List<Range> ranges, EndPoint target, List<String> fileList)
    {
        isCompacting_.set(true);
        List<String> files = new ArrayList<String>(ssTables_);
        boolean result = true;
        try
        {
        	 result = doFileAntiCompaction(files, ranges, target, fileList, null);
        }
        catch ( Exception ex)
        {
        	ex.printStackTrace();
        }
        finally
        {
        	isCompacting_.set(false);
        }
        return result;

    }

    /*
     * Read the next key from the data file , this fn will skip teh block index
     * and read teh next available key into the filestruct that is passed.
     * If it cannot read or a end of file is reached it will return null.
     */
    FileStruct getNextKey(FileStruct filestruct) throws IOException
    {
        filestruct.bufOut_.reset();
        if (filestruct.reader_.isEOF())
        {
            filestruct.reader_.close();
            return null;
        }
        
        long bytesread = filestruct.reader_.next(filestruct.bufOut_);
        if (bytesread == -1)
        {
            filestruct.reader_.close();
            return null;
        }

        filestruct.bufIn_.reset(filestruct.bufOut_.getData(), filestruct.bufOut_.getLength());
        filestruct.key_ = filestruct.bufIn_.readUTF();
        /* If the key we read is the Block Index Key then we are done reading the keys so exit */
        if ( filestruct.key_.equals(SSTable.blockIndexKey_) )
        {
            filestruct.reader_.close();
            return null;
        }
        return filestruct;
    }

    void forceCleanup()
    {
    	MinorCompactionManager.instance().submitCleanup(ColumnFamilyStore.this);
    }
    
    /**
     * This function goes over each file and removes the keys that the node is not responsible for 
     * and only keeps keys that this node is responsible for.
     * @throws IOException
     */
    void doCleanupCompaction()
    {
        isCompacting_.set(true);
        List<String> files = new ArrayList<String>(ssTables_);
        for(String file: files)
        {
	        try
	        {
	        	doCleanup(file);
	        }
	        catch ( Exception ex)
	        {
	        	ex.printStackTrace();
	        }
        }
    	isCompacting_.set(false);
    }
    /**
     * cleans up one particular file by removing keys that this node is not responsible for.
     * @param file
     * @throws IOException
     */
    /* TODO: Take care of the comments later. */
    void doCleanup(String file) throws IOException
    {
    	if(file == null )
    		return;
        List<Range> myRanges = null;
    	List<String> files = new ArrayList<String>();
    	files.add(file);
    	List<String> newFiles = new ArrayList<String>();
    	Map<EndPoint, List<Range>> endPointtoRangeMap = StorageService.instance().constructEndPointToRangesMap();
    	myRanges = endPointtoRangeMap.get(StorageService.getLocalStorageEndPoint());
    	List<BloomFilter> compactedBloomFilters = new ArrayList<BloomFilter>();
        doFileAntiCompaction(files, myRanges, null, newFiles, compactedBloomFilters);
        logger_.debug("Original file : " + file + " of size " + new File(file).length());
        lock_.writeLock().lock();
        try
        {
            ssTables_.remove(file);
            SSTable.removeAssociatedBloomFilter(file);
            for (String newfile : newFiles)
            {                            	
                logger_.debug("New file : " + newfile + " of size " + new File(newfile).length());
                if ( newfile != null )
                {
                    ssTables_.add(newfile);
                    logger_.debug("Inserting bloom filter for file " + newfile);
                    SSTable.storeBloomFilter(newfile, compactedBloomFilters.get(0));
                }
            }
            SSTable.delete(file);
        }
        finally
        {
            lock_.writeLock().unlock();
        }
    }
    
    /**
     * This function is used to do the anti compaction process , it spits out the file which has keys that belong to a given range
     * If the target is not specified it spits out the file as a compacted file with the unecessary ranges wiped out.
     * @param files
     * @param ranges
     * @param target
     * @param fileList
     * @return
     * @throws IOException
     */
    boolean doFileAntiCompaction(List<String> files, List<Range> ranges, EndPoint target, List<String> fileList, List<BloomFilter> compactedBloomFilters)
    {
    	boolean result = false;
        long startTime = System.currentTimeMillis();
        long totalBytesRead = 0;
        long totalBytesWritten = 0;
        long totalkeysRead = 0;
        long totalkeysWritten = 0;
        String rangeFileLocation = null;
        String mergedFileName = null;
        try
        {
	        // Calculate the expected compacted filesize
	    	long expectedRangeFileSize = getExpectedCompactedFileSize(files);
	    	/* in the worst case a node will be giving out alf of its data so we take a chance */
	    	expectedRangeFileSize = expectedRangeFileSize / 2;
	        rangeFileLocation = DatabaseDescriptor.getCompactionFileLocation(expectedRangeFileSize);
//	        boolean isLoop = isLoopAround( ranges );
//	        Range maxRange = getMaxRange( ranges );
	        // If the compaction file path is null that means we have no space left for this compaction.
	        if( rangeFileLocation == null )
	        {
	            logger_.warn("Total bytes to be written for range compaction  ..."
	                    + expectedRangeFileSize + "   is greater than the safe limit of the disk space available.");
	            return result;
	        }
	        PriorityQueue<FileStruct> pq = initializePriorityQueue(files, ranges, ColumnFamilyStore.bufSize_);
	        if (pq.size() > 0)
	        {
	            mergedFileName = getTempFileName();
	            SSTable ssTableRange = null ;
	            String lastkey = null;
	            List<FileStruct> lfs = new ArrayList<FileStruct>();
	            DataOutputBuffer bufOut = new DataOutputBuffer();
	            int expectedBloomFilterSize = SSTable.getApproximateKeyCount(files);
	            expectedBloomFilterSize = (expectedBloomFilterSize > 0) ? expectedBloomFilterSize : SSTable.indexInterval();
	            logger_.debug("Expected bloom filter size : " + expectedBloomFilterSize);
	            /* Create the bloom filter for the compacted file. */
	            BloomFilter compactedRangeBloomFilter = new BloomFilter(expectedBloomFilterSize, 15);
	            List<ColumnFamily> columnFamilies = new ArrayList<ColumnFamily>();

	            while (pq.size() > 0 || lfs.size() > 0)
	            {
	                FileStruct fs = null;
	                if (pq.size() > 0)
	                {
	                    fs = pq.poll();
	                }
	                if (fs != null
	                        && (lastkey == null || lastkey.compareTo(fs.key_) == 0))
	                {
	                    // The keys are the same so we need to add this to the
	                    // ldfs list
	                    lastkey = fs.key_;
	                    lfs.add(fs);
	                }
	                else
	                {
	                    Collections.sort(lfs, new FileStructComparator());
	                    ColumnFamily columnFamily = null;
	                    bufOut.reset();
	                    if(lfs.size() > 1)
	                    {
		                    for (FileStruct filestruct : lfs)
		                    {
		                    	try
		                    	{
	                                /* read the length although we don't need it */
	                                filestruct.bufIn_.readInt();
	                                // Skip the Index
                                    IndexHelper.skipBloomFilterAndIndex(filestruct.bufIn_);
	                                // We want to add only 2 and resolve them right there in order to save on memory footprint
	                                if(columnFamilies.size() > 1)
	                                {
	    		                        // Now merge the 2 column families
                                        merge(columnFamilies);
	                                }
			                        // deserialize into column families
			                        columnFamilies.add(ColumnFamily.serializer().deserialize(filestruct.bufIn_));
		                    	}
		                    	catch ( Exception ex)
		                    	{
                                    logger_.warn(LogUtil.throwableToString(ex));
                                }
		                    }
		                    // Now after merging all crap append to the sstable
		                    columnFamily = resolveAndRemoveDeleted(columnFamilies);
		                    columnFamilies.clear();
		                    if( columnFamily != null )
		                    {
			                	/* serialize the cf with column indexes */
			                    ColumnFamily.serializerWithIndexes().serialize(columnFamily, bufOut);
		                    }
	                    }
	                    else
	                    {
		                    FileStruct filestruct = lfs.get(0);
	                    	try
	                    	{
		                        /* read the length although we don't need it */
		                        int size = filestruct.bufIn_.readInt();
		                        bufOut.write(filestruct.bufIn_, size);
	                    	}
	                    	catch ( Exception ex)
	                    	{
	                    		logger_.warn(LogUtil.throwableToString(ex));
	                            filestruct.reader_.close();
	                            continue;
	                    	}
	                    }
	                    if ( Range.isKeyInRanges(lastkey, ranges) )
	                    {
	                        if(ssTableRange == null )
	                        {
	                        	if( target != null )
	                        		rangeFileLocation = rangeFileLocation + System.getProperty("file.separator") + "bootstrap";
	                	        FileUtils.createDirectory(rangeFileLocation);
	                            ssTableRange = new SSTable(rangeFileLocation, mergedFileName);
	                        }	                        
	                        try
	                        {
		                        ssTableRange.append(lastkey, bufOut);
		                        compactedRangeBloomFilter.fill(lastkey);                                
	                        }
	                        catch(Exception ex)
	                        {
	                            logger_.warn( LogUtil.throwableToString(ex) );
	                        }
	                    }
	                    totalkeysWritten++;
	                    for (FileStruct filestruct : lfs)
	                    {
	                    	try
	                    	{
	                    		filestruct = getNextKey	( filestruct );
	                    		if(filestruct == null)
	                    		{
	                    			continue;
	                    		}
	                    		/* keep on looping until we find a key in the range */
	                            while ( !Range.isKeyInRanges(filestruct.key_, ranges) )
	                            {
		                    		filestruct = getNextKey	( filestruct );
		                    		if(filestruct == null)
		                    		{
		                    			break;
		                    		}
	        	                    /* check if we need to continue , if we are done with ranges empty the queue and close all file handles and exit */
	        	                    //if( !isLoop && StorageService.token(filestruct.key).compareTo(maxRange.right()) > 0 && !filestruct.key.equals(""))
	        	                    //{
	                                    //filestruct.reader.close();
	                                    //filestruct = null;
	                                    //break;
	        	                    //}
	                            }
	                            if ( filestruct != null)
	                            {
	                            	pq.add(filestruct);
	                            }
		                        totalkeysRead++;
	                    	}
	                    	catch ( Exception ex )
	                    	{
	                    		// Ignore the exception as it might be a corrupted file
	                    		// in any case we have read as far as possible from it
	                    		// and it will be deleted after compaction.
                                logger_.warn(LogUtil.throwableToString(ex));
	                            filestruct.reader_.close();
                            }
	                    }
	                    lfs.clear();
	                    lastkey = null;
	                    if (fs != null)
	                    {
	                        // Add back the fs since we processed the rest of
	                        // filestructs
	                        pq.add(fs);
	                    }
	                }
	            }
	            if( ssTableRange != null )
	            {
                    if ( fileList == null )
                        fileList = new ArrayList<String>();
                    ssTableRange.closeRename(compactedRangeBloomFilter, fileList);
                    if(compactedBloomFilters != null)
                    	compactedBloomFilters.add(compactedRangeBloomFilter);
	            }
	        }
        }
        catch ( Exception ex)
        {
            logger_.warn( LogUtil.throwableToString(ex) );
        }
        logger_.debug("Total time taken for range split   ..."
                + (System.currentTimeMillis() - startTime));
        logger_.debug("Total bytes Read for range split  ..." + totalBytesRead);
        logger_.debug("Total bytes written for range split  ..."
                + totalBytesWritten + "   Total keys read ..." + totalkeysRead);
        return result;
    }

    private void doFill(BloomFilter bf, String decoratedKey)
    {
        bf.fill(StorageService.getPartitioner().undecorateKey(decoratedKey));
    }
    
    /*
     * This function does the actual compaction for files.
     * It maintains a priority queue of with the first key from each file
     * and then removes the top of the queue and adds it to the SStable and
     * repeats this process while reading the next from each file until its
     * done with all the files . The SStable to which the keys are written
     * represents the new compacted file. Before writing if there are keys
     * that occur in multiple files and are the same then a resolution is done
     * to get the latest data.
     *
     */
    void  doFileCompaction(List<String> files,  int minBufferSize)
    {
    	String newfile = null;
        long startTime = System.currentTimeMillis();
        long totalBytesRead = 0;
        long totalBytesWritten = 0;
        long totalkeysRead = 0;
        long totalkeysWritten = 0;
        try
        {
	        // Calculate the expected compacted filesize
	    	long expectedCompactedFileSize = getExpectedCompactedFileSize(files);
	        String compactionFileLocation = DatabaseDescriptor.getCompactionFileLocation(expectedCompactedFileSize);
	        // If the compaction file path is null that means we have no space left for this compaction.
	        if( compactionFileLocation == null )
	        {
        		String maxFile = getMaxSizeFile( files );
        		files.remove( maxFile );
        		doFileCompaction(files , minBufferSize);
        		return;
	        }
	        PriorityQueue<FileStruct> pq = initializePriorityQueue(files, null, minBufferSize);
	        if (pq.size() > 0)
	        {
	            String mergedFileName = getTempFileName( files );
	            SSTable ssTable = null;
	            String lastkey = null;
	            List<FileStruct> lfs = new ArrayList<FileStruct>();
	            DataOutputBuffer bufOut = new DataOutputBuffer();
	            int expectedBloomFilterSize = SSTable.getApproximateKeyCount(files);
	            expectedBloomFilterSize = (expectedBloomFilterSize > 0) ? expectedBloomFilterSize : SSTable.indexInterval();
	            logger_.debug("Expected bloom filter size : " + expectedBloomFilterSize);
	            /* Create the bloom filter for the compacted file. */
	            BloomFilter compactedBloomFilter = new BloomFilter(expectedBloomFilterSize, 15);
	            List<ColumnFamily> columnFamilies = new ArrayList<ColumnFamily>();

	            while (pq.size() > 0 || lfs.size() > 0)
	            {
	                FileStruct fs = null;
	                if (pq.size() > 0)
	                {
	                    fs = pq.poll();                        
	                }
	                if (fs != null
	                        && (lastkey == null || lastkey.compareTo(fs.key_) == 0))
	                {
	                    // The keys are the same so we need to add this to the
	                    // ldfs list
	                    lastkey = fs.key_;
	                    lfs.add(fs);
	                }
	                else
	                {
	                    Collections.sort(lfs, new FileStructComparator());
	                    ColumnFamily columnFamily = null;
	                    bufOut.reset();
	                    if(lfs.size() > 1)
	                    {
		                    for (FileStruct filestruct : lfs)
		                    {
		                    	try
		                    	{
	                                /* read the length although we don't need it */
	                                filestruct.bufIn_.readInt();
	                                // Skip the Index
                                    IndexHelper.skipBloomFilterAndIndex(filestruct.bufIn_);
	                                // We want to add only 2 and resolve them right there in order to save on memory footprint
	                                if(columnFamilies.size() > 1)
	                                {
	    		                        merge(columnFamilies);
	                                }
			                        // deserialize into column families                                    
			                        columnFamilies.add(ColumnFamily.serializer().deserialize(filestruct.bufIn_));
		                    	}
		                    	catch ( Exception ex)
		                    	{
                                    logger_.warn("error in filecompaction", ex);
                                }
		                    }
		                    // Now after merging all crap append to the sstable
		                    columnFamily = resolveAndRemoveDeleted(columnFamilies);
		                    columnFamilies.clear();
		                    if( columnFamily != null )
		                    {
			                	/* serialize the cf with column indexes */
			                    ColumnFamily.serializerWithIndexes().serialize(columnFamily, bufOut);
		                    }
	                    }
	                    else
	                    {
		                    FileStruct filestruct = lfs.get(0);
	                    	try
	                    	{
		                        /* read the length although we don't need it */
		                        int size = filestruct.bufIn_.readInt();
		                        bufOut.write(filestruct.bufIn_, size);
	                    	}
	                    	catch ( Exception ex)
	                    	{
	                    		ex.printStackTrace();
	                            filestruct.reader_.close();
	                            continue;
	                    	}
	                    }
	                    	         
	                    if ( ssTable == null )
	                    {
	                    	ssTable = new SSTable(compactionFileLocation, mergedFileName);	                    	
	                    }
                        ssTable.append(lastkey, bufOut);

                        /* Fill the bloom filter with the key */
	                    doFill(compactedBloomFilter, lastkey);                        
	                    totalkeysWritten++;
	                    for (FileStruct filestruct : lfs)
	                    {
	                    	try
	                    	{
	                    		filestruct = getNextKey(filestruct);
	                    		if(filestruct == null)
	                    		{
	                    			continue;
	                    		}
	                    		pq.add(filestruct);
		                        totalkeysRead++;
	                    	}
	                    	catch ( Throwable ex )
	                    	{
	                    		// Ignore the exception as it might be a corrupted file
	                    		// in any case we have read as far as possible from it
	                    		// and it will be deleted after compaction.
	                            filestruct.reader_.close();
                            }
	                    }
	                    lfs.clear();
	                    lastkey = null;
	                    if (fs != null)
	                    {
	                        /* Add back the fs since we processed the rest of filestructs */
	                        pq.add(fs);
	                    }
	                }
	            }
	            if ( ssTable != null )
	            {
	                ssTable.closeRename(compactedBloomFilter);
	                newfile = ssTable.getDataFileLocation();
	            }
	            lock_.writeLock().lock();
	            try
	            {
	                for (String file : files)
	                {
	                    ssTables_.remove(file);
	                    SSTable.removeAssociatedBloomFilter(file);
	                }
	                if ( newfile != null )
	                {
	                    ssTables_.add(newfile);
	                    logger_.debug("Inserting bloom filter for file " + newfile);
	                    SSTable.storeBloomFilter(newfile, compactedBloomFilter);
	                    totalBytesWritten = (new File(newfile)).length();
	                }
	            }
	            finally
	            {
	                lock_.writeLock().unlock();
	            }
	            for (String file : files)
	            {
	                SSTable.delete(file);
	            }
	        }
        }
        catch ( Exception ex)
        {
            logger_.warn( LogUtil.throwableToString(ex) );
        }
        logger_.debug("Total time taken for compaction  ..."
                + (System.currentTimeMillis() - startTime));
        logger_.debug("Total bytes Read for compaction  ..." + totalBytesRead);
        logger_.debug("Total bytes written for compaction  ..."
                + totalBytesWritten + "   Total keys read ..." + totalkeysRead);
    }

    public boolean isSuper()
    {
        return DatabaseDescriptor.getColumnType(getColumnFamilyName()).equals("Super");
    }

    public void flushMemtableOnRecovery() throws IOException
    {
        memtable_.get().flushOnRecovery();
    }
}
