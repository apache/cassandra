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

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.concurrent.ThreadFactoryImpl;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.DataOutputBuffer;
import org.apache.cassandra.io.SSTable;
import org.apache.cassandra.service.IComponentShutdown;
import org.apache.cassandra.service.PartitionerType;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.BloomFilter;
import org.apache.cassandra.utils.LogUtil;
import org.apache.log4j.Logger;
import org.apache.cassandra.io.*;
import org.apache.cassandra.utils.*;

/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public class Memtable implements MemtableMBean, Comparable<Memtable>
{
	private static Logger logger_ = Logger.getLogger( Memtable.class );
    private static Map<String, ExecutorService> apartments_ = new HashMap<String, ExecutorService>();
    public static final String flushKey_ = "FlushKey";
    
    public static void shutdown()
    {
    	Set<String> names = apartments_.keySet();
    	for (String name : names)
    	{
    		apartments_.get(name).shutdownNow();
    	}
    }

    private int threshold_ = DatabaseDescriptor.getMemtableSize()*1024*1024;
    private int thresholdCount_ = DatabaseDescriptor.getMemtableObjectCount()*1024*1024;
    private AtomicInteger currentSize_ = new AtomicInteger(0);
    private AtomicInteger currentObjectCount_ = new AtomicInteger(0);

    /* Table and ColumnFamily name are used to determine the ColumnFamilyStore */
    private String table_;
    private String cfName_;
    /* Creation time of this Memtable */
    private long creationTime_;
    private boolean isFrozen_ = false;
    private Map<String, ColumnFamily> columnFamilies_ = new HashMap<String, ColumnFamily>();
    /* Lock and Condition for notifying new clients about Memtable switches */
    Lock lock_ = new ReentrantLock();
    Condition condition_;

    Memtable(String table, String cfName) throws IOException
    {
        if ( apartments_.get(cfName) == null )
        {
            apartments_.put(cfName, new DebuggableThreadPoolExecutor( 1,
                    1,
                    Integer.MAX_VALUE,
                    TimeUnit.SECONDS,
                    new LinkedBlockingQueue<Runnable>(),
                    new ThreadFactoryImpl("FAST-MEMTABLE-POOL")
                    ));
        }

        condition_ = lock_.newCondition();
        table_ = table;
        cfName_ = cfName;
        creationTime_ = System.currentTimeMillis();
    }

    class Putter implements Runnable
    {
        private String key_;
        private ColumnFamily columnFamily_;

        Putter(String key, ColumnFamily cf)
        {
            key_ = key;
            columnFamily_ = cf;
        }

        public void run()
        {
        	resolve(key_, columnFamily_);
        }
    }

    class Getter implements Callable<ColumnFamily>
    {
        private String key_;
        private String columnFamilyName_;
        private IFilter filter_;

        Getter(String key, String cfName)
        {
            key_ = key;
            columnFamilyName_ = cfName;
        }
        
        Getter(String key, String cfName, IFilter filter)
        {
            this(key, cfName);
            filter_ = filter;
        }

        public ColumnFamily call()
        {
        	ColumnFamily cf = getLocalCopy(key_, columnFamilyName_, filter_);            
            return cf;
        }
    }

    class Remover implements Runnable
    {
        private String key_;
        private ColumnFamily columnFamily_;

        Remover(String key, ColumnFamily columnFamily)
        {
            key_ = key;
            columnFamily_ = columnFamily;
        }

        public void run()
        {
        	columnFamily_.delete();
            columnFamilies_.put(key_, columnFamily_);
        }
    }
    
    /**
     * Flushes the current memtable to disk.
     * 
     * @author alakshman
     *
     */
    class Flusher implements Runnable
    {
        private CommitLog.CommitLogContext cLogCtx_;
        
        Flusher(CommitLog.CommitLogContext cLogCtx)
        {
            cLogCtx_ = cLogCtx;
        }
        
        public void run()
        {
            ColumnFamilyStore cfStore = Table.open(table_).getColumnFamilyStore(cfName_);
            MemtableManager.instance().submit(cfName_, Memtable.this, cLogCtx_);
        }
    }

    /**
     * Compares two Memtable based on creation time. 
     * @param rhs
     * @return
     */
    public int compareTo(Memtable rhs)
    {
    	long diff = creationTime_ - rhs.creationTime_;
    	if ( diff > 0 )
    		return 1;
    	else if ( diff < 0 )
    		return -1;
    	else
    		return 0;
    }

    public int getMemtableThreshold()
    {
        return currentSize_.get();
    }

    void resolveSize(int oldSize, int newSize)
    {
        currentSize_.addAndGet(newSize - oldSize);
    }

    void resolveCount(int oldCount, int newCount)
    {
        currentObjectCount_.addAndGet(newCount - oldCount);
    }

    private boolean isLifetimeViolated()
    {
      /* Memtable lifetime in terms of milliseconds */
      long lifetimeInMillis = DatabaseDescriptor.getMemtableLifetime() * 3600 * 1000;
      return ( ( System.currentTimeMillis() - creationTime_ ) >= lifetimeInMillis );
    }

    boolean isThresholdViolated(String key)
    {
    	boolean bVal = false;//isLifetimeViolated();
        if (currentSize_.get() >= threshold_ ||  currentObjectCount_.get() >= thresholdCount_ || bVal || key.equals(flushKey_))
        {
        	if ( bVal )
        		logger_.info("Memtable's lifetime for " + cfName_ + " has been violated.");
        	return true;
        }
        return false;
    }

    String getColumnFamily()
    {
    	return cfName_;
    }

    void printExecutorStats()
    {
    	DebuggableThreadPoolExecutor es = (DebuggableThreadPoolExecutor)apartments_.get(cfName_);
    	long taskCount = (es.getTaskCount() - es.getCompletedTaskCount());
    	logger_.debug("MEMTABLE TASKS : " + taskCount);
    }

    /*
     * This version is used by the external clients to put data into
     * the memtable. This version will respect the threshold and flush
     * the memtable to disk when the size exceeds the threshold.
    */
    void put(String key, ColumnFamily columnFamily, CommitLog.CommitLogContext cLogCtx) throws IOException
    {
        if (isThresholdViolated(key) )
        {
            lock_.lock();
            try
            {
                ColumnFamilyStore cfStore = Table.open(table_).getColumnFamilyStore(cfName_);
                if (!isFrozen_)
                {
                    isFrozen_ = true;
                    /* Submit this Memtable to be flushed. */
                    Runnable flusher = new Flusher(cLogCtx);
                    apartments_.get(cfName_).submit(flusher);
                    // MemtableManager.instance().submit(cfStore.getColumnFamilyName(), this, cLogCtx);
                    cfStore.switchMemtable(key, columnFamily, cLogCtx);
                }
                else
                {
                    cfStore.apply(key, columnFamily, cLogCtx);
                }
            }
            finally
            {
                lock_.unlock();
            }
        }
        else
        {
        	printExecutorStats();
        	Runnable putter = new Putter(key, columnFamily);
        	apartments_.get(cfName_).submit(putter);
        }
    }

    /*
     * This version is used to switch memtable and force flush.
    */
    void forceflush(ColumnFamilyStore cfStore, boolean fRecovery) throws IOException
    {
        if(!fRecovery)
        {
	    	RowMutation rm = new RowMutation(DatabaseDescriptor.getTables().get(0), flushKey_);
	        try
	        {
	            rm.add(cfStore.columnFamily_ + ":Column","0".getBytes());
	            rm.apply();
	        }
	        catch(ColumnFamilyNotDefinedException ex)
	        {
	            logger_.debug(LogUtil.throwableToString(ex));
	        }
        }
        else
        {
        	flush(CommitLog.CommitLogContext.NULL);
        }
    }

    private void resolve(String key, ColumnFamily columnFamily)
    {
    	ColumnFamily oldCf = columnFamilies_.get(key);
        if ( oldCf != null )
        {
            int oldSize = oldCf.size();
            int oldObjectCount = oldCf.getColumnCount();
            oldCf.addColumns(columnFamily);
            int newSize = oldCf.size();
            int newObjectCount = oldCf.getColumnCount();
            resolveSize(oldSize, newSize);
            resolveCount(oldObjectCount, newObjectCount);
        }
        else
        {
            columnFamilies_.put(key, columnFamily);
            currentSize_.addAndGet(columnFamily.size() + key.length());
            currentObjectCount_.addAndGet(columnFamily.getColumnCount());
        }
    }

    /*
     * This version is called on commit log recovery. The threshold
     * is not respected and a forceFlush() needs to be invoked to flush
     * the contents to disk.
    */
    void putOnRecovery(String key, ColumnFamily columnFamily) throws IOException
    {
        if(!key.equals(Memtable.flushKey_))
        	resolve(key, columnFamily);
    }

    ColumnFamily getLocalCopy(String key, String cfName, IFilter filter)
    {
    	String[] values = RowMutation.getColumnAndColumnFamily(cfName);
    	ColumnFamily columnFamily = null;
        if(values.length == 1 )
        {
        	columnFamily = columnFamilies_.get(key);        	
        }
        else
        {
        	ColumnFamily cFamily = columnFamilies_.get(key);
        	if(cFamily == null)
        		return null;
        	IColumn column = null;
        	if(values.length == 2)
        	{
        		column = cFamily.getColumn(values[1]);
        		if(column != null )
        		{
        			columnFamily = new ColumnFamily(cfName_);
        			columnFamily.addColumn(column.name(), column);
        		}
        	}
        	else
        	{
        		column = cFamily.getColumn(values[1]);
        		if(column != null )
        		{
        			 
        			IColumn subColumn = ((SuperColumn)column).getSubColumn(values[2]);
        			if(subColumn != null)
        			{
	        			columnFamily = new ColumnFamily(cfName_);
	            		columnFamily.createColumn(values[1] + ":" + values[2], subColumn.value(), subColumn.timestamp());
        			}
        		}
        	}
        }
        /* Filter unnecessary data from the column based on the provided filter */
        return filter.filter(cfName, columnFamily);
    }

    ColumnFamily get(String key, String cfName)
    {
    	printExecutorStats();
    	Callable<ColumnFamily> call = new Getter(key, cfName);
    	ColumnFamily cf = null;
    	try
    	{
    		cf = apartments_.get(cfName_).submit(call).get();
    	}
    	catch ( ExecutionException ex )
    	{
    		logger_.debug(LogUtil.throwableToString(ex));
    	}
    	catch ( InterruptedException ex2 )
    	{
    		logger_.debug(LogUtil.throwableToString(ex2));
    	}
    	return cf;
    }
    
    ColumnFamily get(String key, String cfName, IFilter filter)
    {
    	printExecutorStats();
    	Callable<ColumnFamily> call = new Getter(key, cfName, filter);
    	ColumnFamily cf = null;
    	try
    	{
    		cf = apartments_.get(cfName_).submit(call).get();
    	}
    	catch ( ExecutionException ex )
    	{
    		logger_.debug(LogUtil.throwableToString(ex));
    	}
    	catch ( InterruptedException ex2 )
    	{
    		logger_.debug(LogUtil.throwableToString(ex2));
    	}
    	return cf;
    }

    /*
     * Although the method is named remove() we cannot remove the key
     * from memtable. We add it to the memtable but mark it as deleted.
     * The reason for this because we do not want a successive get()
     * for the same key to scan the ColumnFamilyStore files for this key.
    */
    void remove(String key, ColumnFamily columnFamily) throws IOException
    {
    	printExecutorStats();
    	Runnable deleter = new Remover(key, columnFamily);
    	apartments_.get(cfName_).submit(deleter);
    }

    /*
     * param recoveryMode - indicates if this was invoked during
     *                      recovery.
    */
    void flush(CommitLog.CommitLogContext cLogCtx) throws IOException
    {
        ColumnFamilyStore cfStore = Table.open(table_).getColumnFamilyStore(cfName_);
        if ( columnFamilies_.size() == 0 )
        {
        	// This should be called even if size is 0
        	// This is because we should try to delete the useless commitlogs
        	// even though there is nothing to flush in memtables for a given family like Hints etc.
            cfStore.onMemtableFlush(cLogCtx);
            return;
        }

        PartitionerType pType = StorageService.getPartitionerType();
        String directory = DatabaseDescriptor.getDataFileLocation();
        String filename = cfStore.getNextFileName();
        SSTable ssTable = new SSTable(directory, filename, pType);
        switch (pType) 
        {
            case OPHF:
                flushForOrderPreservingPartitioner(ssTable, cfStore, cLogCtx);
                break;
                
            default:
                flushForRandomPartitioner(ssTable, cfStore, cLogCtx);
                break;
        }
        
        columnFamilies_.clear();        
    }
    
    private void flushForRandomPartitioner(SSTable ssTable, ColumnFamilyStore cfStore, CommitLog.CommitLogContext cLogCtx) throws IOException
    {
        /* List of primary keys in sorted order */
        List<PrimaryKey> pKeys = PrimaryKey.create( columnFamilies_.keySet() );
        DataOutputBuffer buffer = new DataOutputBuffer();
        /* Use this BloomFilter to decide if a key exists in a SSTable */
        BloomFilter bf = new BloomFilter(pKeys.size(), 15);
        for ( PrimaryKey pKey : pKeys )
        {
            buffer.reset();
            ColumnFamily columnFamily = columnFamilies_.get(pKey.key());
            if ( columnFamily != null )
            {
                /* serialize the cf with column indexes */
                ColumnFamily.serializer2().serialize( columnFamily, buffer );                                
                /* Now write the key and value to disk */
                ssTable.append(pKey.key(), pKey.hash(), buffer);
                bf.fill(pKey.key());
                columnFamily.clear();
            }
        }
        ssTable.close(bf);
        cfStore.onMemtableFlush(cLogCtx);
        cfStore.storeLocation( ssTable.getDataFileLocation(), bf );
        buffer.close();
    }
    
    private void flushForOrderPreservingPartitioner(SSTable ssTable, ColumnFamilyStore cfStore, CommitLog.CommitLogContext cLogCtx) throws IOException
    {
        List<String> keys = new ArrayList<String>( columnFamilies_.keySet() );
        Collections.sort(keys);
        DataOutputBuffer buffer = new DataOutputBuffer();
        /* Use this BloomFilter to decide if a key exists in a SSTable */
        BloomFilter bf = new BloomFilter(keys.size(), 15);
        for ( String key : keys )
        {
            buffer.reset();
            ColumnFamily columnFamily = columnFamilies_.get(key);
            if ( columnFamily != null )
            {
                /* serialize the cf with column indexes */
                ColumnFamily.serializer2().serialize( columnFamily, buffer );                                              
                /* Now write the key and value to disk */
                ssTable.append(key, buffer);
                bf.fill(key);
                columnFamily.clear();
            }
        }
        ssTable.close(bf);
        cfStore.onMemtableFlush(cLogCtx);
        cfStore.storeLocation( ssTable.getDataFileLocation(), bf );
        buffer.close();
    }
}
