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

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.concurrent.ThreadFactoryImpl;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.LogUtil;
import org.apache.log4j.Logger;
import org.apache.cassandra.utils.*;

/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public class MemtableManager
{
    private static MemtableManager instance_;
    private static Lock lock_ = new ReentrantLock();
    private static Logger logger_ = Logger.getLogger(MemtableManager.class);
    private ReentrantReadWriteLock rwLock_ = new ReentrantReadWriteLock(true);
    static MemtableManager instance() 
    {
        if ( instance_ == null )
        {
            lock_.lock();
            try
            {
                if ( instance_ == null )
                    instance_ = new MemtableManager();
            }
            finally
            {
                lock_.unlock();
            }
        }
        return instance_;
    }
    
    class MemtableFlusher implements Runnable
    {
        private Memtable memtable_;
        private CommitLog.CommitLogContext cLogCtx_;
        
        MemtableFlusher(Memtable memtable, CommitLog.CommitLogContext cLogCtx)
        {
            memtable_ = memtable;
            cLogCtx_ = cLogCtx;
        }
        
        public void run()
        {
            try
            {
            	memtable_.flush(cLogCtx_);
            }
            catch (IOException e)
            {
                logger_.debug( LogUtil.throwableToString(e) );
            }
        	rwLock_.writeLock().lock();
            try
            {
            	List<Memtable> memtables = history_.get(memtable_.getColumnFamily());
                memtables.remove(memtable_);                	
            }
        	finally
        	{
            	rwLock_.writeLock().unlock();
        	}
        }
    }
    
    private Map<String, List<Memtable>> history_ = new HashMap<String, List<Memtable>>();
    private ExecutorService flusher_ = new DebuggableThreadPoolExecutor( 1,
            1,
            Integer.MAX_VALUE,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<Runnable>(),
            new ThreadFactoryImpl("MEMTABLE-FLUSHER-POOL")
            );  
    
    /* Submit memtables to be flushed to disk */
    void submit(String cfName, Memtable memtbl, CommitLog.CommitLogContext cLogCtx)
    {
    	rwLock_.writeLock().lock();
    	try
    	{
	        List<Memtable> memtables = history_.get(cfName);
	        if ( memtables == null )
	        {
	            memtables = new ArrayList<Memtable>();
	            history_.put(cfName, memtables);
	        }
	        memtables.add(memtbl);	        
	        flusher_.submit( new MemtableFlusher(memtbl, cLogCtx) );
    	}
    	finally
    	{
        	rwLock_.writeLock().unlock();
    	}
    }
    

    /*
     * Retrieve column family from the list of Memtables that have been
     * submitted for flush but have not yet been flushed.
     * It also filters out unneccesary columns based on the passed in filter.
    */
    void getColumnFamily(String key, String cfName, String cf, IFilter filter, List<ColumnFamily> columnFamilies)
    {
    	rwLock_.readLock().lock();
    	try
    	{
	        /* Get all memtables associated with this column family */
	        List<Memtable> memtables = history_.get(cfName);
	        if ( memtables != null )
	        {
		        Collections.sort(memtables);
	        	int size = memtables.size();
	            for ( int i = size - 1; i >= 0; --i  )
	            {
	                ColumnFamily columnFamily = memtables.get(i).getLocalCopy(key, cf, filter);
	                if ( columnFamily != null )
	                {
	                    columnFamilies.add(columnFamily);
	                    if( filter.isDone())
	                    	break;
	                }
	            }
	        }        
    	}
    	finally
    	{
        	rwLock_.readLock().unlock();
    	}
    }




}
