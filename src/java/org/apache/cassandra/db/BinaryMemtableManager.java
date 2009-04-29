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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.concurrent.ThreadFactoryImpl;
import org.apache.cassandra.utils.LogUtil;
import org.apache.log4j.Logger;


/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public class BinaryMemtableManager
{
    private static BinaryMemtableManager instance_;
    private static Lock lock_ = new ReentrantLock();
    private static Logger logger_ = Logger.getLogger(BinaryMemtableManager.class);    

    static BinaryMemtableManager instance() 
    {
        if ( instance_ == null )
        {
            lock_.lock();
            try
            {
                if ( instance_ == null )
                    instance_ = new BinaryMemtableManager();
            }
            finally
            {
                lock_.unlock();
            }
        }
        return instance_;
    }
    
    class BinaryMemtableFlusher implements Runnable
    {
        private BinaryMemtable memtable_;
        
        BinaryMemtableFlusher(BinaryMemtable memtable)
        {
            memtable_ = memtable;
        }
        
        public void run()
        {
            try
            {
            	memtable_.flush();
            }
            catch (IOException e)
            {
                logger_.debug( LogUtil.throwableToString(e) );
            }        	
        }
    }
    
    private ExecutorService flusher_ = new DebuggableThreadPoolExecutor( 1,
            1,
            Integer.MAX_VALUE,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<Runnable>(),
            new ThreadFactoryImpl("BINARY-MEMTABLE-FLUSHER-POOL")
            );  
    
    /* Submit memtables to be flushed to disk */
    void submit(String cfName, BinaryMemtable memtbl)
    {
    	flusher_.submit( new BinaryMemtableFlusher(memtbl) );
    }
}
