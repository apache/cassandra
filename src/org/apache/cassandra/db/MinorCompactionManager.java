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
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.cassandra.concurrent.DebuggableScheduledThreadPoolExecutor;
import org.apache.cassandra.concurrent.ThreadFactoryImpl;
import org.apache.cassandra.db.HintedHandOffManager.HintedHandOff;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.net.EndPoint;
import org.apache.cassandra.service.IComponentShutdown;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.BloomFilter;
import org.apache.cassandra.utils.LogUtil;
import org.apache.log4j.Logger;
import org.apache.cassandra.concurrent.*;

/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

class MinorCompactionManager implements IComponentShutdown
{
    private static MinorCompactionManager instance_;
    private static Lock lock_ = new ReentrantLock();
    private static Logger logger_ = Logger.getLogger(MinorCompactionManager.class);
    final static long intervalInMins_ = 5;

    public static MinorCompactionManager instance()
    {
        if ( instance_ == null )
        {
            lock_.lock();
            try
            {
                if ( instance_ == null )
                    instance_ = new MinorCompactionManager();
            }
            finally
            {
                lock_.unlock();
            }
        }
        return instance_;
    }

    class FileCompactor implements Runnable
    {
        private ColumnFamilyStore columnFamilyStore_;

        FileCompactor(ColumnFamilyStore columnFamilyStore)
        {
        	columnFamilyStore_ = columnFamilyStore;
        }

        public void run()
        {
            try
            {
                logger_.debug("Started  compaction ..."+columnFamilyStore_.columnFamily_);
            	columnFamilyStore_.doCompaction();
                logger_.debug("Finished compaction ..."+columnFamilyStore_.columnFamily_);
            }
            catch (Throwable th)
            {
                logger_.error( LogUtil.throwableToString(th) );
            }
        }
    }

    class FileCompactor2 implements Callable<Boolean>
    {
        private ColumnFamilyStore columnFamilyStore_;
        private List<Range> ranges_;
        private EndPoint target_;
        private List<String> fileList_;

        FileCompactor2(ColumnFamilyStore columnFamilyStore, List<Range> ranges)
        {
            columnFamilyStore_ = columnFamilyStore;
            ranges_ = ranges;
        }
        
        FileCompactor2(ColumnFamilyStore columnFamilyStore, List<Range> ranges, EndPoint target,List<String> fileList)
        {
            columnFamilyStore_ = columnFamilyStore;
            ranges_ = ranges;
            target_ = target;
            fileList_ = fileList;
        }

        public Boolean call()
        {
        	boolean result = true;
            logger_.debug("Started  compaction ..."+columnFamilyStore_.columnFamily_);
            result = columnFamilyStore_.doAntiCompaction(ranges_, target_,fileList_);
            logger_.debug("Finished compaction ..."+columnFamilyStore_.columnFamily_);
            return result;
        }
    }

    class OnDemandCompactor implements Runnable
    {
        private ColumnFamilyStore columnFamilyStore_;
        private long skip_ = 0L;

        OnDemandCompactor(ColumnFamilyStore columnFamilyStore, long skip)
        {
            columnFamilyStore_ = columnFamilyStore;
            skip_ = skip;
        }

        public void run()
        {
            try
            {
                logger_.debug("Started  Major compaction ..."+columnFamilyStore_.columnFamily_);
                columnFamilyStore_.doMajorCompaction(skip_);
                logger_.debug("Finished Major compaction ..."+columnFamilyStore_.columnFamily_);
            }
            catch (IOException e)
            {
                logger_.debug( LogUtil.throwableToString(e) );
            }
            return ;
        }
    }

    class CleanupCompactor implements Runnable
    {
        private ColumnFamilyStore columnFamilyStore_;

        CleanupCompactor(ColumnFamilyStore columnFamilyStore)
        {
        	columnFamilyStore_ = columnFamilyStore;
        }

        public void run()
        {
            try
            {
                logger_.debug("Started  compaction ..."+columnFamilyStore_.columnFamily_);
            	columnFamilyStore_.doCleanupCompaction();
                logger_.debug("Finished compaction ..."+columnFamilyStore_.columnFamily_);
            }
            catch (Throwable th)
            {
                logger_.error( LogUtil.throwableToString(th) );
            }
        }
    }
    
    
    private ScheduledExecutorService compactor_ = new DebuggableScheduledThreadPoolExecutor(1, new ThreadFactoryImpl("MINOR-COMPACTION-POOL"));

    public MinorCompactionManager()
    {
    	StorageService.instance().registerComponentForShutdown(this);
	}

    public void shutdown()
    {
    	compactor_.shutdownNow();
    }

    public void submitPeriodicCompaction(ColumnFamilyStore columnFamilyStore)
    {        
    	compactor_.scheduleWithFixedDelay(new FileCompactor(columnFamilyStore), MinorCompactionManager.intervalInMins_,
    			MinorCompactionManager.intervalInMins_, TimeUnit.MINUTES);       
    }

    public void submit(ColumnFamilyStore columnFamilyStore)
    {
        compactor_.submit(new FileCompactor(columnFamilyStore));
    }
    
    public void submitCleanup(ColumnFamilyStore columnFamilyStore)
    {
        compactor_.submit(new CleanupCompactor(columnFamilyStore));
    }

    public Future<Boolean> submit(ColumnFamilyStore columnFamilyStore, List<Range> ranges, EndPoint target, List<String> fileList)
    {
        return compactor_.submit( new FileCompactor2(columnFamilyStore, ranges, target, fileList) );
    }

    public void  submitMajor(ColumnFamilyStore columnFamilyStore, long skip)
    {
        compactor_.submit( new OnDemandCompactor(columnFamilyStore, skip) );
    }
}
