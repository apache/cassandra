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
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.net.EndPoint;
import org.apache.cassandra.service.StorageService;
import org.apache.log4j.Logger;

/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

class MinorCompactionManager
{
    private static MinorCompactionManager instance_;
    private static Lock lock_ = new ReentrantLock();
    private static Logger logger_ = Logger.getLogger(MinorCompactionManager.class);
    private static final long intervalInMins_ = 5;
    static final int COMPACTION_THRESHOLD = 4; // compact this many sstables at a time

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

    class FileCompactor2 implements Callable<Boolean>
    {
        private ColumnFamilyStore columnFamilyStore_;
        private List<Range> ranges_;
        private EndPoint target_;
        private List<String> fileList_;

        FileCompactor2(ColumnFamilyStore columnFamilyStore, List<Range> ranges, EndPoint target,List<String> fileList)
        {
            columnFamilyStore_ = columnFamilyStore;
            ranges_ = ranges;
            target_ = target;
            fileList_ = fileList;
        }

        public Boolean call()
        {
        	boolean result;
            if (logger_.isDebugEnabled())
              logger_.debug("Started  compaction ..."+columnFamilyStore_.columnFamily_);
            try
            {
                result = columnFamilyStore_.doAntiCompaction(ranges_, target_,fileList_);
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
            if (logger_.isDebugEnabled())
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
            if (logger_.isDebugEnabled())
              logger_.debug("Started  Major compaction for " + columnFamilyStore_.columnFamily_);
            try
            {
                columnFamilyStore_.doMajorCompaction(skip_);
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
            if (logger_.isDebugEnabled())
              logger_.debug("Finished Major compaction for " + columnFamilyStore_.columnFamily_);
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
            if (logger_.isDebugEnabled())
              logger_.debug("Started  compaction ..."+columnFamilyStore_.columnFamily_);
            try
            {
                columnFamilyStore_.doCleanupCompaction();
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
            if (logger_.isDebugEnabled())
              logger_.debug("Finished compaction ..."+columnFamilyStore_.columnFamily_);
        }
    }
    
    
    private ScheduledExecutorService compactor_ = new DebuggableScheduledThreadPoolExecutor(1, new ThreadFactoryImpl("MINOR-COMPACTION-POOL"));

    public Future<Integer> submit(final ColumnFamilyStore columnFamilyStore)
    {
        return submit(columnFamilyStore, COMPACTION_THRESHOLD);
    }

    Future<Integer> submit(final ColumnFamilyStore columnFamilyStore, final int threshold)
    {
        Callable<Integer> callable = new Callable<Integer>()
        {
            public Integer call() throws IOException
            {
                return columnFamilyStore.doCompaction(threshold);
            }
        };
        return compactor_.submit(callable);
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
