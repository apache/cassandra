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
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.log4j.Logger;

import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.io.SSTableReader;
import java.net.InetAddress;

public class CompactionManager implements CompactionManagerMBean
{
    public static String MBEAN_OBJECT_NAME = "org.apache.cassandra.db:type=CompactionManager";
    private static CompactionManager instance_;
    private static Lock lock_ = new ReentrantLock();
    private static Logger logger_ = Logger.getLogger(CompactionManager.class);
    private int minimumCompactionThreshold_ = 4; // compact this many sstables min at a time
    private int maximumCompactionThreshold = 32; // compact this many sstables max at a time

    public static CompactionManager instance()
    {
        if ( instance_ == null )
        {
            lock_.lock();
            try
            {
                if ( instance_ == null )
                {
                    instance_ = new CompactionManager();
                    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
                    mbs.registerMBean(instance_, new ObjectName(MBEAN_OBJECT_NAME));
                }
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
            finally
            {
                lock_.unlock();
            }
        }
        return instance_;
    }

    static class FileCompactor2 implements Callable<List<SSTableReader>>
    {
        private ColumnFamilyStore columnFamilyStore_;
        private Collection<Range> ranges_;
        private InetAddress target_;

        FileCompactor2(ColumnFamilyStore columnFamilyStore, Collection<Range> ranges, InetAddress target)
        {
            columnFamilyStore_ = columnFamilyStore;
            ranges_ = ranges;
            target_ = target;
        }

        public List<SSTableReader> call()
        {
        	List<SSTableReader> results;
            if (logger_.isDebugEnabled())
              logger_.debug("Started  compaction ..."+columnFamilyStore_.columnFamily_);
            try
            {
                results = columnFamilyStore_.doAntiCompaction(ranges_, target_);
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
            if (logger_.isDebugEnabled())
              logger_.debug("Finished compaction ..."+columnFamilyStore_.columnFamily_);
            return results;
        }
    }

    static class OnDemandCompactor implements Runnable
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

    static class CleanupCompactor implements Runnable
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
    
    
    private ExecutorService compactor_ = new DebuggableThreadPoolExecutor("COMPACTION-POOL");

    /**
     * Call this whenever a compaction might be needed on the given columnfamily.
     * It's okay to over-call (within reason) since the compactions are single-threaded,
     * and if a call is unnecessary, it will just be no-oped in the bucketing phase.
     */
    public Future<Integer> submit(final ColumnFamilyStore columnFamilyStore)
    {
        return submit(columnFamilyStore, minimumCompactionThreshold_, maximumCompactionThreshold);
    }

    Future<Integer> submit(final ColumnFamilyStore columnFamilyStore, final int minThreshold, final int maxThreshold)
    {
        Callable<Integer> callable = new Callable<Integer>()
        {
            public Integer call() throws IOException
            {
                return columnFamilyStore.doCompaction(minThreshold, maxThreshold);
            }
        };
        return compactor_.submit(callable);
    }

    public void submitCleanup(ColumnFamilyStore columnFamilyStore)
    {
        compactor_.submit(new CleanupCompactor(columnFamilyStore));
    }

    public Future<List<SSTableReader>> submit(ColumnFamilyStore columnFamilyStore, Collection<Range> ranges, InetAddress target)
    {
        return compactor_.submit( new FileCompactor2(columnFamilyStore, ranges, target) );
    }

    public void  submitMajor(ColumnFamilyStore columnFamilyStore, long skip)
    {
        compactor_.submit( new OnDemandCompactor(columnFamilyStore, skip) );
    }

    /**
     * Gets the minimum number of sstables in queue before compaction kicks off
     */
    public int getMinimumCompactionThreshold()
    {
        return minimumCompactionThreshold_;
    }

    /**
     * Sets the minimum number of sstables in queue before compaction kicks off
     */
    public void setMinimumCompactionThreshold(int threshold)
    {
        minimumCompactionThreshold_ = threshold;
    }

    /**
     * Gets the maximum number of sstables in queue before compaction kicks off
     */
    public int getMaximumCompactionThreshold()
    {
        return maximumCompactionThreshold;
    }

    /**
     * Sets the maximum number of sstables in queue before compaction kicks off
     */
    public void setMaximumCompactionThreshold(int threshold)
    {
        maximumCompactionThreshold = threshold;
    }

    public void disableCompactions()
    {
        minimumCompactionThreshold_ = 0;
        maximumCompactionThreshold = 0;
    }
}
