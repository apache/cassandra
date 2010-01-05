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

    static abstract class Compactor<T> implements Callable<T>
    {
        protected final ColumnFamilyStore cfstore;
        public Compactor(ColumnFamilyStore columnFamilyStore)
        {
            cfstore = columnFamilyStore;
        }

        abstract T compact() throws IOException;
        
        public T call()
        {
        	T results;
            if (logger_.isDebugEnabled())
                logger_.debug("Starting " + this + ".");
            try
            {
                results = compact();
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
            if (logger_.isDebugEnabled())
                logger_.debug("Finished " + this + ".");
            return results;
        }

        @Override
        public String toString()
        {
            StringBuilder buff = new StringBuilder();
            buff.append("<").append(getClass().getSimpleName());
            buff.append(" for ").append(cfstore).append(">");
            return buff.toString();
        }
    }

    static class AntiCompactor extends Compactor<List<SSTableReader>>
    {
        private final Collection<Range> ranges;
        private final InetAddress target;
        AntiCompactor(ColumnFamilyStore cfstore, Collection<Range> ranges, InetAddress target)
        {
            super(cfstore);
            this.ranges = ranges;
            this.target = target;
        }

        public List<SSTableReader> compact() throws IOException
        {
            return cfstore.doAntiCompaction(ranges, target);
        }
    }

    static class OnDemandCompactor extends Compactor<Object>
    {
        private final long skip;
        OnDemandCompactor(ColumnFamilyStore cfstore, long skip)
        {
            super(cfstore);
            this.skip = skip;
        }

        public Object compact() throws IOException
        {
            cfstore.doMajorCompaction(skip);
            return this;
        }
    }

    static class CleanupCompactor extends Compactor<Object>
    {
        CleanupCompactor(ColumnFamilyStore cfstore)
        {
            super(cfstore);
        }

        public Object compact() throws IOException
        {
            cfstore.doCleanupCompaction();
            return this;
        }
    }
    
    static class MinorCompactor extends Compactor<Integer>
    {
        private final int minimum;
        private final int maximum;
        MinorCompactor(ColumnFamilyStore cfstore, int minimumThreshold, int maximumThreshold)
        {
            super(cfstore);
            minimum = minimumThreshold;
            maximum = maximumThreshold;
        }

        public Integer compact() throws IOException
        {
            return cfstore.doCompaction(minimum, maximum);
        }
    }

    static class ReadonlyCompactor extends Compactor<Object>
    {
        private final InetAddress initiator;
        ReadonlyCompactor(ColumnFamilyStore cfstore, InetAddress initiator)
        {
            super(cfstore);
            this.initiator = initiator;
        }

        public Object compact() throws IOException
        {
            cfstore.doReadonlyCompaction(initiator);
            return this;
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
        return compactor_.submit(new MinorCompactor(columnFamilyStore, minThreshold, maxThreshold));
    }

    public Future submitCleanup(ColumnFamilyStore columnFamilyStore)
    {
        return compactor_.submit(new CleanupCompactor(columnFamilyStore));
    }

    public Future<List<SSTableReader>> submitAnti(ColumnFamilyStore columnFamilyStore, Collection<Range> ranges, InetAddress target)
    {
        return compactor_.submit(new AntiCompactor(columnFamilyStore, ranges, target));
    }

    public Future submitMajor(ColumnFamilyStore columnFamilyStore, long skip)
    {
        return compactor_.submit(new OnDemandCompactor(columnFamilyStore, skip));
    }

    public Future submitReadonly(ColumnFamilyStore columnFamilyStore, InetAddress initiator)
    {
        return compactor_.submit(new ReadonlyCompactor(columnFamilyStore, initiator));
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
