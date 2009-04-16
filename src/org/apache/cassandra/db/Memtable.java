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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Comparator;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.concurrent.ThreadFactoryImpl;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.DataOutputBuffer;
import org.apache.cassandra.io.SSTable;
import org.apache.cassandra.utils.BloomFilter;
import org.apache.cassandra.utils.LogUtil;
import org.apache.cassandra.service.IPartitioner;
import org.apache.cassandra.service.StorageService;
import org.cliffc.high_scale_lib.NonBlockingHashSet;

/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public class Memtable implements Comparable<Memtable>
{
	private static Logger logger_ = Logger.getLogger( Memtable.class );
    private static Set<ExecutorService> runningExecutorServices_ = new NonBlockingHashSet<ExecutorService>();
    public static final String flushKey_ = "FlushKey";

    public static void shutdown()
    {
        for (ExecutorService exs : runningExecutorServices_)
        {
            exs.shutdownNow();
        }
    }

    private MemtableThreadPoolExecutor executor_;

    private int threshold_ = DatabaseDescriptor.getMemtableSize()*1024*1024;
    private int thresholdCount_ = DatabaseDescriptor.getMemtableObjectCount()*1024*1024;
    private AtomicInteger currentSize_ = new AtomicInteger(0);
    private AtomicInteger currentObjectCount_ = new AtomicInteger(0);

    /* Table and ColumnFamily name are used to determine the ColumnFamilyStore */
    private String table_;
    private String cfName_;
    /* Creation time of this Memtable */
    private long creationTime_;
    private volatile boolean isFrozen_ = false;
    private Map<String, ColumnFamily> columnFamilies_ = new HashMap<String, ColumnFamily>();
    /* Lock and Condition for notifying new clients about Memtable switches */
    Lock lock_ = new ReentrantLock();

    Memtable(String table, String cfName)
    {
        executor_ = new MemtableThreadPoolExecutor();
        runningExecutorServices_.add(executor_);

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

    /**
     * Compares two Memtable based on creation time.
     * @param rhs Memtable to compare to.
     * @return a negative integer, zero, or a positive integer as this object
     * is less than, equal to, or greater than the specified object.
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

    public int getCurrentSize()
    {
        return currentSize_.get();
    }
    
    public int getCurrentObjectCount()
    {
        return currentObjectCount_.get();
    }

    void resolveSize(int oldSize, int newSize)
    {
        currentSize_.addAndGet(newSize - oldSize);
    }

    void resolveCount(int oldCount, int newCount)
    {
        currentObjectCount_.addAndGet(newCount - oldCount);
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
    	long taskCount = (executor_.getTaskCount() - executor_.getCompletedTaskCount());
    	logger_.debug("MEMTABLE TASKS : " + taskCount);
    }

    /*
     * This version is used by the external clients to put data into
     * the memtable. This version will respect the threshold and flush
     * the memtable to disk when the size exceeds the threshold.
    */
    void put(String key, ColumnFamily columnFamily, final CommitLog.CommitLogContext cLogCtx) throws IOException
    {
        if (isThresholdViolated(key) )
        {
            lock_.lock();
            try
            {
                final ColumnFamilyStore cfStore = Table.open(table_).getColumnFamilyStore(cfName_);
                if (!isFrozen_)
                {
                    isFrozen_ = true;
                    cfStore.switchMemtable(key, columnFamily, cLogCtx);
                    executor_.flushWhenTerminated(cLogCtx);
                    executor_.shutdown();
                }
                else
                {
                    // retry the put on the new memtable
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
        	executor_.submit(putter);
        }
    }

    /*
     * This version is used to switch memtable and force flush.
     * Flushing is still done in a separate executor -- forceFlush only blocks
     * until the flush runnable is queued.
    */
    public void forceflush(ColumnFamilyStore cfStore) throws IOException
    {
        RowMutation rm = new RowMutation(DatabaseDescriptor.getTables().get(0), flushKey_);

        try
        {
            if (cfStore.isSuper())
            {
                rm.add(cfStore.getColumnFamilyName() + ":SC1:Column", "0".getBytes(), 0);
            } else {
                rm.add(cfStore.getColumnFamilyName() + ":Column", "0".getBytes(), 0);
            }
            rm.apply();
            executor_.flushQueuer.get();
        }
        catch (Exception ex)
        {
            throw new RuntimeException(ex);
        }
    }

    void flushOnRecovery() throws IOException {
        flush(CommitLog.CommitLogContext.NULL);
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
            oldCf.delete(Math.max(oldCf.getMarkedForDeleteAt(), columnFamily.getMarkedForDeleteAt()));
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
    void putOnRecovery(String key, ColumnFamily columnFamily)
    {
        if(!key.equals(Memtable.flushKey_))
        	resolve(key, columnFamily);
    }

    ColumnFamily getLocalCopy(String key, String columnFamilyColumn, IFilter filter)
    {
    	String[] values = RowMutation.getColumnAndColumnFamily(columnFamilyColumn);
    	ColumnFamily columnFamily = null;
        if(values.length == 1 )
        {
        	columnFamily = columnFamilies_.get(key);
        }
        else
        {
        	ColumnFamily cFamily = columnFamilies_.get(key);
        	if (cFamily == null) return null;

        	if (values.length == 2) {
                IColumn column = cFamily.getColumn(values[1]); // super or normal column
                if (column != null )
                {
                    columnFamily = new ColumnFamily(cfName_);
                    columnFamily.addColumn(column);
                }
        	}
            else
            {
                assert values.length == 3;
                SuperColumn superColumn = (SuperColumn)cFamily.getColumn(values[1]);
                if (superColumn != null)
                {
                    IColumn subColumn = superColumn.getSubColumn(values[2]);
                    if (subColumn != null)
                    {
                        columnFamily = new ColumnFamily(cfName_);
                        columnFamily.addColumn(values[1] + ":" + values[2], subColumn.value(), subColumn.timestamp(), subColumn.isMarkedForDelete());
                    }
                }
        	}
        }
        /* Filter unnecessary data from the column based on the provided filter */
        return filter.filter(columnFamilyColumn, columnFamily);
    }

    ColumnFamily get(String key, String cfName, IFilter filter)
    {
    	printExecutorStats();
    	Callable<ColumnFamily> call = new Getter(key, cfName, filter);
    	ColumnFamily cf = null;
    	try
    	{
    		cf = executor_.submit(call).get();
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

        String directory = DatabaseDescriptor.getDataFileLocation();
        String filename = cfStore.getNextFileName();
        SSTable ssTable = new SSTable(directory, filename);

        // sort keys in the order they would be in when decorated
        final IPartitioner partitioner = StorageService.getPartitioner();
        final Comparator<String> dc = partitioner.getDecoratedKeyComparator();
        ArrayList<String> orderedKeys = new ArrayList<String>(columnFamilies_.keySet());
        Collections.sort(orderedKeys, new Comparator<String>()
        {
            public int compare(String o1, String o2)
            {
                return dc.compare(partitioner.decorateKey(o1), partitioner.decorateKey(o2));
            }
        });
        DataOutputBuffer buffer = new DataOutputBuffer();
        /* Use this BloomFilter to decide if a key exists in a SSTable */
        BloomFilter bf = new BloomFilter(columnFamilies_.size(), 15);
        for (String key : orderedKeys)
        {
            buffer.reset();
            ColumnFamily columnFamily = columnFamilies_.get(key);
            if ( columnFamily != null )
            {
                /* serialize the cf with column indexes */
                ColumnFamily.serializerWithIndexes().serialize( columnFamily, buffer );
                /* Now write the key and value to disk */
                ssTable.append(partitioner.decorateKey(key), buffer);
                bf.fill(key);
                columnFamily.clear();
            }
        }
        ssTable.close(bf);
        cfStore.onMemtableFlush(cLogCtx);
        cfStore.storeLocation( ssTable.getDataFileLocation(), bf );
        buffer.close();

        columnFamilies_.clear();
    }

    private class MemtableThreadPoolExecutor extends DebuggableThreadPoolExecutor
    {
        FutureTask flushQueuer;

        public MemtableThreadPoolExecutor()
        {
            super(1, 1, Integer.MAX_VALUE, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(), new ThreadFactoryImpl("FAST-MEMTABLE-POOL"));
        }

        protected void terminated()
        {
            super.terminated();
            runningExecutorServices_.remove(this);
            if (flushQueuer != null)
            {
                flushQueuer.run();
            }
        }

        public void flushWhenTerminated(final CommitLog.CommitLogContext cLogCtx)
        {
            Runnable runnable = new Runnable()
            {
                public void run()
                {
                    MemtableManager.instance().submit(cfName_, Memtable.this, cLogCtx);
                }
            };
            flushQueuer = new FutureTask(runnable, null);
        }
    }
}
