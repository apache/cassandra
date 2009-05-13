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

import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.DataOutputBuffer;
import org.apache.cassandra.io.SSTable;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.BloomFilter;
import org.apache.cassandra.utils.DestructivePQIterator;
import org.apache.log4j.Logger;
import org.cliffc.high_scale_lib.NonBlockingHashSet;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public class Memtable implements Comparable<Memtable>
{
	private static Logger logger_ = Logger.getLogger( Memtable.class );
    private static Set<ExecutorService> runningExecutorServices_ = new NonBlockingHashSet<ExecutorService>();
    private static AtomicInteger executorCount_ = new AtomicInteger(0);

    public static void shutdown()
    {
        for (ExecutorService exs : runningExecutorServices_)
        {
            exs.shutdownNow();
        }
    }

    private MemtableThreadPoolExecutor executor_;
    private volatile boolean isFrozen_;
    private volatile boolean isDirty_;
    private volatile boolean isFlushed_; // for tests, in particular forceBlockingFlush asserts this

    private int threshold_ = DatabaseDescriptor.getMemtableSize()*1024*1024;
    private int thresholdCount_ = (int)(DatabaseDescriptor.getMemtableObjectCount()*1024*1024);
    private AtomicInteger currentSize_ = new AtomicInteger(0);
    private AtomicInteger currentObjectCount_ = new AtomicInteger(0);

    /* Table and ColumnFamily name are used to determine the ColumnFamilyStore */
    private String table_;
    private String cfName_;
    /* Creation time of this Memtable */
    private long creationTime_;
    private Map<String, ColumnFamily> columnFamilies_ = new HashMap<String, ColumnFamily>();
    /* Lock and Condition for notifying new clients about Memtable switches */

    Memtable(String table, String cfName)
    {
        table_ = table;
        cfName_ = cfName;
        creationTime_ = System.currentTimeMillis();

        executor_ = new MemtableThreadPoolExecutor();
        runningExecutorServices_.add(executor_);
    }

    public boolean isFlushed()
    {
        return isFlushed_;
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

    boolean isThresholdViolated()
    {
        return currentSize_.get() >= threshold_ ||  currentObjectCount_.get() >= thresholdCount_;
    }

    String getColumnFamily()
    {
    	return cfName_;
    }

    private synchronized void enqueueFlush(CommitLog.CommitLogContext cLogCtx)
    {
        if (!isFrozen_)
        {
            isFrozen_ = true;
            ColumnFamilyStore cfStore = Table.open(table_).getColumnFamilyStore(cfName_);
            cfStore.switchMemtable();
            executor_.flushWhenTerminated(cLogCtx);
            executor_.shutdown();
        }
    }

    /*
     * This version is used by the external clients to put data into
     * the memtable. This version will respect the threshold and flush
     * the memtable to disk when the size exceeds the threshold.
    */
    public void put(String key, ColumnFamily columnFamily, CommitLog.CommitLogContext cLogCtx) throws IOException
    {
        if (isThresholdViolated())
        {
            enqueueFlush(cLogCtx);
            // retry the put on the new memtable
            ColumnFamilyStore cfStore = Table.open(table_).getColumnFamilyStore(cfName_);
            cfStore.apply(key, columnFamily, cLogCtx);
            return;
        }

        isDirty_ = true;
        executor_.submit(new Putter(key, columnFamily));
    }

    /*
     * This version is used to switch memtable and force flush.
     * Flushing is still done in a separate executor -- forceFlush only blocks
     * until the flush runnable is queued.
    */
    public void forceflush()
    {
        if (isClean())
            return;

        try
        {
            enqueueFlush(CommitLog.open(table_).getContext());
            executor_.flushQueuer.get();
        }
        catch (Exception ex)
        {
            throw new RuntimeException(ex);
        }
    }

    void flushOnRecovery() throws IOException {
        if (!isClean())
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
            oldCf.delete(Math.max(oldCf.getLocalDeletionTime(), columnFamily.getLocalDeletionTime()),
                         Math.max(oldCf.getMarkedForDeleteAt(), columnFamily.getMarkedForDeleteAt()));
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
                    columnFamily = cFamily.cloneMeShallow();
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
                        columnFamily = cFamily.cloneMeShallow();
                        SuperColumn container = new SuperColumn(superColumn.name());
                        container.markForDeleteAt(superColumn.getLocalDeletionTime(), superColumn.getMarkedForDeleteAt());
                        container.addColumn(subColumn);
                        columnFamily.addColumn(container);
                    }
                }
        	}
        }
        /* Filter unnecessary data from the column based on the provided filter */
        return filter.filter(columnFamilyColumn, columnFamily);
    }

    ColumnFamily get(String key, String cfName, IFilter filter)
    {
    	Callable<ColumnFamily> call = new Getter(key, cfName, filter);
    	ColumnFamily cf = null;
        try
        {
            cf = executor_.submit(call).get();
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
        catch (ExecutionException e)
        {
            throw new RuntimeException(e);
        }
        return cf;
    }

    void flush(CommitLog.CommitLogContext cLogCtx) throws IOException
    {
        ColumnFamilyStore cfStore = Table.open(table_).getColumnFamilyStore(cfName_);

        String directory = DatabaseDescriptor.getDataFileLocation();
        String filename = cfStore.getTempFileName();
        SSTable ssTable = new SSTable(directory, filename, StorageService.getPartitioner());

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
                bf.add(key);
            }
        }
        ssTable.closeRename(bf);
        cfStore.onMemtableFlush(cLogCtx);
        cfStore.storeLocation( ssTable.getDataFileLocation(), bf );
        buffer.close();
        isFlushed_ = true;
    }

    private class MemtableThreadPoolExecutor extends DebuggableThreadPoolExecutor
    {
        FutureTask flushQueuer;

        public MemtableThreadPoolExecutor()
        {
            super("MEMTABLE-POOL-" + cfName_ + executorCount_.addAndGet(1));
        }

        protected void terminated()
        {
            super.terminated();
            runningExecutorServices_.remove(this);
            if (flushQueuer != null)
            {
                flushQueuer.run();
            }
            this.unregisterMBean();
        }

        public void flushWhenTerminated(final CommitLog.CommitLogContext cLogCtx)
        {
            Runnable runnable = new Runnable()
            {
                public void run()
                {
                    ColumnFamilyStore.submitFlush(Memtable.this, cLogCtx);
                }
            };
            flushQueuer = new FutureTask(runnable, null);
        }
    }

    public Iterator<String> sortedKeyIterator() throws ExecutionException, InterruptedException
    {
        Callable<Set<String>> callable = new Callable<Set<String>>()
        {
            public Set<String> call() throws Exception
            {
                return columnFamilies_.keySet();
            }
        };
        Set<String> keys = executor_.submit(callable).get();
        if (keys.size() == 0)
        {
            // cannot create a PQ of size zero (wtf?)
            return Arrays.asList(new String[0]).iterator();
        }
        PriorityQueue<String> pq = new PriorityQueue<String>(keys.size(), StorageService.getPartitioner().getDecoratedKeyComparator());
        pq.addAll(keys);
        return new DestructivePQIterator<String>(pq);
    }

    public boolean isClean()
    {
        // executor taskcount is inadequate for our needs here -- it can return zero under certain
        // race conditions even though a task has been processed.
        return !isDirty_;
    }
}
