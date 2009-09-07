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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.ArrayUtils;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.DataOutputBuffer;
import org.apache.cassandra.io.SSTableReader;
import org.apache.cassandra.io.SSTableWriter;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.DestructivePQIterator;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.marshal.AbstractType;

import org.apache.log4j.Logger;

public class Memtable implements Comparable<Memtable>
{
	private static Logger logger_ = Logger.getLogger( Memtable.class );

    private boolean isFrozen_;
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
    }

    public boolean isFlushed()
    {
        return isFlushed_;
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

    boolean isFrozen()
    {
        return isFrozen_;
    }

    void freeze()
    {
        isFrozen_ = true;
    }

    /**
     * Should only be called by ColumnFamilyStore.apply.  NOT a public API.
     * (CFS handles locking to avoid submitting an op
     *  to a flushing memtable.  Any other way is unsafe.)
    */
    void put(String key, ColumnFamily columnFamily)
    {
        assert !isFrozen_; // not 100% foolproof but hell, it's an assert
        isDirty_ = true;
        resolve(key, columnFamily);
    }

    /** flush synchronously (in the current thread, not on the executor).
     *  only the recover code should call this. */
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
            oldCf.delete(columnFamily);
        }
        else
        {
            columnFamilies_.put(key, columnFamily);
            currentSize_.addAndGet(columnFamily.size() + key.length());
            currentObjectCount_.addAndGet(columnFamily.getColumnCount());
        }
    }

    // for debugging
    public String contents()
    {
        StringBuilder builder = new StringBuilder();
        builder.append("{");
        for (Map.Entry<String, ColumnFamily> entry : columnFamilies_.entrySet())
        {
            builder.append(entry.getKey()).append(": ").append(entry.getValue()).append(", ");
        }
        builder.append("}");
        return builder.toString();
    }

    void flush(CommitLog.CommitLogContext cLogCtx) throws IOException
    {
        logger_.info("Flushing " + this);
        ColumnFamilyStore cfStore = Table.open(table_).getColumnFamilyStore(cfName_);

        SSTableWriter writer = new SSTableWriter(cfStore.getTempSSTablePath(), columnFamilies_.size(), StorageService.getPartitioner());

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
        for (String key : orderedKeys)
        {
            buffer.reset();
            ColumnFamily columnFamily = columnFamilies_.get(key);
            if (columnFamily != null)
            {
                /* serialize the cf with column indexes */
                ColumnFamily.serializer().serializeWithIndexes(columnFamily, buffer);
                /* Now write the key and value to disk */
                writer.append(partitioner.decorateKey(key), buffer);
            }
        }
        SSTableReader ssTable = writer.closeAndOpenReader();
        cfStore.onMemtableFlush(cLogCtx);
        cfStore.storeLocation(ssTable);
        buffer.close();
        isFlushed_ = true;
        logger_.info("Completed flushing " + this);
    }

    public String toString()
    {
        return "Memtable(" + cfName_ + ")@" + hashCode();
    }

    /**
     * there does not appear to be any data structure that we can pass to PriorityQueue that will
     * get it to heapify in-place instead of copying first, so we might as well return a Set.
    */
    Set<String> getKeys() throws ExecutionException, InterruptedException
    {
        return new HashSet<String>(columnFamilies_.keySet());
    }

    public static Iterator<String> getKeyIterator(Set<String> keys)
    {
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

    /**
     * obtain an iterator of columns in this memtable in the specified order starting from a given column.
     */
    public ColumnIterator getSliceIterator(SliceQueryFilter filter, AbstractType typeComparator)
    {
        ColumnFamily cf = columnFamilies_.get(filter.key);
        final ColumnFamily columnFamily = cf == null ? ColumnFamily.create(table_, filter.getColumnFamilyName()) : cf.cloneMeShallow();

        final IColumn columns[] = (cf == null ? columnFamily : cf).getSortedColumns().toArray(new IColumn[columnFamily.getSortedColumns().size()]);
        // TODO if we are dealing with supercolumns, we need to clone them while we have the read lock since they can be modified later
        if (filter.reversed)
            ArrayUtils.reverse(columns);
        IColumn startIColumn;
        if (DatabaseDescriptor.getColumnFamilyType(table_, filter.getColumnFamilyName()).equals("Standard"))
            startIColumn = new Column(filter.start);
        else
            startIColumn = new SuperColumn(filter.start, null); // ok to not have subcolumnComparator since we won't be adding columns to this object

        // can't use a ColumnComparatorFactory comparator since those compare on both name and time (and thus will fail to match
        // our dummy column, since the time there is arbitrary).
        Comparator<IColumn> comparator = filter.getColumnComparator(typeComparator);
        int index;
        if (filter.start.length == 0 && filter.reversed)
        {
            /* scan from the largest column in descending order */
            index = 0;
        }
        else
        {
            index = Arrays.binarySearch(columns, startIColumn, comparator);
        }
        final int startIndex = index < 0 ? -(index + 1) : index;

        return new AbstractColumnIterator()
        {
            private int curIndex_ = startIndex;

            public ColumnFamily getColumnFamily()
            {
                return columnFamily;
            }

            public boolean hasNext()
            {
                return curIndex_ < columns.length;
            }

            public IColumn next()
            {
                return columns[curIndex_++];
            }
        };
    }

    public ColumnIterator getNamesIterator(final NamesQueryFilter filter)
    {
        final ColumnFamily cf = columnFamilies_.get(filter.key);
        final ColumnFamily columnFamily = cf == null ? ColumnFamily.create(table_, filter.getColumnFamilyName()) : cf.cloneMeShallow();

        return new SimpleAbstractColumnIterator()
        {
            private Iterator<byte[]> iter = filter.columns.iterator();
            private byte[] current;

            public ColumnFamily getColumnFamily()
            {
                return columnFamily;
            }

            protected IColumn computeNext()
            {
                if (cf == null)
                {
                    return endOfData();
                }
                while (iter.hasNext())
                {
                    current = iter.next();
                    IColumn column = cf.getColumn(current);
                    if (column != null)
                        return column;
                }
                return endOfData();
            }
        };
    }
    
    void clearUnsafe()
    {
        columnFamilies_.clear();
    }
}
