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
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.cassandra.io.SSTableWriter;
import org.apache.cassandra.io.SSTableReader;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.config.DatabaseDescriptor;

import org.apache.log4j.Logger;
import org.cliffc.high_scale_lib.NonBlockingHashMap;
import org.apache.cassandra.dht.IPartitioner;

public class BinaryMemtable implements IFlushable<DecoratedKey>
{
    private static Logger logger_ = Logger.getLogger(BinaryMemtable.class);
    private int threshold_ = DatabaseDescriptor.getBMTThreshold() * 1024 * 1024;
    private AtomicInteger currentSize_ = new AtomicInteger(0);

    /* Table and ColumnFamily name are used to determine the ColumnFamilyStore */
    private String table_;
    private String cfName_;
    private boolean isFrozen_ = false;
    private Map<DecoratedKey, byte[]> columnFamilies_ = new NonBlockingHashMap<DecoratedKey, byte[]>();
    /* Lock and Condition for notifying new clients about Memtable switches */
    Lock lock_ = new ReentrantLock();
    Condition condition_;
    private final IPartitioner partitioner_ = StorageService.getPartitioner();

    BinaryMemtable(String table, String cfName) throws IOException
    {
        condition_ = lock_.newCondition();
        table_ = table;
        cfName_ = cfName;
    }

    public int getMemtableThreshold()
    {
        return currentSize_.get();
    }

    void resolveSize(int oldSize, int newSize)
    {
        currentSize_.addAndGet(newSize - oldSize);
    }


    boolean isThresholdViolated()
    {
        return currentSize_.get() >= threshold_;
    }

    String getColumnFamily()
    {
    	return cfName_;
    }

    /*
     * This version is used by the external clients to put data into
     * the memtable. This version will respect the threshold and flush
     * the memtable to disk when the size exceeds the threshold.
    */
    void put(String key, byte[] buffer) throws IOException
    {
        if (isThresholdViolated())
        {
            lock_.lock();
            try
            {
                ColumnFamilyStore cfStore = Table.open(table_).getColumnFamilyStore(cfName_);
                if (!isFrozen_)
                {
                    isFrozen_ = true;
                    cfStore.submitFlush(this);
                    cfStore.switchBinaryMemtable(key, buffer);
                }
                else
                {
                    cfStore.applyBinary(key, buffer);
                }
            }
            finally
            {
                lock_.unlock();
            }
        }
        else
        {
            resolve(key, buffer);
        }
    }

    public boolean isClean()
    {
        return columnFamilies_.isEmpty();
    }

    private void resolve(String key, byte[] buffer)
    {
        columnFamilies_.put(partitioner_.decorateKey(key), buffer);
        currentSize_.addAndGet(buffer.length + key.length());
    }

    public List<DecoratedKey> getSortedKeys()
    {
        assert !columnFamilies_.isEmpty();
        logger_.info("Sorting " + this);
        List<DecoratedKey> keys = new ArrayList<DecoratedKey>(columnFamilies_.keySet());
        Collections.sort(keys, partitioner_.getDecoratedKeyComparator());
        return keys;
    }

    public SSTableReader writeSortedContents(List<DecoratedKey> sortedKeys) throws IOException
    {
        logger_.info("Writing " + this);
        ColumnFamilyStore cfStore = Table.open(table_).getColumnFamilyStore(cfName_);
        String path = cfStore.getTempSSTablePath();
        SSTableWriter writer = new SSTableWriter(path, sortedKeys.size(), StorageService.getPartitioner());

        for (DecoratedKey key : sortedKeys)
        {
            byte[] bytes = columnFamilies_.get(key);
            assert bytes.length > 0;
            writer.append(key, bytes);
        }
        SSTableReader sstable = writer.closeAndOpenReader(DatabaseDescriptor.getKeysCachedFraction(table_));
        logger_.info("Completed flushing " + writer.getFilename());
        return sstable;
    }
}
