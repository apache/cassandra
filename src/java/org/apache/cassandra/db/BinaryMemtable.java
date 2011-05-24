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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.ReplayPosition;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.sstable.SSTableWriter;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.WrappedRunnable;
import org.cliffc.high_scale_lib.NonBlockingHashMap;

public class BinaryMemtable implements IFlushable
{
    private static final Logger logger = LoggerFactory.getLogger(BinaryMemtable.class);
    private final int threshold = DatabaseDescriptor.getBMTThreshold() * 1024 * 1024;
    private final AtomicInteger currentSize = new AtomicInteger(0);

    /* Table and ColumnFamily name are used to determine the ColumnFamilyStore */
    private boolean isFrozen = false;
    private final Map<DecoratedKey, ByteBuffer> columnFamilies = new NonBlockingHashMap<DecoratedKey, ByteBuffer>();
    /* Lock and Condition for notifying new clients about Memtable switches */
    private final Lock lock = new ReentrantLock();
    Condition condition;
    private final IPartitioner partitioner = StorageService.getPartitioner();
    private final ColumnFamilyStore cfs;

    public BinaryMemtable(ColumnFamilyStore cfs)
    {
        this.cfs = cfs;
        condition = lock.newCondition();
    }

    boolean isThresholdViolated()
    {
        return currentSize.get() >= threshold;
    }

    /*
     * This version is used by the external clients to put data into
     * the memtable. This version will respect the threshold and flush
     * the memtable to disk when the size exceeds the threshold.
    */
    void put(DecoratedKey key, ByteBuffer buffer)
    {
        if (isThresholdViolated())
        {
            lock.lock();
            try
            {
                if (!isFrozen)
                {
                    isFrozen = true;
                    cfs.submitFlush(this, new CountDownLatch(1), null);
                    cfs.switchBinaryMemtable(key, buffer);
                }
                else
                {
                    cfs.applyBinary(key, buffer);
                }
            }
            finally
            {
                lock.unlock();
            }
        }
        else
        {
            resolve(key, buffer);
        }
    }

    public boolean isClean()
    {
        return columnFamilies.isEmpty();
    }

    private void resolve(DecoratedKey key, ByteBuffer buffer)
    {
        columnFamilies.put(key, buffer);
        currentSize.addAndGet(buffer.remaining() + key.key.remaining());
    }

    private List<DecoratedKey> getSortedKeys()
    {
        assert !columnFamilies.isEmpty();
        logger.info("Sorting " + this);
        List<DecoratedKey> keys = new ArrayList<DecoratedKey>(columnFamilies.keySet());
        Collections.sort(keys);
        return keys;
    }

    private SSTableReader writeSortedContents(List<DecoratedKey> sortedKeys, ReplayPosition context) throws IOException
    {
        logger.info("Writing " + this);
        SSTableWriter writer = cfs.createFlushWriter(sortedKeys.size(), DatabaseDescriptor.getBMTThreshold(), context);

        for (DecoratedKey key : sortedKeys)
        {
            ByteBuffer bytes = columnFamilies.get(key);
            assert bytes.remaining() > 0;
            writer.append(key, bytes);
        }
        SSTableReader sstable = writer.closeAndOpenReader();
        logger.info("Completed flushing " + writer.getFilename());
        return sstable;
    }

    public void flushAndSignal(final CountDownLatch latch, ExecutorService sorter, final ExecutorService writer, final ReplayPosition context)
    {
        sorter.execute(new Runnable()
        {
            public void run()
            {
                final List<DecoratedKey> sortedKeys = getSortedKeys();
                writer.execute(new WrappedRunnable()
                {
                    public void runMayThrow() throws IOException
                    {
                        cfs.addSSTable(writeSortedContents(sortedKeys, context));
                        latch.countDown();
                    }
                });
            }
        });
    }
}
