/*
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
package org.apache.cassandra.io.sstable;

import java.io.IOException;
import java.util.concurrent.locks.ReadWriteLock;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.CloseableIterator;

public class KeyIterator extends AbstractIterator<DecoratedKey> implements CloseableIterator<DecoratedKey>
{
    private final AbstractBounds<PartitionPosition> bounds;
    private final IPartitioner partitioner;
    private final KeyReader it;
    private final ReadWriteLock fileAccessLock;
    private final long totalBytes;

    private boolean initialized = false;

    public KeyIterator(AbstractBounds<PartitionPosition> bounds, KeyReader it, IPartitioner partitioner, long totalBytes, ReadWriteLock fileAccessLock)
    {
        this.bounds = bounds;
        this.it = it;
        this.partitioner = partitioner;
        this.totalBytes = totalBytes;
        this.fileAccessLock = fileAccessLock;
    }

    protected DecoratedKey computeNext()
    {
        if (fileAccessLock != null)
            fileAccessLock.readLock().lock();
        try
        {
            while (true)
            {
                if (!initialized)
                {
                    initialized = true;
                    if (it.isExhausted())
                        break;
                }
                else if (!it.advance())
                    break;

                DecoratedKey key = partitioner.decorateKey(it.key());
                if (bounds == null || bounds.contains(key))
                    return key;

                if (key.compareTo(bounds.right) >= 0)
                    break;
            }

            return endOfData();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        finally
        {
            if (fileAccessLock != null)
                fileAccessLock.readLock().unlock();
        }
    }

    public void close()
    {
        if (fileAccessLock != null)
            fileAccessLock.writeLock().lock();
        try
        {
            it.close();
        }
        finally
        {
            if (fileAccessLock != null)
                fileAccessLock.writeLock().unlock();
        }
    }

    public long getBytesRead()
    {
        if (fileAccessLock != null)
            fileAccessLock.readLock().lock();
        try
        {
            return it.isExhausted() ? totalBytes : it.dataPosition();
        }
        finally
        {
            if (fileAccessLock != null)
                fileAccessLock.readLock().unlock();
        }
    }

    public long getTotalBytes()
    {
        return totalBytes;
    }

}
