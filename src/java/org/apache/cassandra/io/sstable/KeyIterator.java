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
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.CloseableIterator;

public class KeyIterator extends AbstractIterator<DecoratedKey> implements CloseableIterator<DecoratedKey>
{
    private final IPartitioner partitioner;
    private final KeyReader it;
    private final ReadWriteLock fileAccessLock;
    private final long totalBytes;

    private boolean initialized = false;

    public KeyIterator(KeyReader it, IPartitioner partitioner, long totalBytes, ReadWriteLock fileAccessLock)
    {
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
            if (!initialized)
            {
                initialized = true;
                return it.isExhausted()
                       ? endOfData()
                       : partitioner.decorateKey(it.key());
            }
            else
            {
                return it.advance()
                       ? partitioner.decorateKey(it.key())
                       : endOfData();
            }
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
