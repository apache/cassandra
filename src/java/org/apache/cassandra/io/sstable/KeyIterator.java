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
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.sstable.format.PartitionIndexIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.CloseableIterator;

public class KeyIterator extends AbstractIterator<DecoratedKey> implements CloseableIterator<DecoratedKey>
{
    private final IPartitioner partitioner;
    private final PartitionIndexIterator it;
    private final ReadWriteLock fileAccessLock;
    private final long indexLength;

    private long keyPosition = -1;

    public KeyIterator(PartitionIndexIterator it, IPartitioner partitioner, ReadWriteLock fileAccessLock)
    {
        this.it = it;
        this.partitioner = partitioner;
        this.fileAccessLock = fileAccessLock;
        this.indexLength = it.indexLength();
    }

    public KeyIterator(PartitionIndexIterator it, IPartitioner partitioner)
    {
        this(it, partitioner, null);
    }

    public static KeyIterator forSSTable(SSTableReader ssTableReader) throws IOException
    {
        return new KeyIterator(ssTableReader.allKeysIterator(), ssTableReader.getPartitioner(), new ReentrantReadWriteLock());
    }

    public static KeyIterator create(SSTableReader.Factory factory, Descriptor descriptor, TableMetadata metadata)
    {
        return new KeyIterator(factory.indexIterator(descriptor, metadata), metadata.partitioner, new ReentrantReadWriteLock());
    }

    protected DecoratedKey computeNext()
    {
        if (fileAccessLock != null)
            fileAccessLock.readLock().lock();
        try
        {
            if (keyPosition < 0)
            {
                keyPosition = 0;
                return it.isExhausted()
                       ? endOfData()
                       : partitioner.decorateKey(it.key());
            }
            else
            {
                keyPosition = it.indexPosition();
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
            return it.indexPosition();
        }
        finally
        {
            if (fileAccessLock != null)
                fileAccessLock.readLock().unlock();
        }
    }

    public long getTotalBytes()
    {
        return indexLength;
    }

    public long getKeyPosition()
    {
        return keyPosition;
    }

    public void reset()
    {
        if (fileAccessLock != null)
            fileAccessLock.readLock().lock();
        try
        {
            it.reset();
            keyPosition = -1;
        }
        catch (IOException ex)
        {
            throw new RuntimeException(ex);
        }
        finally
        {
            if (fileAccessLock != null)
                fileAccessLock.readLock().unlock();
        }
    }
}
