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
import org.apache.cassandra.db.RowIndexEntry;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CloseableIterator;

public class KeyIterator extends AbstractIterator<DecoratedKey> implements CloseableIterator<DecoratedKey>
{
    private final static class In
    {
        private final File path;
        private volatile RandomAccessReader in;

        public In(File path)
        {
            this.path = path;
        }

        private void maybeInit()
        {
            if (in != null)
                return;

            synchronized (this)
            {
                if (in == null)
                {
                    in = RandomAccessReader.open(path);
                }
            }
        }

        public DataInputPlus get()
        {
            maybeInit();
            return in;
        }

        public boolean isEOF()
        {
            maybeInit();
            return in.isEOF();
        }

        public void close()
        {
            if (in != null)
                in.close();
        }

        public long getFilePointer()
        {
            maybeInit();
            return in.getFilePointer();
        }

        public long length()
        {
            maybeInit();
            return in.length();
        }
    }

    private final Descriptor desc;
    private final In in;
    private final IPartitioner partitioner;
    private final ReadWriteLock fileAccessLock;

    private long keyPosition;

    public KeyIterator(Descriptor desc, TableMetadata metadata)
    {
        this.desc = desc;
        in = new In(new File(desc.filenameFor(Component.PRIMARY_INDEX)));
        partitioner = metadata.partitioner;
        fileAccessLock = new ReentrantReadWriteLock();
    }

    protected DecoratedKey computeNext()
    {
        fileAccessLock.readLock().lock();
        try
        {
            if (in.isEOF())
                return endOfData();

            keyPosition = in.getFilePointer();
            DecoratedKey key = partitioner.decorateKey(ByteBufferUtil.readWithShortLength(in.get()));
            RowIndexEntry.Serializer.skip(in.get(), desc.version); // skip remainder of the entry
            return key;
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        finally
        {
            fileAccessLock.readLock().unlock();
        }
    }

    public void close()
    {
        fileAccessLock.writeLock().lock();
        try
        {
            in.close();
        }
        finally
        {
            fileAccessLock.writeLock().unlock();
        }
    }

    public long getBytesRead()
    {
        fileAccessLock.readLock().lock();
        try
        {
            return in.getFilePointer();
        }
        finally
        {
            fileAccessLock.readLock().unlock();
        }
    }

    public long getTotalBytes()
    {
        // length is final in the referenced object.
        // no need to acquire the lock
        return in.length();
    }

    public long getKeyPosition()
    {
        return keyPosition;
    }
}
