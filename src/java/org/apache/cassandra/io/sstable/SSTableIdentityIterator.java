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

import java.io.*;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.utils.ByteBufferUtil;

public class SSTableIdentityIterator implements Comparable<SSTableIdentityIterator>, UnfilteredRowIterator
{
    private final SSTableReader sstable;
    private final DecoratedKey key;
    private final DeletionTime partitionLevelDeletion;
    private final String filename;

    protected final SSTableSimpleIterator iterator;
    private final Row staticRow;

    public SSTableIdentityIterator(SSTableReader sstable, DecoratedKey key, DeletionTime partitionLevelDeletion,
            String filename, SSTableSimpleIterator iterator) throws IOException
    {
        super();
        this.sstable = sstable;
        this.key = key;
        this.partitionLevelDeletion = partitionLevelDeletion;
        this.filename = filename;
        this.iterator = iterator;
        this.staticRow = iterator.readStaticRow();
    }

    public static SSTableIdentityIterator create(SSTableReader sstable, RandomAccessReader file, DecoratedKey key)
    {
        try
        {
            DeletionTime partitionLevelDeletion = DeletionTime.serializer.deserialize(file);
            SerializationHelper helper = new SerializationHelper(sstable.metadata, sstable.descriptor.version.correspondingMessagingVersion(), SerializationHelper.Flag.LOCAL);
            SSTableSimpleIterator iterator = SSTableSimpleIterator.create(sstable.metadata, file, sstable.header, helper, partitionLevelDeletion);
            return new SSTableIdentityIterator(sstable, key, partitionLevelDeletion, file.getPath(), iterator);
        }
        catch (IOException e)
        {
            sstable.markSuspect();
            throw new CorruptSSTableException(e, file.getPath());
        }
    }

    public static SSTableIdentityIterator create(SSTableReader sstable, FileDataInput dfile, RowIndexEntry<?> indexEntry, DecoratedKey key, boolean tombstoneOnly)
    {
        try
        {
            dfile.seek(indexEntry.position);
            ByteBufferUtil.skipShortLength(dfile); // Skip partition key
            DeletionTime partitionLevelDeletion = DeletionTime.serializer.deserialize(dfile);
            SerializationHelper helper = new SerializationHelper(sstable.metadata, sstable.descriptor.version.correspondingMessagingVersion(), SerializationHelper.Flag.LOCAL);
            SSTableSimpleIterator iterator = tombstoneOnly
                    ? SSTableSimpleIterator.createTombstoneOnly(sstable.metadata, dfile, sstable.header, helper, partitionLevelDeletion)
                    : SSTableSimpleIterator.create(sstable.metadata, dfile, sstable.header, helper, partitionLevelDeletion);
            return new SSTableIdentityIterator(sstable, key, partitionLevelDeletion, dfile.getPath(), iterator);
        }
        catch (IOException e)
        {
            sstable.markSuspect();
            throw new CorruptSSTableException(e, dfile.getPath());
        }
    }

    public CFMetaData metadata()
    {
        return sstable.metadata;
    }

    public PartitionColumns columns()
    {
        return metadata().partitionColumns();
    }

    public boolean isReverseOrder()
    {
        return false;
    }

    public DecoratedKey partitionKey()
    {
        return key;
    }

    public DeletionTime partitionLevelDeletion()
    {
        return partitionLevelDeletion;
    }

    public Row staticRow()
    {
        return staticRow;
    }

    public boolean hasNext()
    {
        try
        {
            return iterator.hasNext();
        }
        catch (IndexOutOfBoundsException e)
        {
            sstable.markSuspect();
            throw new CorruptSSTableException(e, filename);
        }
        catch (IOError e)
        {
            if (e.getCause() instanceof IOException)
            {
                sstable.markSuspect();
                throw new CorruptSSTableException((Exception)e.getCause(), filename);
            }
            else
            {
                throw e;
            }
        }
    }

    public Unfiltered next()
    {
        try
        {
            return doCompute();
        }
        catch (IndexOutOfBoundsException e)
        {
            sstable.markSuspect();
            throw new CorruptSSTableException(e, filename);
        }
        catch (IOError e)
        {
            if (e.getCause() instanceof IOException)
            {
                sstable.markSuspect();
                throw new CorruptSSTableException((Exception)e.getCause(), filename);
            }
            else
            {
                throw e;
            }
        }
    }

    protected Unfiltered doCompute()
    {
        return iterator.next();
    }

    public void close()
    {
        // creator is responsible for closing file when finished
    }

    public String getPath()
    {
        return filename;
    }

    public EncodingStats stats()
    {
        // We could return sstable.header.stats(), but this may not be as accurate than the actual sstable stats (see
        // SerializationHeader.make() for details) so we use the latter instead.
        return new EncodingStats(sstable.getMinTimestamp(), sstable.getMinLocalDeletionTime(), sstable.getMinTTL());
    }

    public int compareTo(SSTableIdentityIterator o)
    {
        return key.compareTo(o.key);
    }
}
