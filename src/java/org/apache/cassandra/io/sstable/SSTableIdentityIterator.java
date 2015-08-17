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

import org.apache.cassandra.utils.AbstractIterator;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.RandomAccessReader;

public class SSTableIdentityIterator extends AbstractIterator<Unfiltered> implements Comparable<SSTableIdentityIterator>, UnfilteredRowIterator
{
    private final SSTableReader sstable;
    private final DecoratedKey key;
    private final DeletionTime partitionLevelDeletion;
    private final String filename;

    private final SSTableSimpleIterator iterator;
    private final Row staticRow;

    /**
     * Used to iterate through the columns of a row.
     * @param sstable SSTable we are reading ffrom.
     * @param file Reading using this file.
     * @param key Key of this row.
     */
    public SSTableIdentityIterator(SSTableReader sstable, RandomAccessReader file, DecoratedKey key)
    {
        this.sstable = sstable;
        this.filename = file.getPath();
        this.key = key;

        try
        {
            this.partitionLevelDeletion = DeletionTime.serializer.deserialize(file);
            SerializationHelper helper = new SerializationHelper(sstable.metadata, sstable.descriptor.version.correspondingMessagingVersion(), SerializationHelper.Flag.LOCAL);
            this.iterator = SSTableSimpleIterator.create(sstable.metadata, file, sstable.header, helper, partitionLevelDeletion);
            this.staticRow = iterator.readStaticRow();
        }
        catch (IOException e)
        {
            sstable.markSuspect();
            throw new CorruptSSTableException(e, filename);
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

    protected Unfiltered computeNext()
    {
        try
        {
            return iterator.hasNext() ? iterator.next() : endOfData();
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
