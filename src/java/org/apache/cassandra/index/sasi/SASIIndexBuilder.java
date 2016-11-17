/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.index.sasi;

import java.io.File;
import java.io.IOException;
import java.util.*;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.RowIndexEntry;
import org.apache.cassandra.db.compaction.CompactionInfo;
import org.apache.cassandra.db.compaction.CompactionInterruptedException;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.index.SecondaryIndexBuilder;
import org.apache.cassandra.index.sasi.conf.ColumnIndex;
import org.apache.cassandra.index.sasi.disk.PerSSTableIndexWriter;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.sstable.KeyIterator;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.SSTableIdentityIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.UUIDGen;

class SASIIndexBuilder extends SecondaryIndexBuilder
{
    private final ColumnFamilyStore cfs;
    private final UUID compactionId = UUIDGen.getTimeUUID();

    private final SortedMap<SSTableReader, Map<ColumnDefinition, ColumnIndex>> sstables;

    private long bytesProcessed = 0;
    private final long totalSizeInBytes;

    public SASIIndexBuilder(ColumnFamilyStore cfs, SortedMap<SSTableReader, Map<ColumnDefinition, ColumnIndex>> sstables)
    {
        long totalIndexBytes = 0;
        for (SSTableReader sstable : sstables.keySet())
            totalIndexBytes += getPrimaryIndexLength(sstable);

        this.cfs = cfs;
        this.sstables = sstables;
        this.totalSizeInBytes = totalIndexBytes;
    }

    public void build()
    {
        AbstractType<?> keyValidator = cfs.metadata.getKeyValidator();
        for (Map.Entry<SSTableReader, Map<ColumnDefinition, ColumnIndex>> e : sstables.entrySet())
        {
            SSTableReader sstable = e.getKey();
            Map<ColumnDefinition, ColumnIndex> indexes = e.getValue();

            try (RandomAccessReader dataFile = sstable.openDataReader())
            {
                PerSSTableIndexWriter indexWriter = SASIIndex.newWriter(keyValidator, sstable.descriptor, indexes, OperationType.COMPACTION);

                long previousKeyPosition = 0;
                try (KeyIterator keys = new KeyIterator(sstable.descriptor, cfs.metadata))
                {
                    while (keys.hasNext())
                    {
                        if (isStopRequested())
                            throw new CompactionInterruptedException(getCompactionInfo());

                        final DecoratedKey key = keys.next();
                        final long keyPosition = keys.getKeyPosition();

                        indexWriter.startPartition(key, keyPosition);

                        try
                        {
                            RowIndexEntry indexEntry = sstable.getPosition(key, SSTableReader.Operator.EQ);
                            dataFile.seek(indexEntry.position);
                            ByteBufferUtil.readWithShortLength(dataFile); // key

                            try (SSTableIdentityIterator partition = SSTableIdentityIterator.create(sstable, dataFile, key))
                            {
                                // if the row has statics attached, it has to be indexed separately
                                if (cfs.metadata.hasStaticColumns())
                                    indexWriter.nextUnfilteredCluster(partition.staticRow());

                                while (partition.hasNext())
                                    indexWriter.nextUnfilteredCluster(partition.next());
                            }
                        }
                        catch (IOException ex)
                        {
                            throw new FSReadError(ex, sstable.getFilename());
                        }

                        bytesProcessed += keyPosition - previousKeyPosition;
                        previousKeyPosition = keyPosition;
                    }

                    completeSSTable(indexWriter, sstable, indexes.values());
                }
            }
        }
    }

    public CompactionInfo getCompactionInfo()
    {
        return new CompactionInfo(cfs.metadata,
                                  OperationType.INDEX_BUILD,
                                  bytesProcessed,
                                  totalSizeInBytes,
                                  compactionId);
    }

    private long getPrimaryIndexLength(SSTable sstable)
    {
        File primaryIndex = new File(sstable.getIndexFilename());
        return primaryIndex.exists() ? primaryIndex.length() : 0;
    }

    private void completeSSTable(PerSSTableIndexWriter indexWriter, SSTableReader sstable, Collection<ColumnIndex> indexes)
    {
        indexWriter.complete();

        for (ColumnIndex index : indexes)
        {
            File tmpIndex = new File(sstable.descriptor.filenameFor(index.getComponent()));
            if (!tmpIndex.exists()) // no data was inserted into the index for given sstable
                continue;

            index.update(Collections.<SSTableReader>emptyList(), Collections.singletonList(sstable));
        }
    }
}
