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
package org.apache.cassandra.io.sstable.format.big;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.NoSuchFileException;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.DeserializationHelper;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredSerializer;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.IVerifier;
import org.apache.cassandra.io.sstable.IndexInfo;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SortedTableVerifier;
import org.apache.cassandra.io.sstable.format.big.BigFormat.Components;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.OutputHandler;

public class BigTableVerifier extends SortedTableVerifier<BigTableReader> implements IVerifier
{
    public BigTableVerifier(ColumnFamilyStore cfs, BigTableReader sstable, OutputHandler outputHandler, boolean isOffline, Options options)
    {
        super(cfs, sstable, outputHandler, isOffline, options);
    }

    protected void verifyPartition(DecoratedKey key, UnfilteredRowIterator iterator)
    {
        Row first = null;
        int duplicateRows = 0;
        long minTimestamp = Long.MAX_VALUE;
        long maxTimestamp = Long.MIN_VALUE;
        while (iterator.hasNext())
        {
            Unfiltered uf = iterator.next();
            if (uf.isRow())
            {
                Row row = (Row) uf;
                if (first != null && first.clustering().equals(row.clustering()))
                {
                    duplicateRows++;
                    for (Cell cell : row.cells())
                    {
                        maxTimestamp = Math.max(cell.timestamp(), maxTimestamp);
                        minTimestamp = Math.min(cell.timestamp(), minTimestamp);
                    }
                }
                else
                {
                    if (duplicateRows > 0)
                        logDuplicates(key, first, duplicateRows, minTimestamp, maxTimestamp);
                    duplicateRows = 0;
                    first = row;
                    maxTimestamp = Long.MIN_VALUE;
                    minTimestamp = Long.MAX_VALUE;
                }
            }
        }
        if (duplicateRows > 0)
            logDuplicates(key, first, duplicateRows, minTimestamp, maxTimestamp);
    }

    private void verifyIndexSummary()
    {
        try
        {
            if (outputHandler.isDebugEnabled()) outputHandler.debug("Deserializing index summary for %s", sstable);
            deserializeIndexSummary(sstable);
        }
        catch (Throwable t)
        {
            outputHandler.output("Index summary is corrupt - if it is removed it will get rebuilt on startup %s", sstable.descriptor.fileFor(Components.SUMMARY));
            outputHandler.warn(t);
            markAndThrow(t, false);
        }
    }

    protected void verifyIndex()
    {
        verifyIndexSummary();
        super.verifyIndex();
    }

    @Override
    protected void deserializeIndex(SSTableReader sstable) throws IOException
    {
        DeserializationHelper deserializationHelper = new DeserializationHelper(sstable.metadata(),
                                                                                sstable.descriptor.version.correspondingMessagingVersion(),
                                                                                DeserializationHelper.Flag.LOCAL);
        ClusteringComparator comparator = sstable.metadata().comparator;
        try (BigTableKeyReader it = (BigTableKeyReader)sstable.keyReader(true))
        {
            if (it.isExhausted())
            {
                if (sstable.hasSplitPrefix())
                    verifyFirstIndexEntry(it, false);
                return;
            }

            if (sstable.hasSplitPrefix())
                verifyFirstIndexEntry(it, false);
            ByteBuffer key;
            boolean isFirst = true;
            do
            {
                key = it.key();
                if (outputHandler.isDebugEnabled()) outputHandler.debug("Key %s", sstable.metadata().partitionKeyType.getString(key));

                if (isFirst && !Objects.equals(key, sstable.getFirst().getKey()))
                    throw new CorruptSSTableException(new IOException("First partition does not match index"), it.toString());
                else
                    isFirst = false;
                RowIndexEntry rowIndexEntry = it.rowIndexEntry();
                if (outputHandler.isDebugEnabled()) outputHandler.debug("rowIndexEntry %s", rowIndexEntry.toString());

                long partitionBase = it.dataPosition();
                int blockCount = rowIndexEntry.blockCount();
                if (options.extendedVerification && blockCount > 0)
                {
                    long expectedNextOffset = 0;
                    RowIndexEntry.IndexInfoRetriever indexInfoRetriever = rowIndexEntry.openWithIndex(it.indexFile());
                    for (int blockIndex = 0; blockIndex < blockCount; blockIndex++)
                    {
                        IndexInfo indexInfo = indexInfoRetriever.columnsIndex(blockIndex);
                        if (outputHandler.isDebugEnabled()) outputHandler.debug("indexInfo %s", indexInfo.toString(sstable.metadata()));

                        long dataFileOffset = partitionBase + indexInfo.offset;
                        if (expectedNextOffset != 0)
                        {
                            if (expectedNextOffset != indexInfo.offset)
                                throw new CorruptSSTableException(new IOException("Row entry indexInfo offset + width should match next block offset:" + indexInfo.offset + " expected: " + expectedNextOffset), it.toString());
                        }
                        expectedNextOffset = indexInfo.offset + indexInfo.width;
                        dataFile.seek(dataFileOffset);

                        Unfiltered unfiltered = UnfilteredSerializer.serializer.deserialize(dataFile,
                                                                                            sstable.header,
                                                                                            deserializationHelper,
                                                                                            BTreeRow.sortedBuilder());
                        if (!Objects.equals(indexInfo.firstName, unfiltered.clustering()))
                            throw new CorruptSSTableException(new IOException("Unfiltered clustering in index does not match data:{info=" + indexInfo.toString(sstable.metadata()) + ", row:" + unfiltered.clustering().toString(sstable.metadata()) + "}"), it.toString());

                        if (comparator.compare(indexInfo.firstName, indexInfo.lastName) > 0)
                            throw new CorruptSSTableException(new IOException("First name is > Last name:{info=" + indexInfo.toString(sstable.metadata()) + "}"), it.toString());

                    }
                }
            }
            // The verifier checks that the partition key/position in the index and data match; here we want to verify
            // the entries clustering keys match.
            while (it.advance()); // no-op, just check if index is readable

            if (!Objects.equals(key, sstable.getLast().getKey()))
                throw new CorruptSSTableException(new IOException("Last partition does not match index"), it.toString());
        }
        if (options.extendedVerification)
            dataFile.reset();
    }

    private void logDuplicates(DecoratedKey key, Row first, int duplicateRows, long minTimestamp, long maxTimestamp)
    {
        String keyString = sstable.metadata().partitionKeyType.getString(key.getKey());
        long firstMaxTs = Long.MIN_VALUE;
        long firstMinTs = Long.MAX_VALUE;
        for (Cell cell : first.cells())
        {
            firstMaxTs = Math.max(firstMaxTs, cell.timestamp());
            firstMinTs = Math.min(firstMinTs, cell.timestamp());
        }
        outputHandler.output("%d duplicate rows found for [%s %s] in %s.%s (%s), timestamps: [first row (%s, %s)], [duplicates (%s, %s, eq:%b)]",
                             duplicateRows,
                             keyString, first.clustering().toString(sstable.metadata()),
                             sstable.metadata().keyspace,
                             sstable.metadata().name,
                             sstable,
                             dateString(firstMinTs), dateString(firstMaxTs),
                             dateString(minTimestamp), dateString(maxTimestamp), minTimestamp == maxTimestamp);
    }

    private String dateString(long time)
    {
        return Instant.ofEpochMilli(TimeUnit.MICROSECONDS.toMillis(time)).toString();
    }

    private void deserializeIndexSummary(SSTableReader sstable) throws IOException
    {
        IndexSummaryComponent summaryComponent = IndexSummaryComponent.load(sstable.descriptor.fileFor(Components.SUMMARY), cfs.metadata());
        if (summaryComponent == null)
            throw new NoSuchFileException("Index summary component of sstable " + sstable.descriptor + " is missing");
        FileUtils.closeQuietly(summaryComponent.indexSummary);
    }
}
