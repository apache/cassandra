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
package org.apache.cassandra.index.sai.disk;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.base.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.ScoreStoreProxy;
import org.apache.cassandra.index.sai.utils.AbortedOperationException;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.RangeIterator;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.Throwables;


/**
 * From sstable row id range iterator to primary key range iterator
 */
@NotThreadSafe
public class SSTableRowIdKeyRangeIterator extends RangeIterator<PrimaryKey>
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final Stopwatch timeToExhaust = Stopwatch.createStarted();
    private final QueryContext queryContext;
    private final PrimaryKeyMap primaryKeyMap;
    private final ScoreStoreProxy scoreStoreProxy;
    private final RangeIterator<Long> sstableRowIdIterator;

    private boolean needsSkipping = false;
    private PrimaryKey skipToToken = null;

    /**
     * Create a direct PostingListRangeIterator where the underlying PostingList is materialised
     * immediately so the posting list size can be used.
     */
    private SSTableRowIdKeyRangeIterator(PrimaryKey min,
                                         PrimaryKey max,
                                         long count,
                                         PrimaryKeyMap primaryKeyMap,
                                         QueryContext queryContext,
                                         RangeIterator<Long> sstableRowIdIterator)
    {
        super(min, max, count);

        this.primaryKeyMap = primaryKeyMap;
        this.queryContext = queryContext;
        this.scoreStoreProxy = queryContext.getScoreStoreProxyForSSTable(primaryKeyMap.getSSTableId());
        this.sstableRowIdIterator = sstableRowIdIterator;
    }

    public static RangeIterator<PrimaryKey> create(PrimaryKeyMap primaryKeyMap,
                                                   QueryContext queryContext,
                                                   RangeIterator<Long> sstableRowIdIterator)
    {
        if (sstableRowIdIterator.getCount() <= 0)
            return RangeIterator.emptyKeys();

        PrimaryKey min = primaryKeyMap.primaryKeyFromRowId(sstableRowIdIterator.getMinimum());
        PrimaryKey max = primaryKeyMap.primaryKeyFromRowId(sstableRowIdIterator.getMaximum());
        long count = sstableRowIdIterator.getCount();
        return new SSTableRowIdKeyRangeIterator(min, max, count, primaryKeyMap, queryContext, sstableRowIdIterator);
    }

    @Override
    protected void performSkipTo(PrimaryKey nextKey)
    {
        if (skipToToken != null && skipToToken.compareTo(nextKey) >= 0)
            return;

        skipToToken = nextKey;
        needsSkipping = true;
    }

    @Override
    protected PrimaryKey computeNext()
    {
        try
        {
            queryContext.checkpoint();

            // just end the iterator if we don't have a postingList or current segment is skipped
            if (exhausted())
                return endOfData();

            long rowId = getNextRowId();
            if (rowId == PostingList.END_OF_STREAM)
                return endOfData();

            PrimaryKey pk = primaryKeyMap.primaryKeyFromRowId(rowId);
            scoreStoreProxy.mapStoredScoreForRowIdToPrimaryKey(rowId, pk);
            return pk;
        }
        catch (Throwable t)
        {
            //VSTODO We aren't tidying up resources here
            if (!(t instanceof AbortedOperationException))
                logger.error("Unable to provide next token!", t);

            throw Throwables.cleaned(t);
        }
    }

    @Override
    public void close() throws IOException
    {
        if (logger.isTraceEnabled())
        {
            final long exhaustedInMills = timeToExhaust.stop().elapsed(TimeUnit.MILLISECONDS);
            logger.trace("PostinListRangeIterator exhausted after {} ms", exhaustedInMills);
        }

        FileUtils.closeQuietly(sstableRowIdIterator, primaryKeyMap);
    }

    private boolean exhausted()
    {
        return needsSkipping && skipToToken.compareTo(getMaximum()) > 0;
    }

    /**
     * reads the next sstable row ID from the underlying range iterator, potentially skipping to get there.
     */
    private long getNextRowId()
    {
        long sstableRowId;
        if (needsSkipping)
        {
            long targetRowID = primaryKeyMap.rowIdFromPrimaryKey(skipToToken);
            // skipToToken is larger than max token in token file
            if (targetRowID < 0)
                return PostingList.END_OF_STREAM;

            sstableRowIdIterator.skipTo(targetRowID);
            sstableRowId = sstableRowIdIterator.hasNext() ? sstableRowIdIterator.next() : PostingList.END_OF_STREAM;
            needsSkipping = false;
        }
        else
        {
            sstableRowId = sstableRowIdIterator.hasNext() ? sstableRowIdIterator.next() : PostingList.END_OF_STREAM;
        }

        return sstableRowId;
    }
}
