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

import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.utils.AbortedOperationException;
import org.apache.cassandra.index.sai.utils.RangeIterator;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.Throwables;

@NotThreadSafe
public class SSTableRowIdsRangeIterator extends RangeIterator<Long>
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final Stopwatch timeToExhaust = Stopwatch.createStarted();
    private final QueryContext queryContext;

    private final PostingList sstableRowIdPostings;
    private final IndexContext indexContext;

    private boolean needsSkipping = false;
    private Long skipToToken = null;


    /**
     * Create a direct PostingListRangeIterator where the underlying PostingList is materialised
     * immediately so the posting list size can be used.
     */
    public SSTableRowIdsRangeIterator(IndexContext indexContext, IndexSearcherContext searcherContext)
    {
        super(searcherContext.minSSTableRowId, searcherContext.maxSSTableRowId, searcherContext.count());

        this.indexContext = indexContext;
        this.sstableRowIdPostings = searcherContext.postingList;
        this.queryContext = searcherContext.context;
    }

    @Override
    protected void performSkipTo(Long nextKey)
    {
        if (skipToToken != null && skipToToken.compareTo(nextKey) >= 0)
            return;

        skipToToken = nextKey;
        needsSkipping = true;
    }

    @Override
    protected Long computeNext()
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

            return rowId;
        }
        catch (Throwable t)
        {
            //VSTODO We aren't tidying up resources here
            if (!(t instanceof AbortedOperationException))
                logger.error(indexContext.logMessage("Unable to provide next sstable row id!"), t);

            throw Throwables.cleaned(t);
        }
    }

    @Override
    public void close() throws IOException
    {
        if (logger.isTraceEnabled())
        {
            final long exhaustedInMills = timeToExhaust.stop().elapsed(TimeUnit.MILLISECONDS);
            logger.trace(indexContext.logMessage("SSTableRowIdsRangeIterator exhausted after {} ms"), exhaustedInMills);
        }

        FileUtils.closeQuietly(sstableRowIdPostings);
    }

    private boolean exhausted()
    {
        return needsSkipping && skipToToken.compareTo(getMaximum()) > 0;
    }

    /**
     * reads the next sstable row ID from the underlying posting list, potentially skipping to get there.
     */
    private long getNextRowId() throws IOException
    {
        long sstableRowId;
        if (needsSkipping)
        {
            long targetRowID = skipToToken;
            // skipToToken is larger than max token in token file
            if (targetRowID < 0)
            {
                return PostingList.END_OF_STREAM;
            }

            sstableRowId = sstableRowIdPostings.advance(targetRowID);
            needsSkipping = false;
        }
        else
        {
            sstableRowId = sstableRowIdPostings.nextPosting();
        }

        return sstableRowId;
    }
}
