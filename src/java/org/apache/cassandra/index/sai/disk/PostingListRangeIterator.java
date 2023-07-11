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
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.RangeIterator;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.Throwables;

/**
 * A range iterator based on {@link PostingList}.
 *
 * <ol>
 *   <li> fetch next segment row id from posting list or skip to specific segment row id if {@link #skipTo(PrimaryKey)} is called </li>
 *   <li> add segmentRowIdOffset to obtain the sstable row id </li>
 *   <li> produce a {@link PrimaryKey} from {@link PrimaryKeyMap#primaryKeyFromRowId(long)} which is used
 *       to avoid fetching duplicated keys due to partition-level indexing on wide partition schema.
 *       <br/>
 *       Note: in order to reduce disk access in multi-index query, partition keys will only be fetched for intersected tokens
 *       in {@link org.apache.cassandra.index.sai.plan.StorageAttachedIndexSearcher}.
 *  </li>
 * </ol>
 *
 */

@NotThreadSafe
public class PostingListRangeIterator extends RangeIterator<PrimaryKey>
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final Stopwatch timeToExhaust = Stopwatch.createStarted();
    private final QueryContext queryContext;

    private final PostingList postingList;
    private final IndexContext indexContext;
    private final PrimaryKeyMap primaryKeyMap;
    private final IndexSearcherContext searcherContext;

    private boolean needsSkipping = false;
    private PrimaryKey skipToToken = null;


    /**
     * Create a direct PostingListRangeIterator where the underlying PostingList is materialised
     * immediately so the posting list size can be used.
     */
    public PostingListRangeIterator(IndexContext indexContext,
                                    PrimaryKeyMap primaryKeyMap,
                                    IndexSearcherContext searcherContext)
    {
        super(searcherContext.minimumKey, searcherContext.maximumKey, searcherContext.count());

        this.indexContext = indexContext;
        this.primaryKeyMap = primaryKeyMap;
        this.postingList = searcherContext.postingList;
        this.searcherContext = searcherContext;
        this.queryContext = this.searcherContext.context;
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

            return primaryKeyMap.primaryKeyFromRowId(rowId);
        }
        catch (Throwable t)
        {
            //TODO We aren't tidying up resources here
            if (!(t instanceof AbortedOperationException))
                logger.error(indexContext.logMessage("Unable to provide next token!"), t);

            throw Throwables.cleaned(t);
        }
    }

    @Override
    public void close() throws IOException
    {
        if (logger.isTraceEnabled())
        {
            final long exhaustedInMills = timeToExhaust.stop().elapsed(TimeUnit.MILLISECONDS);
            logger.trace(indexContext.logMessage("PostinListRangeIterator exhausted after {} ms"), exhaustedInMills);
        }

        FileUtils.closeQuietly(postingList, primaryKeyMap);
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
        long segmentRowId;
        if (needsSkipping)
        {
            long targetRowID = primaryKeyMap.rowIdFromPrimaryKey(skipToToken);
            // skipToToken is larger than max token in token file
            if (targetRowID < 0)
            {
                return PostingList.END_OF_STREAM;
            }

            segmentRowId = postingList.advance(targetRowID - searcherContext.segmentRowIdOffset);
            needsSkipping = false;
        }
        else
        {
            segmentRowId = postingList.nextPosting();
        }

        return segmentRowId != PostingList.END_OF_STREAM
               ? segmentRowId + searcherContext.segmentRowIdOffset
               : PostingList.END_OF_STREAM;
    }
}
