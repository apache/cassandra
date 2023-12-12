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
package org.apache.cassandra.index.sai.disk.v1.postings;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.base.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.exceptions.QueryCancelledException;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.utils.IndexIdentifier;
import org.apache.cassandra.index.sai.disk.v1.segment.IndexSegmentSearcherContext;
import org.apache.cassandra.index.sai.iterators.KeyRangeIterator;
import org.apache.cassandra.index.sai.postings.PostingList;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.Throwables;

/**
 * A key iterator based on a  {@link PostingList} derived from a single index segment.
 *
 * <ol>
 *   <li> fetch next segment row id from posting list or skip to specific segment row id if {@link #skipTo(PrimaryKey)} is called </li>
 *   <li> add {@link IndexSegmentSearcherContext#segmentRowIdOffset} to obtain the sstable row id </li>
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
public class PostingListRangeIterator extends KeyRangeIterator
{
    private static final Logger logger = LoggerFactory.getLogger(PostingListRangeIterator.class);

    private final Stopwatch timeToExhaust = Stopwatch.createStarted();
    private final QueryContext queryContext;

    private final PostingList postingList;
    private final IndexIdentifier indexIdentifier;
    private final PrimaryKeyMap primaryKeyMap;
    private final long rowIdOffset;

    private boolean needsSkipping = false;
    private PrimaryKey skipToKey = null;

    /**
     * Create a direct PostingListRangeIterator where the underlying PostingList is materialised
     * immediately so the posting list size can be used.
     */
    public PostingListRangeIterator(IndexIdentifier indexIdentifier,
                                    PrimaryKeyMap primaryKeyMap,
                                    IndexSegmentSearcherContext searcherContext)
    {
        super(searcherContext.minimumKey, searcherContext.maximumKey, searcherContext.count());

        this.indexIdentifier = indexIdentifier;
        this.primaryKeyMap = primaryKeyMap;
        this.postingList = searcherContext.postingList;
        this.rowIdOffset = searcherContext.segmentRowIdOffset;
        this.queryContext = searcherContext.context;
    }

    @Override
    protected void performSkipTo(PrimaryKey nextKey)
    {
        if (skipToKey != null && skipToKey.compareTo(nextKey) > 0)
            return;

        skipToKey = nextKey;
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
            if (!(t instanceof QueryCancelledException))
                logger.error(indexIdentifier.logMessage("Unable to provide next token!"), t);

            FileUtils.closeQuietly(Arrays.asList(postingList, primaryKeyMap));
            throw Throwables.cleaned(t);
        }
    }

    @Override
    public void close()
    {
        if (logger.isTraceEnabled())
        {
            final long exhaustedInMills = timeToExhaust.stop().elapsed(TimeUnit.MILLISECONDS);
            logger.trace(indexIdentifier.logMessage("PostingListRangeIterator exhausted after {} ms"), exhaustedInMills);
        }

        FileUtils.closeQuietly(Arrays.asList(postingList, primaryKeyMap));
    }

    private boolean exhausted()
    {
        return needsSkipping && skipToKey.compareTo(getMaximum()) > 0;
    }

    /**
     * reads the next sstable row ID from the underlying posting list, potentially skipping to get there.
     */
    private long getNextRowId() throws IOException
    {
        long segmentRowId;
        if (needsSkipping)
        {
            long targetRowID = primaryKeyMap.rowIdFromPrimaryKey(skipToKey);
            // skipToToken is larger than max token in token file
            if (targetRowID < 0)
            {
                return PostingList.END_OF_STREAM;
            }

            segmentRowId = postingList.advance(targetRowID - rowIdOffset);

            needsSkipping = false;
        }
        else
        {
            segmentRowId = postingList.nextPosting();
        }

        return segmentRowId != PostingList.END_OF_STREAM
               ? segmentRowId + rowIdOffset
               : PostingList.END_OF_STREAM;
    }
}
