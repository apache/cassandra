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

import org.apache.cassandra.index.sai.SSTableContext;
import org.apache.cassandra.index.sai.SSTableQueryContext;
import org.apache.cassandra.index.sai.Token;
import org.apache.cassandra.index.sai.disk.io.IndexComponents;
import org.apache.cassandra.index.sai.utils.AbortedOperationException;
import org.apache.cassandra.index.sai.utils.LongArray;
import org.apache.cassandra.index.sai.utils.RangeIterator;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.utils.Throwables;

/**
 * A range iterator based on {@link PostingList}.
 *
 * <ol>
 *   <li> fetch next segment row id from posting list or skip to specific segment row id if {@link #skipTo(Long)} is called </li>
 *   <li> produce a {@link OnDiskKeyProducer.OnDiskToken} from {@link OnDiskKeyProducer#produceToken(long, int)} which is used
 *       to avoid fetching duplicated keys due to partition-level indexing on wide partition schema.
 *       <br/>
 *       Note: in order to reduce disk access in multi-index query, partition keys will only be fetched for intersected tokens
 *       in {@link org.apache.cassandra.index.sai.plan.StorageAttachedIndexSearcher}.
 *  </li>
 * </ol>
 *
 */

@NotThreadSafe
public class PostingListRangeIterator extends RangeIterator
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final Stopwatch timeToExhaust = Stopwatch.createStarted();
    private final SSTableQueryContext queryContext;
    private final IndexComponents components;

    private final PostingList postingList;
    private final SSTableContext.KeyFetcher keyFetcher;
    private final IndexSearcher.SearcherContext context;
    private final LongArray segmentRowIdToToken;
    private final LongArray segmentRowIdToOffset;

    private RandomAccessReader keyReader = null;
    private OnDiskKeyProducer producer = null;

    private boolean opened = false;
    private boolean needsSkipping = false;
    private long skipToToken = Long.MIN_VALUE;


    /**
     * Create a direct PostingListRangeIterator where the underlying PostingList is materialised
     * immediately so the posting list size can be used.
     */
    public PostingListRangeIterator(IndexSearcher.SearcherContext context,
                                    SSTableContext.KeyFetcher keyFetcher,
                                    IndexComponents components)
    {
        super(context.minToken(), context.maxToken(), context.count());

        this.keyFetcher = keyFetcher;
        this.segmentRowIdToToken = context.segmentRowIdToToken;
        this.segmentRowIdToOffset = context.segmentRowIdToOffset;
        this.postingList = context.postingList;
        this.context = context;
        this.queryContext = context.context;
        this.components = components;
    }

    @Override
    protected void performSkipTo(Long nextToken)
    {
        if (skipToToken >= nextToken)
            return;

        skipToToken = nextToken;
        needsSkipping = true;
    }

    @Override
    protected Token computeNext()
    {
        try
        {
            queryContext.queryContext.checkpoint();

            if (!opened)
                open();

            // just end the iterator if we don't have a postingList or current segment is skipped
            if (exhausted())
                return endOfData();

            long segmentRowId = getNextSegmentRowId();
            if (segmentRowId == PostingList.END_OF_STREAM)
                return endOfData();

            return getNextToken(segmentRowId);
        }
        catch (Throwable t)
        {
            //TODO We aren't tidying up resources here
            if (!(t instanceof AbortedOperationException))
                logger.error(components.logMessage("Unable to provide next token!"), t);

            throw Throwables.cleaned(t);
        }
    }

    @Override
    public void close() throws IOException
    {
        if (logger.isTraceEnabled())
        {
            final long exhaustedInMills = timeToExhaust.stop().elapsed(TimeUnit.MILLISECONDS);
            logger.trace(components.logMessage("PostinListRangeIterator exhausted after {} ms"), exhaustedInMills);
        }

        postingList.close();
        FileUtils.closeQuietly(segmentRowIdToToken, segmentRowIdToOffset, keyReader);
    }

    private void open()
    {
        this.keyReader = keyFetcher.createReader();
        this.producer = new OnDiskKeyProducer(keyFetcher, keyReader, segmentRowIdToOffset, context.maxPartitionOffset);
        opened = true;
    }

    private boolean exhausted()
    {
        return needsSkipping && skipToToken > getMaximum();
    }

    /**
     * reads the next row ID from the underlying posting list, potentially skipping to get there.
     */
    private long getNextSegmentRowId() throws IOException
    {
        if (needsSkipping)
        {
            int targetRowID = Math.toIntExact(segmentRowIdToToken.findTokenRowID(skipToToken));
            // skipToToken is larger than max token in token file
            if (targetRowID < 0)
            {
                return PostingList.END_OF_STREAM;
            }

            long segmentRowId = postingList.advance(targetRowID);

            needsSkipping = false;
            return segmentRowId;
        }
        else
        {
            return postingList.nextPosting();
        }
    }

    /**
     * takes a segment row ID and produces a {@link Token} for its partition key.
     */
    private Token getNextToken(long segmentRowId)
    {
        assert segmentRowId != PostingList.END_OF_STREAM;

        long tokenValue = segmentRowIdToToken.get(segmentRowId);

        // Used to remove duplicated key offset
        return producer.produceToken(tokenValue, segmentRowId);
    }
}
