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
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.base.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.exceptions.QueryCancelledException;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.iterators.KeyRangeIterator;
import org.apache.cassandra.index.sai.postings.PostingList;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.Throwables;


/**
 * From sstable row id range iterator to primary key range iterator
 */
@NotThreadSafe
public class SSTableRowIdKeyRangeIterator extends KeyRangeIterator
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final Stopwatch timeToExhaust = Stopwatch.createStarted();
    private final QueryContext queryContext;
    private final PrimaryKeyMap primaryKeyMap;
    private final PostingList postingList;

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
                                         PostingList postingList)
    {
        super(min, max, count);

        this.primaryKeyMap = primaryKeyMap;
        this.queryContext = queryContext;
        this.postingList = postingList;
    }

    public static KeyRangeIterator create(PrimaryKeyMap primaryKeyMap, QueryContext queryContext, PostingList postingList)
    {
        if (postingList.size() <= 0)
            return KeyRangeIterator.empty();

        PrimaryKey min = primaryKeyMap.primaryKeyFromRowId(postingList.minimum());
        PrimaryKey max = primaryKeyMap.primaryKeyFromRowId(postingList.maximum());
        return new SSTableRowIdKeyRangeIterator(min, max, postingList.size(), primaryKeyMap, queryContext, postingList);
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
            //VSTODO We aren't tidying up resources here
            if (!(t instanceof QueryCancelledException))
                logger.error("Unable to provide next token!", t);

            throw Throwables.cleaned(t);
        }
    }

    @Override
    public void close()
    {
        if (logger.isTraceEnabled())
        {
            final long exhaustedInMills = timeToExhaust.stop().elapsed(TimeUnit.MILLISECONDS);
            logger.trace("PostinListRangeIterator exhausted after {} ms", exhaustedInMills);
        }

        FileUtils.closeQuietly(Arrays.asList(postingList, primaryKeyMap));
    }

    private boolean exhausted()
    {
        return needsSkipping && skipToToken.compareTo(getMaximum()) > 0;
    }

    /**
     * reads the next sstable row ID from the underlying range iterator, potentially skipping to get there.
     */
    private long getNextRowId() throws IOException
    {
        long sstableRowId;
        if (needsSkipping)
        {
            long targetRowID = primaryKeyMap.rowIdFromPrimaryKey(skipToToken);
            // skipToToken is larger than max token in token file
            if (targetRowID < 0)
                return PostingList.END_OF_STREAM;

            sstableRowId = postingList.advance(targetRowID);
            needsSkipping = false;
        }
        else
        {
            sstableRowId = postingList.nextPosting();
        }

        return sstableRowId;
    }
}
