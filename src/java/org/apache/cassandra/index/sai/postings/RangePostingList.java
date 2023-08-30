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

package org.apache.cassandra.index.sai.postings;

import java.io.IOException;

import org.apache.cassandra.index.sai.QueryContext;

public class RangePostingList implements PostingList
{
    private final PostingList wrapped;
    private final long rowIdOffset;
    private final long minimum;
    private final long maximum;
    private final long count;
    private final QueryContext queryContext;

    public RangePostingList(PostingList wrapped, long rowIdOffset, long minimum, long maximum, long count, QueryContext queryContext)
    {
        this.wrapped = wrapped;
        this.rowIdOffset = rowIdOffset;
        this.minimum = minimum;
        this.maximum = maximum;
        this.count = count;
        this.queryContext = queryContext;
    }

    @Override
    public long minimum()
    {
        return minimum;
    }

    @Override
    public long maximum()
    {
        return maximum;
    }

    @Override
    public long nextPosting() throws IOException
    {
        queryContext.checkpoint();
        return nextSSTableRowId(wrapped.nextPosting());
    }

    @Override
    public long size()
    {
        return count;
    }

    @Override
    public long advance(long targetRowID) throws IOException
    {
        queryContext.checkpoint();
        long segmentRowId = targetRowID - rowIdOffset;
        assert segmentRowId >= 0;
        return nextSSTableRowId(wrapped.advance(segmentRowId));
    }

    @Override
    public void close()
    {
        wrapped.close();
    }

    private long nextSSTableRowId(long segmentRowId)
    {
        return segmentRowId == PostingList.END_OF_STREAM ? PostingList.END_OF_STREAM : segmentRowId + rowIdOffset;
    }
}
