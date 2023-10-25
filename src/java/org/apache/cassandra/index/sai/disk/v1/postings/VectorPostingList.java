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
import java.util.PrimitiveIterator;

import org.apache.cassandra.index.sai.postings.PostingList;
import org.apache.lucene.util.LongHeap;

/**
 * A {@link PostingList} for ANN search results. Transforms result from similarity order to row ID order.
 */
public class VectorPostingList implements PostingList
{
    private final LongHeap segmentRowIds;
    private final int size;
    private final int visitedCount;

    public VectorPostingList(PrimitiveIterator.OfInt source, int limit, int visitedCount)
    {
        this.visitedCount = visitedCount;
        segmentRowIds = new LongHeap(Math.max(limit, 1));
        int n = 0;
        while (source.hasNext() && n++ < limit)
            segmentRowIds.push(source.nextInt());
        this.size = n;
    }

    @Override
    public long nextPosting()
    {
        if (segmentRowIds.size() == 0)
            return PostingList.END_OF_STREAM;
        return segmentRowIds.pop();
    }

    @Override
    public long size()
    {
        return size;
    }

    @Override
    public long advance(long targetRowID) throws IOException
    {
        long rowId;
        do
        {
            rowId = nextPosting();
        } while (rowId < targetRowID);
        return rowId;
    }

    public int getVisitedCount()
    {
        return visitedCount;
    }
}
