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

import java.util.PrimitiveIterator;

import org.apache.cassandra.index.sai.postings.PostingList;
import org.apache.lucene.util.LongHeap;

/**
 * A {@link PostingList} implementation that takes an unordered iterator of primitive {@code int} and returns
 * ordered {@code long} postings.
 */
public class ReorderingPostingList implements PostingList
{
    private final LongHeap segmentRowIds;
    private final int size;

    public ReorderingPostingList(PrimitiveIterator.OfInt source, int estimatedSize)
    {
        segmentRowIds = new LongHeap(Math.max(estimatedSize, 1));
        int n = 0;
        while (source.hasNext())
        {
            segmentRowIds.push(source.nextInt());
            n++;
        }
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
    public long advance(long targetRowID)
    {
        long rowId;
        do
        {
            rowId = nextPosting();
        }
        while (rowId < targetRowID);
        return rowId;
    }
}
