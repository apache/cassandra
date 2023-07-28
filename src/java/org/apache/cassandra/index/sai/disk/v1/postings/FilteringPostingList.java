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

import org.apache.cassandra.index.sai.postings.OrdinalPostingList;
import org.apache.cassandra.index.sai.postings.PostingList;
import org.apache.lucene.util.FixedBitSet;

/**
 * A wrapper that iterates over a delegate {@link PostingList}, filtering out postings at
 * positions that are not present in a provided filter.
 */
public class FilteringPostingList implements PostingList
{
    private final FixedBitSet filter;
    private final OrdinalPostingList delegate;
    private final int cardinality;
    private int position = 0;

    public FilteringPostingList(FixedBitSet filter, OrdinalPostingList delegate)
    {
        cardinality = filter.cardinality();
        this.filter = filter;
        this.delegate = delegate;
    }

    @Override
    public void close()
    {
        delegate.close();
    }

    /**
     *
     * @return the segment row ID of the next match
     */
    @Override
    public long nextPosting() throws IOException
    {
        while (true)
        {
            long segmentRowId = delegate.nextPosting();

            if (segmentRowId == PostingList.END_OF_STREAM)
            {
                return PostingList.END_OF_STREAM;
            }

            if (filter.get(position++))
            {
                return segmentRowId;
            }
        }
    }

    @Override
    public long size()
    {
        return cardinality;
    }

    @Override
    public  long advance(long targetRowID) throws IOException
    {
        long segmentRowId = delegate.advance(targetRowID);

        if (segmentRowId == PostingList.END_OF_STREAM)
        {
            return PostingList.END_OF_STREAM;
        }

        // these are always for leaf balanced tree postings so the max is 1024
        position = (int)delegate.getOrdinal();

        // If the ordinal of the ID we just read satisfies the filter, just return it...
        if (filter.get(position - 1))
        {
            return segmentRowId;
        }

        // ...but if the ID doesn't satisfy the filter, get the next match.
        return nextPosting();
    }
}
