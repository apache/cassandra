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

import com.google.common.base.MoreObjects;

public class IntArrayPostingList implements OrdinalPostingList
{
    private final int[] postings;
    private int idx = 0;

    public IntArrayPostingList(int[] postings)
    {
        this.postings = postings;
    }

    @Override
    public long getOrdinal()
    {
        return idx;
    }

    @Override
    public long nextPosting()
    {
        if (idx >= postings.length)
        {
            return PostingList.END_OF_STREAM;
        }
        return postings[idx++];
    }

    @Override
    public long size()
    {
        return postings.length;
    }

    @Override
    public long advance(long targetRowID)
    {
        for (int i = idx; i < postings.length; ++i)
        {
            final int segmentRowId = getPostingAt(i);

            idx++;

            if (segmentRowId >= targetRowID)
            {
                return segmentRowId;
            }
        }
        return PostingList.END_OF_STREAM;
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                          .add("idx", idx)
                          .add("hashCode", Integer.toHexString(hashCode()))
                          .toString();
    }

    public int getPostingAt(int i)
    {
        return postings[i];
    }
}
