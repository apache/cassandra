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

import com.google.common.base.Preconditions;

public class SSTableRowIdPostingList implements PostingList
{
    private final PostingList segmentPostingList;
    private final long segmentOffset;

    public SSTableRowIdPostingList(PostingList segmentPostingList, long segmentOffset)
    {
        this.segmentPostingList = segmentPostingList;
        this.segmentOffset = segmentOffset;
    }

    @Override
    public void close() throws IOException
    {
        segmentPostingList.close();
    }

    @Override
    public long nextPosting() throws IOException
    {
        long segmentRowId = segmentPostingList.nextPosting();
        return segmentRowId == PostingList.END_OF_STREAM ? segmentRowId : segmentRowId + segmentOffset;
    }

    @Override
    public long size()
    {
        return segmentPostingList.size();
    }

    @Override
    public long advance(long targetRowID) throws IOException
    {
        long segmentRowId = targetRowID - segmentOffset;
        Preconditions.checkState(segmentRowId >= 0);

        long ret = segmentPostingList.advance(segmentRowId);
        return ret == PostingList.END_OF_STREAM ? ret : ret + segmentOffset;
    }

    @Override
    public PeekablePostingList peekable()
    {
        return new PeekablePostingList(this);
    }
}
