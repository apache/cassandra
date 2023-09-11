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
import java.util.List;
import java.util.stream.Collectors;

import org.apache.cassandra.io.util.FileUtils;

public class PostingListIntersection implements PostingList
{
    private final List<PeekablePostingList> postings;
    private final long size;

    public static PostingList createFromPeekable(List<PeekablePostingList> postings)
    {
        if (postings == null || postings.isEmpty())
            return PostingList.EMPTY;
        if (postings.size() == 1)
            return postings.get(0);
        return new PostingListIntersection(postings);
    }

    public static PostingList createFromPostings(List<PostingList> postings)
    {
        if (postings == null || postings.isEmpty())
            return PostingList.EMPTY;
        if (postings.size() == 1)
            return postings.get(0);
        return new PostingListIntersection(postings.stream().map(PeekablePostingList::makePeekable).collect(Collectors.toList()));
    }

    private PostingListIntersection(List<PeekablePostingList> postings)
    {
        this.postings = postings;
        this.size = postings.stream().mapToLong(PostingList::size).count();
    }

    @Override
    public long nextPosting() throws IOException
    {
        long highest = Long.MIN_VALUE;

        PeekablePostingList advanced = null;

        outer:
        while (true)
        {
            for (PeekablePostingList postingList : postings)
            {
                if (postingList == advanced)
                    continue;
                long nextPosting = highest > Long.MIN_VALUE && highest >= postingList.peek() ? postingList.advance(highest)
                                                                                             : postingList.nextPosting();

                // If any posting list has finished then the intersection has finished
                if (nextPosting == PostingList.END_OF_STREAM)
                    return PostingList.END_OF_STREAM;

                if (nextPosting > highest)
                {
                    highest = nextPosting;
                    advanced = postingList;
                    continue outer;
                }
            }
            return highest;
        }
    }

    @Override
    public long size()
    {
        return size;
    }

    @Override
    public long advance(long targetRowID) throws IOException
    {
        long highest = Long.MIN_VALUE;
        for (PostingList postingList : postings)
        {
            highest = Math.max(highest, postingList.advance(targetRowID));
        }
        return highest;
    }

    @Override
    public void close()
    {
        FileUtils.closeQuietly(postings);
    }
}
