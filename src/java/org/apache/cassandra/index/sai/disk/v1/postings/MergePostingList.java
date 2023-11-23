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

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import javax.annotation.concurrent.NotThreadSafe;

import org.apache.cassandra.index.sai.postings.PeekablePostingList;
import org.apache.cassandra.index.sai.postings.PostingList;
import org.apache.cassandra.io.util.FileUtils;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Merges multiple {@link PostingList} which individually contain unique items into a single list.
 * While the individual lists contain unique items, there can be duplicate items between lists so
 * the class also checks for duplicates and only returns unique items in sorted order.
 */
@NotThreadSafe
public class MergePostingList implements PostingList
{
    private final PriorityQueue<PeekablePostingList> postingLists;
    private final List<PeekablePostingList> temp;
    private final Closeable onClose;
    private final long minimum;
    private final long maximum;
    private final long size;
    private long lastRowId = -1;

    private MergePostingList(PriorityQueue<PeekablePostingList> postingLists, Closeable onClose)
    {
        this.temp = new ArrayList<>(postingLists.size());
        this.onClose = onClose;
        this.postingLists = postingLists;
        long minimum = 0;
        long maximum = 0;
        long totalPostings = 0;
        for (PostingList postingList : postingLists)
        {
            minimum = Math.min(minimum, postingList.minimum());
            maximum = Math.max(maximum, postingList.maximum());
            totalPostings += postingList.size();
        }
        this.minimum = minimum;
        this.maximum = maximum;
        this.size = totalPostings;
    }

    public static PostingList merge(PriorityQueue<PeekablePostingList> postings, Closeable onClose)
    {
        checkArgument(!postings.isEmpty(), "Cannot merge an empty queue of posting lists");
        return postings.size() > 1 ? new MergePostingList(postings, onClose) : postings.poll();
    }

    public static PostingList merge(PriorityQueue<PeekablePostingList> postings)
    {
        return merge(postings, () -> FileUtils.close(postings));
    }

    public static PostingList merge(List<PostingList> postings)
    {
        PriorityQueue<PeekablePostingList> postingsQueue = new PriorityQueue<>(postings.size(), Comparator.comparingLong(PeekablePostingList::peek));
        postings.stream().map(PeekablePostingList::makePeekable).forEach(postingsQueue::add);
        return merge(postingsQueue);
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
        while (!postingLists.isEmpty())
        {
            PeekablePostingList head = postingLists.poll();
            long next = head.nextPosting();

            if (next == END_OF_STREAM)
            {
                // skip current posting list
                continue;
            }

            if (next > lastRowId)
            {
                lastRowId = next;
                postingLists.add(head);
                return next;
            }
            else if (next == lastRowId)
            {
                postingLists.add(head);
            }
        }

        return PostingList.END_OF_STREAM;
    }

    @Override
    public long advance(long targetRowID) throws IOException
    {
        temp.clear();

        while (!postingLists.isEmpty())
        {
            PeekablePostingList peekable = postingLists.poll();
            peekable.advanceWithoutConsuming(targetRowID);
            if (peekable.peek() != PostingList.END_OF_STREAM)
                temp.add(peekable);
        }
        postingLists.addAll(temp);

        return nextPosting();
    }

    @Override
    public long size()
    {
        return size;
    }

    @Override
    public void close()
    {
        FileUtils.closeQuietly(onClose);
    }
}
