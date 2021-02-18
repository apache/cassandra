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
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.PriorityQueue;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.index.sai.disk.v1.MergePostingList;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;

public class TermsIteratorMerger implements TermsIterator
{
    private final TermsIterator[] iterators;
    private final MergingIterator mergedIterator;
    private final AbstractType<?> type;

    public long maxSSTableRowId = -1;
    public long minSSTableRowId = Long.MAX_VALUE;
    public ByteComparable minTerm;
    public ByteComparable maxTerm;

    public TermsIteratorMerger(final TermsIterator[] iterators, AbstractType<?> type)
    {
        this.iterators = iterators;
        this.mergedIterator = new MergingIterator(type, iterators);
        this.type = type;
    }

    @Override
    public ByteBuffer getMinTerm()
    {
        byte[] bytes = ByteSourceInverse.readBytes(minTerm.asPeekableBytes(ByteComparable.Version.OSS41));
        return ByteBuffer.wrap(bytes);
    }

    @Override
    public ByteBuffer getMaxTerm()
    {
        byte[] bytes = ByteSourceInverse.readBytes(maxTerm.asPeekableBytes(ByteComparable.Version.OSS41));
        return ByteBuffer.wrap(bytes);
    }

    @Override
    public void close() throws IOException
    {
        for (TermsIterator iterator : iterators)
            iterator.close();
    }

    @Override
    public boolean hasNext()
    {
        return mergedIterator.hasNext();
    }

    @Override
    @SuppressWarnings("resource")
    public PostingList postings() throws IOException
    {
        final PriorityQueue<PostingList.PeekablePostingList> postingLists = new PriorityQueue<>(100, Comparator.comparingLong(PostingList.PeekablePostingList::peek));
        for (int x = 0; x < mergedIterator.getNumTop(); x++)
        {
            final int index = mergedIterator.top[x].index;
            final TermsIterator termsIterator = iterators[index];
            final PostingList postings = termsIterator.postings();

            postingLists.add(postings.peekable());
        }
        return new MonitoringPostingList(MergePostingList.merge(postingLists));
    }

    @Override
    public ByteComparable next()
    {
        ByteComparable nextTerm = mergedIterator.next();
        minTerm = type.isReversed() ? TypeUtil.max(nextTerm, minTerm) : TypeUtil.min(nextTerm, minTerm);
        maxTerm = type.isReversed() ? TypeUtil.min(nextTerm, maxTerm) : TypeUtil.max(nextTerm, minTerm);

        return nextTerm;
    }

    private class MonitoringPostingList implements PostingList
    {
        private final PostingList monitored;

        private MonitoringPostingList(PostingList monitored)
        {
            this.monitored = monitored;
        }

        @Override
        public long nextPosting() throws IOException
        {
            long next = monitored.nextPosting();
            if (next != PostingList.END_OF_STREAM)
            {
                minSSTableRowId = Math.min(minSSTableRowId, next);
                maxSSTableRowId = Math.max(maxSSTableRowId, next);
            }
            return next;
        }

        @Override
        public long size()
        {
            return monitored.size();
        }

        @Override
        public long advance(long targetRowID) throws IOException
        {
            return monitored.advance(targetRowID);
        }

        @Override
        public void close() throws IOException
        {
            monitored.close();
        }
    }
}
