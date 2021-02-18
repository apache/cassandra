/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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


import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.lucene.util.PriorityQueue;

public final class MergingIterator implements Iterator<ByteComparable>
{
    private ByteComparable current;
    private final TermMergeQueue queue;
    final SubIterator[] top;
    private final boolean removeDuplicates = true;
    private int numTop;

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public MergingIterator(AbstractType type, Iterator<ByteComparable>... iterators)
    {
        queue = new TermMergeQueue(iterators.length, type);
        top = new SubIterator[iterators.length];
        int index = 0;
        for (Iterator<ByteComparable> iterator : iterators)
        {
            if (iterator.hasNext())
            {
                SubIterator sub = new SubIterator();
                sub.current = iterator.next();
                sub.iterator = iterator;
                sub.index = index++;
                queue.add(sub);
            }
        }
    }

    @Override
    public boolean hasNext()
    {
        if (queue.size() > 0)
        {
            return true;
        }

        for (int i = 0; i < numTop; i++)
        {
            if (top[i].iterator.hasNext())
            {
                return true;
            }
        }
        return false;
    }

    public int getNumTop()
    {
        return numTop;
    }

    @Override
    public ByteComparable next()
    {
        // restore queue
        pushTop();

        // gather equal top elements
        if (queue.size() > 0)
        {
            pullTop();
        }
        else
        {
            current = null;
        }
        if (current == null)
        {
            throw new NoSuchElementException();
        }
        return current;
    }

    @Override
    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    private void pullTop()
    {
        assert numTop == 0;
        top[numTop++] = queue.pop();
        if (removeDuplicates)
        {
            // extract all subs from the queue that have the same top element
            while (queue.size() != 0
                   && ByteComparable.compare(queue.top().current, top[0].current, ByteComparable.Version.OSS41) == 0)
            {
                top[numTop++] = queue.pop();
            }
        }
        current = top[0].current;
    }

    private void pushTop()
    {
        // call next() on each top, and put back into queue
        for (int i = 0; i < numTop; i++)
        {
            if (top[i].iterator.hasNext())
            {
                top[i].current = top[i].iterator.next();
                queue.add(top[i]);
            }
            else
            {
                // no more elements
                top[i].current = null;
            }
        }
        numTop = 0;
    }

    public static class SubIterator
    {
        Iterator<ByteComparable> iterator;
        ByteComparable current;
        int index;
    }

    private static class TermMergeQueue extends PriorityQueue<SubIterator>
    {
        final AbstractType type;

        TermMergeQueue(int size, AbstractType type)
        {
            super(size);
            this.type = type;
        }

        @Override
        protected boolean lessThan(SubIterator a, SubIterator b)
        {
            final int cmp = ByteComparable.compare(a.current, b.current, ByteComparable.Version.OSS41);

            if (cmp != 0)
            {
                return cmp < 0;
            }
            else
            {
                return a.index < b.index;
            }
        }
    }
}
