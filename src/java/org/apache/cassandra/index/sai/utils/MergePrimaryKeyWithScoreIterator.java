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

package org.apache.cassandra.index.sai.utils;

import java.util.Collection;
import java.util.PriorityQueue;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

import org.apache.cassandra.index.sai.disk.v1.vector.PrimaryKeyWithScore;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.CloseableIterator;

// TODO: this implementation is sub-optimal due to the combination of PriorityQueue poll/add. A non reducing version of
//  the MergeIterator would be better
public class MergePrimaryKeyWithScoreIterator extends AbstractIterator<PrimaryKeyWithScore>
{
    private final PriorityQueue<PeekingIterator<PrimaryKeyWithScore>> queue;
    private final Collection<CloseableIterator<PrimaryKeyWithScore>> iteratorsToClose;

    public MergePrimaryKeyWithScoreIterator(Collection<CloseableIterator<PrimaryKeyWithScore>> iterators)
    {
        assert !iterators.isEmpty();
        iteratorsToClose = iterators;
        queue = new PriorityQueue<>(iterators.size(), (a, b) -> a.peek().compareTo(b.peek()));
        for (CloseableIterator<PrimaryKeyWithScore> iterator : iterators)
        {
            if (iterator.hasNext())
                queue.add(Iterators.peekingIterator(iterator));
        }
    }

    @Override
    protected PrimaryKeyWithScore computeNext()
    {
        if (queue.isEmpty())
            return endOfData();

        PeekingIterator<PrimaryKeyWithScore> iterator = queue.poll();
        PrimaryKeyWithScore next = iterator.next();
        if (iterator.hasNext())
            queue.add(iterator);
        return next;
    }

    @Override
    public void close()
    {
        for (CloseableIterator<PrimaryKeyWithScore> iterator : iteratorsToClose)
            FileUtils.closeQuietly(iterator);
    }
}
