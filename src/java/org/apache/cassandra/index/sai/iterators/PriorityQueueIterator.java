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

package org.apache.cassandra.index.sai.iterators;

import java.util.PriorityQueue;

import org.apache.cassandra.utils.AbstractIterator;

/**
 * An iterator over a priority queue.
 * @param <T> the type of the elements in the priority queue
 */
public class PriorityQueueIterator<T> extends AbstractIterator<T>
{
    private final PriorityQueue<T> queue;

    /**
     * Build a PriorityQueueIterator.
     * @param queue a priority queue to be lazily consumed by the iterator
     */
    public PriorityQueueIterator(PriorityQueue<T> queue)
    {
        this.queue = queue;
    }

    @Override
    protected T computeNext()
    {
        return queue.isEmpty() ? endOfData() : queue.poll();
    }
}
