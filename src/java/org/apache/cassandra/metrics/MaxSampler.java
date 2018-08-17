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
package org.apache.cassandra.metrics;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.google.common.collect.MinMaxPriorityQueue;

public abstract class MaxSampler<T> extends Sampler<T>
{
    private int capacity;
    private MinMaxPriorityQueue<Sample<T>> queue;
    private long endTimeMillis = -1;
    private final Comparator<Sample<T>> comp = Collections.reverseOrder(Comparator.comparing(p -> p.count));

    public boolean isEnabled()
    {
        return endTimeMillis != -1 && clock.currentTimeMillis() <= endTimeMillis;
    }

    public synchronized void beginSampling(int capacity, int durationMillis)
    {
        if (endTimeMillis == -1 || clock.currentTimeMillis() > endTimeMillis)
        {
            endTimeMillis = clock.currentTimeMillis() + durationMillis;
            queue = MinMaxPriorityQueue
                    .orderedBy(comp)
                    .maximumSize(Math.max(1, capacity))
                    .create();
            this.capacity = capacity;
        }
        else
            throw new RuntimeException("Sampling already in progress");
    }

    public synchronized List<Sample<T>> finishSampling(int count)
    {
        List<Sample<T>> result = new ArrayList<>(count);
        if (endTimeMillis != -1)
        {
            endTimeMillis = -1;
            Sample<T> next;
            while ((next = queue.poll()) != null && result.size() <= count)
                result.add(next);
        }
        return result;
    }

    @Override
    protected synchronized void insert(T item, long value)
    {
        if (value > 0 && clock.currentTimeMillis() <= endTimeMillis
                && (queue.isEmpty() || queue.size() < capacity || queue.peekLast().count < value))
            queue.add(new Sample<T>(item, value, 0));
    }

}
