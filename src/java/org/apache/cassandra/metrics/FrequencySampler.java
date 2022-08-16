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

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clearspring.analytics.stream.StreamSummary;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Find the most frequent sample. A sample adds to the sum of its key ie
 * <p>add("x", 10); and add("x", 20); will result in "x" = 30</p> This uses StreamSummary to only store the
 * approximate cardinality (capacity) of keys. If the number of distinct keys exceed the capacity, the error of the
 * sample may increase depending on distribution of keys among the total set.
 *
 * Note: {@link Sampler#samplerExecutor} is single threaded but we still need to synchronize as we have access
 * from both internal and the external JMX context that can cause races.
 * 
 * @param <T>
 */
public abstract class FrequencySampler<T> extends Sampler<T>
{
    private static final Logger logger = LoggerFactory.getLogger(FrequencySampler.class);

    private StreamSummary<T> summary;

    /**
     * Start to record samples
     *
     * @param capacity Number of sample items to keep in memory, the lower this is
     *                 the less accurate results are. For best results use value
     *                 close to cardinality, but understand the memory trade offs.
     */
    public synchronized void beginSampling(int capacity, long durationMillis)
    {
        if (isActive())
            throw new RuntimeException("Sampling already in progress");
        updateEndTime(clock.now() + MILLISECONDS.toNanos(durationMillis));
        summary = new StreamSummary<>(capacity);
    }

    /**
     * Call to stop collecting samples, and gather the results
     * @param count Number of most frequent items to return
     */
    public synchronized List<Sample<T>> finishSampling(int count)
    {
        List<Sample<T>> results = Collections.emptyList();
        if (isEnabled())
        {
            disable();
            results = summary.topK(count)
                             .stream()
                             .map(c -> new Sample<>(c.getItem(), c.getCount(), c.getError()))
                             .collect(Collectors.toList());
        }
        return results;
    }

    protected synchronized void insert(final T item, final long value)
    {
        if (value > 0 && isActive())
        {
            try
            {
                summary.offer(item, (int) Math.min(value, Integer.MAX_VALUE));
            }
            catch (Exception e)
            {
                logger.trace("Failure to offer sample", e);
            }
        }
    }
}
