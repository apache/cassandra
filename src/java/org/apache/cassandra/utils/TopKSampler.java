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
package org.apache.cassandra.utils;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.*;

import org.apache.cassandra.concurrent.*;
import org.slf4j.*;

import com.clearspring.analytics.stream.*;
import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.google.common.annotations.VisibleForTesting;

public class TopKSampler<T>
{
    private static final Logger logger = LoggerFactory.getLogger(TopKSampler.class);
    private volatile boolean enabled = false;

    @VisibleForTesting
    static final ThreadPoolExecutor samplerExecutor = new JMXEnabledThreadPoolExecutor(1, 1,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<Runnable>(),
            new NamedThreadFactory("Sampler"),
            "internal");

    private StreamSummary<T> summary;
    @VisibleForTesting
    HyperLogLogPlus hll;

    /**
     * Start to record samples
     *
     * @param capacity
     *            Number of sample items to keep in memory, the lower this is
     *            the less accurate results are. For best results use value
     *            close to cardinality, but understand the memory trade offs.
     */
    public synchronized void beginSampling(int capacity)
    {
        if (!enabled)
        {
            summary = new StreamSummary<T>(capacity);
            hll = new HyperLogLogPlus(14);
            enabled = true;
        }
    }

    /**
     * Call to stop collecting samples, and gather the results
     * @param count Number of most frequent items to return
     */
    public synchronized SamplerResult<T> finishSampling(int count)
    {
        List<Counter<T>> results = Collections.EMPTY_LIST;
        long cardinality = 0;
        if (enabled)
        {
            enabled = false;
            results = summary.topK(count);
            cardinality = hll.cardinality();
        }
        return new SamplerResult<T>(results, cardinality);
    }

    public void addSample(T item)
    {
        addSample(item, item.hashCode(), 1);
    }

    /**
     * Adds a sample to statistics collection. This method is non-blocking and will
     * use the "Sampler" thread pool to record results if the sampler is enabled.  If not
     * sampling this is a NOOP
     */
    public void addSample(final T item, final long hash, final int value)
    {
        if (enabled)
        {
            final Object lock = this;
            samplerExecutor.execute(new Runnable()
            {
                public void run()
                {
                    // samplerExecutor is single threaded but still need
                    // synchronization against jmx calls to finishSampling
                    synchronized (lock)
                    {
                        if (enabled)
                        {
                            try
                            {
                                summary.offer(item, value);
                                hll.offerHashed(hash);
                            } catch (Exception e)
                            {
                                logger.trace("Failure to offer sample", e);
                            }
                        }
                    }
                }
            });
        }
    }

    /**
     * Represents the cardinality and the topK ranked items collected during a
     * sample period
     */
    public static class SamplerResult<S> implements Serializable
    {
        public final List<Counter<S>> topK;
        public final long cardinality;

        public SamplerResult(List<Counter<S>> topK, long cardinality)
        {
            this.topK = topK;
            this.cardinality = cardinality;
        }
    }

}

