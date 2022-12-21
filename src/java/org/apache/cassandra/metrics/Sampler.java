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

import java.io.PrintStream;
import java.io.Serializable;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;

import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.utils.ExecutorUtils;
import org.apache.cassandra.utils.MonotonicClock;

import com.google.common.annotations.VisibleForTesting;

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;

public abstract class Sampler<T>
{
    private static long DISABLED = -1L;

    private static final BiFunction<SamplerType, SamplingManager.ResultBuilder, SamplingManager.ResultBuilder>
        FrequencySamplerFomatter = (type, resultBuilder) ->
                                   resultBuilder.forType(type, type.description)
                                                .addColumn("Table", "table")
                                                .addColumn("Partition", "value")
                                                .addColumn("Count", "count")
                                                .addColumn("+/-", "error");

    public enum SamplerType
    {
        READS("Frequency of reads by partition", FrequencySamplerFomatter),
        WRITES("Frequency of writes by partition", FrequencySamplerFomatter),
        LOCAL_READ_TIME("Longest read query times", ((samplerType, resultBuilder) ->
                                                     resultBuilder.forType(samplerType, samplerType.description)
                                                                  .addColumn("Query", "value")
                                                                  .addColumn("Microseconds", "count"))),
        READ_ROW_COUNT("Partitions read with the most rows", ((samplerType, resultBuilder) ->
                                                      resultBuilder.forType(samplerType, samplerType.description)
                                                      .addColumn("Table", "table")
                                                      .addColumn("Partition", "value")
                                                      .addColumn("Rows", "count"))),

        READ_TOMBSTONE_COUNT("Partitions read with the most tombstones", ((samplerType, resultBuilder) ->
                                                      resultBuilder.forType(samplerType, samplerType.description)
                                                                   .addColumn("Table", "table")
                                                                   .addColumn("Partition", "value")
                                                                   .addColumn("Tombstones", "count"))),

        READ_SSTABLE_COUNT("Partitions read with the most sstables", ((samplerType, resultBuilder) ->
                                                                      resultBuilder.forType(samplerType, samplerType.description)
                                                                                   .addColumn("Table", "table")
                                                                                   .addColumn("Partition", "value")
                                                                                   .addColumn("SSTables", "count"))),
        WRITE_SIZE("Max mutation size by partition", ((samplerType, resultBuilder) ->
                                                      resultBuilder.forType(samplerType, samplerType.description)
                                                                   .addColumn("Table", "table")
                                                                   .addColumn("Partition", "value")
                                                                   .addColumn("Bytes", "count"))),
        CAS_CONTENTIONS("Frequency of CAS contention by partition", FrequencySamplerFomatter);

        private final String description;
        private final BiFunction<SamplerType, SamplingManager.ResultBuilder, SamplingManager.ResultBuilder> formatter;

        SamplerType(String description, BiFunction<SamplerType, SamplingManager.ResultBuilder, SamplingManager.ResultBuilder> formatter)
        {
            this.description = description;
            this.formatter = formatter;
        }

        void format(SamplingManager.ResultBuilder resultBuilder, PrintStream ps)
        {
            formatter.apply(this, resultBuilder).print(ps);
        }
    }

    @VisibleForTesting
    MonotonicClock clock = MonotonicClock.Global.approxTime;

    @VisibleForTesting
    static final ExecutorPlus samplerExecutor = executorFactory()
            .withJmxInternal()
            .configureSequential("Sampler")
            .withQueueLimit(1000)
            .withRejectedExecutionHandler((runnable, executor) -> MessagingService.instance().metrics.recordSelfDroppedMessage(Verb._SAMPLE))
            .build();

    private long endTimeNanos = -1;

    public void addSample(final T item, final int value)
    {
        if (isEnabled())
            samplerExecutor.submit(() -> insert(item, value));
    }

    protected abstract void insert(T item, long value);

    /**
     * A sampler is enabled between {@link this#beginSampling} and {@link this#finishSampling}
     * @return true if the sampler is enabled.
     */
    public boolean isEnabled()
    {
        return endTimeNanos != DISABLED;
    }

    public void disable()
    {
        endTimeNanos = DISABLED;
    }

    /**
     * @return true if the sampler is active.
     * A sampler is active only if it is enabled and the current time is within the `durationMillis` when beginning sampling.
     */
    public boolean isActive()
    {
        return isEnabled() && clock.now() <= endTimeNanos;
    }

    /**
     * Update the end time for the sampler. Implicitly, calling this method enables the sampler.
     */
    public void updateEndTime(long endTimeMillis)
    {
        this.endTimeNanos = endTimeMillis;
    }

    /**
     * Begin sampling with the configured capacity and duration
     * @param capacity Number of sample items to keep in memory, the lower this is
     *                 the less accurate results are. For best results use value
     *                 close to cardinality, but understand the memory trade offs.
     * @param durationMillis Upperbound duration in milliseconds for sampling. The sampler
     *                       stops accepting new samples after exceeding the duration
     *                       even if {@link #finishSampling(int)}} is not called.
     */
    public abstract void beginSampling(int capacity, long durationMillis);

    /**
     * Stop sampling and return the results
     * @param count The number of the samples requested to retrieve from the sampler
     * @return a list of samples, the size is the minimum of the total samples and {@param count}.
     */
    public abstract List<Sample<T>> finishSampling(int count);

    public abstract String toString(T value);

    /**
     * Represents the ranked items collected during a sample period
     */
    public static class Sample<S> implements Serializable
    {
        public final S value;
        public final long count;
        public final long error;

        public Sample(S value, long count, long error)
        {
            this.value = value;
            this.count = count;
            this.error = error;
        }

        @Override
        public String toString()
        {
            return "Sample [value=" + value + ", count=" + count + ", error=" + error + "]";
        }
    }

    public static void shutdownNowAndWait(long time, TimeUnit units) throws InterruptedException, TimeoutException
    {
        ExecutorUtils.shutdownNowAndWait(time, units, samplerExecutor);
    }
}
