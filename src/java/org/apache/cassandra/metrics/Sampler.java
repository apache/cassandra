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

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.utils.ExecutorUtils;
import org.apache.cassandra.utils.MonotonicClock;

import com.google.common.annotations.VisibleForTesting;

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;

public abstract class Sampler<T>
{
    public enum SamplerType
    {
        READS, WRITES, LOCAL_READ_TIME, WRITE_SIZE, CAS_CONTENTIONS
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

    public void addSample(final T item, final int value)
    {
        if (isEnabled())
            samplerExecutor.submit(() -> insert(item, value));
    }

    protected abstract void insert(T item, long value);

    public abstract boolean isEnabled();

    public abstract void beginSampling(int capacity, int durationMillis);

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
