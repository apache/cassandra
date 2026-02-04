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

import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import com.codahale.metrics.Metered;
import com.codahale.metrics.Sampling;

import org.apache.cassandra.metrics.CassandraReservoir.BucketStrategy;

/**
 * An interface which mimics {@link com.codahale.metrics.Timer} API and allows alternative implementations
 */
public interface Timer extends Metered, Sampling
{
    /**
     * A timing context.
     */
    class Context implements AutoCloseable
    {
        private final Timer timer;
        private final MetricClock clock;
        private final long startTime;

        Context(Timer timer, MetricClock clock)
        {
            this.timer = timer;
            this.clock = clock;
            this.startTime = clock.getTick();
        }

        /**
         * Updates the timer with the difference between current and start time. Call to this method will
         * not reset the start time. Multiple calls result in multiple updates.
         *
         * @return the elapsed time in nanoseconds
         */
        public long stop()
        {
            final long elapsed = clock.getTick() - startTime;
            timer.update(elapsed, clock.getTickUnit());
            return elapsed;
        }

        /**
         * Equivalent to calling {@link #stop()}.
         */
        @Override
        public void close()
        {
            stop();
        }
    }

    BucketStrategy bucketStrategy();
    long[] bucketStarts(int length);
    boolean isCumulative();

    void update(long duration, TimeUnit unit);

    void update(Duration duration);

    <T> T time(Callable<T> event) throws Exception;

    <T> T timeSupplier(Supplier<T> event);

    void time(Runnable event);

    /* we have to implement another method instead of time() due to 2 reasons:
     * 1) com.codahale.metrics.Timer.Context cannot be inhereted - it has only a package-private constructor
     * 2) we want to avoid direct dependency to com.codahale.metrics.Timer.Context in other Cassandra classes
     */
    Context startTime();
}
