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

package org.apache.cassandra.transport;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.config.DatabaseDescriptor;

import static org.apache.cassandra.utils.MonotonicClock.Global.preciseTime;

/**
 * Native Queue Backpressure mechanism. If the queue fills up above {@link DatabaseDescriptor#getNativeTransportQueueMaxItemAgeThreshold()}.
 * In other words, request has been sitting more than a % of {@link DatabaseDescriptor#getNativeTransportQueueMaxItemAgeThreshold()} in the queue,
 * we start an incident.
 * <p>
 * Incident starts by marking the incident time and raising severity level to 1. Each time we observe an old item in the head of
 * the queue, we first bump then number of times we have applied the backpressure. After bumping it 10 times, we increase severity level by 1.
 * <p>
 * Backpressure delay applied to the client socket is computed by multiplying the severity level by the minimum delay.
 * <p>
 * If we have not seen old requests in the head of the queue for 1 second, we close the incident.
 * <p>
 * If the queue remains saturated for a prolonged period (meaning {@link Dispatcher#hasQueueCapacity returns false), the amount of delay
 * will increase in proportion to the request rate as appliedTimes & severityLevel are incremented. If no new requests are considered
 * overloaded in this way for a second, the incident will be reset and so the delay will drop back down to {@link Impl#minDelayNanos()}.
 */
public interface QueueBackpressure
{
    QueueBackpressure NO_OP = timeUnit -> 0;

    QueueBackpressure DEFAULT = new QueueBackpressure()
    {
        private final AtomicReference<Impl> state = new AtomicReference<>(noBackpressure(() -> DatabaseDescriptor.getNativeTransportMinBackoffOnQueueOverload(TimeUnit.NANOSECONDS),
                                                                                         () -> DatabaseDescriptor.getNativeTransportMaxBackoffOnQueueOverload(TimeUnit.NANOSECONDS)));

        public long markAndGetDelay(TimeUnit timeUnit)
        {
            return state.updateAndGet(Impl::mark).delay(timeUnit);
        }
    };

    long markAndGetDelay(TimeUnit timeUnit);


    static Impl noBackpressure(LongSupplier minDelayNanos, LongSupplier maxDelayNanos)
    {
        return new Impl(minDelayNanos, maxDelayNanos,
                        -1, 0, 0);
    }

    @VisibleForTesting
    class Impl
    {
        private final long appliedAt;

        private final int severityLevel;
        private final int appliedTimes;

        private final LongSupplier minDelayNanos;
        private final LongSupplier maxDelayNanos;

        @VisibleForTesting
        public Impl(LongSupplier minDelayNanos, LongSupplier maxDelayNanos, long appliedAt, int severityLevel, int appliedTimes)
        {
            this.minDelayNanos = minDelayNanos;
            this.maxDelayNanos = maxDelayNanos;
            this.appliedAt = appliedAt;
            this.severityLevel = severityLevel;
            this.appliedTimes = appliedTimes;
        }


        public Impl mark()
        {
            return mark(preciseTime.now());
        }

        @VisibleForTesting
        public Impl mark(long now)
        {
            // Last time we have applied backpressure was over a second ago, consider this a new incident
            if (appliedAt > 0 && now - appliedAt >= TimeUnit.SECONDS.toNanos(1))
            {
                return new Impl(minDelayNanos, maxDelayNanos, now, 1, 1);
            }
            // Continuing incident: apply backpressure but do not bump severity level yet
            else if (appliedTimes < 10)
            {
                return new Impl(minDelayNanos, maxDelayNanos, now, severityLevel == 0 ? 1 : severityLevel, appliedTimes + 1);
            }
            //
            else
            {
                return new Impl(minDelayNanos, maxDelayNanos, now, severityLevel + 1, 1);
            }
        }

        public long appliedAt()
        {
            return appliedAt;
        }

        public long delay(TimeUnit timeUnit)
        {
            return timeUnit.convert(Math.min(maxDelayNanos(), severityLevel * minDelayNanos()), TimeUnit.NANOSECONDS);
        }

        public long minDelayNanos()
        {
            return minDelayNanos.getAsLong();
        }

        public long maxDelayNanos()
        {
            return maxDelayNanos.getAsLong();
        }

        public String toString()
        {
            return "QueueBackpressure{" +
                   "appliedAgo=" + (appliedAt == -1 ? "never" : TimeUnit.NANOSECONDS.toMillis(preciseTime.now() - appliedAt)) +
                   ", severityLevel=" + severityLevel +
                   ", appliedTimes=" + appliedTimes +
                   ", currentDelay=" + delay(TimeUnit.MILLISECONDS) + "ms" +
                   '}';
        }
    }
}