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
package org.apache.cassandra.net;

import java.net.InetAddress;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.RateLimiter;

import org.apache.cassandra.utils.SlidingTimeRate;
import org.apache.cassandra.utils.TimeSource;
import org.apache.cassandra.utils.concurrent.IntervalLock;

/**
 * The rate-based back-pressure state, tracked per replica host.
 * <br/><br/>
 *
 * This back-pressure state is made up of the following attributes:
 * <ul>
 * <li>windowSize: the length of the back-pressure window in milliseconds.</li>
 * <li>incomingRate: the rate of back-pressure supporting incoming messages.</li>
 * <li>outgoingRate: the rate of back-pressure supporting outgoing messages.</li>
 * <li>rateLimiter: the rate limiter to eventually apply to outgoing messages.</li>
 * </ul>
 * <br/>
 * The incomingRate and outgoingRate are updated together when a response is received to guarantee consistency between
 * the two.
 * <br/>
 * It also provides methods to exclusively lock/release back-pressure windows at given intervals;
 * this allows to apply back-pressure even under concurrent modifications. Please also note a read lock is acquired
 * during response processing so that no concurrent rate updates can screw rate computations.
 */
class RateBasedBackPressureState extends IntervalLock implements BackPressureState
{
    private final InetAddress host;
    private final long windowSize;
    final SlidingTimeRate incomingRate;
    final SlidingTimeRate outgoingRate;
    final RateLimiter rateLimiter;

    RateBasedBackPressureState(InetAddress host, TimeSource timeSource, long windowSize)
    {
        super(timeSource);
        this.host = host;
        this.windowSize = windowSize;
        this.incomingRate = new SlidingTimeRate(timeSource, this.windowSize, this.windowSize / 10, TimeUnit.MILLISECONDS);
        this.outgoingRate = new SlidingTimeRate(timeSource, this.windowSize, this.windowSize / 10, TimeUnit.MILLISECONDS);
        this.rateLimiter = RateLimiter.create(Double.POSITIVE_INFINITY);
    }

    @Override
    public void onMessageSent(MessageOut<?> message) {}

    @Override
    public void onResponseReceived()
    {
        readLock().lock();
        try
        {
            incomingRate.update(1);
            outgoingRate.update(1);
        }
        finally
        {
            readLock().unlock();
        }
    }

    @Override
    public void onResponseTimeout()
    {
        readLock().lock();
        try
        {
            outgoingRate.update(1);
        }
        finally
        {
            readLock().unlock();
        }
    }

    @Override
    public double getBackPressureRateLimit()
    {
        return rateLimiter.getRate();
    }

    @Override
    public InetAddress getHost()
    {
        return host;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof RateBasedBackPressureState)
        {
            RateBasedBackPressureState other = (RateBasedBackPressureState) obj;
            return this.host.equals(other.host);
        }
        return false;
    }

    @Override
    public int hashCode()
    {
        return this.host.hashCode();
    }

    @Override
    public String toString()
    {
        return String.format("[host: %s, incoming rate: %.3f, outgoing rate: %.3f, rate limit: %.3f]",
                             host, incomingRate.get(TimeUnit.SECONDS), outgoingRate.get(TimeUnit.SECONDS), rateLimiter.getRate());
    }
}
