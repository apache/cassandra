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

import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.RateLimiter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.SystemTimeSource;
import org.apache.cassandra.utils.TimeSource;
import org.apache.cassandra.utils.concurrent.IntervalLock;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Back-pressure algorithm based on rate limiting according to the ratio between incoming and outgoing rates, computed
 * over a sliding time window with size equal to write RPC timeout.
 */
public class RateBasedBackPressure implements BackPressureStrategy<RateBasedBackPressureState>
{
    static final String HIGH_RATIO = "high_ratio";
    static final String FACTOR = "factor";
    static final String FLOW = "flow";
    private static final String BACK_PRESSURE_HIGH_RATIO = "0.90";
    private static final String BACK_PRESSURE_FACTOR = "5";
    private static final String BACK_PRESSURE_FLOW = "FAST";

    private static final Logger logger = LoggerFactory.getLogger(RateBasedBackPressure.class);
    private static final NoSpamLogger tenSecsNoSpamLogger = NoSpamLogger.getLogger(logger, 10, TimeUnit.SECONDS);
    private static final NoSpamLogger oneMinNoSpamLogger = NoSpamLogger.getLogger(logger, 1, TimeUnit.MINUTES);

    protected final TimeSource timeSource;
    protected final double highRatio;
    protected final int factor;
    protected final Flow flow;
    protected final long windowSize;

    private final Cache<Set<RateBasedBackPressureState>, IntervalRateLimiter> rateLimiters =
            Caffeine.newBuilder()
                    .expireAfterAccess(1, TimeUnit.HOURS)
                    .executor(MoreExecutors.directExecutor())
                    .build();

    enum Flow
    {
        FAST,
        SLOW
    }

    public static ParameterizedClass withDefaultParams()
    {
        return new ParameterizedClass(RateBasedBackPressure.class.getName(),
                                      ImmutableMap.of(HIGH_RATIO, BACK_PRESSURE_HIGH_RATIO,
                                                      FACTOR, BACK_PRESSURE_FACTOR,
                                                      FLOW, BACK_PRESSURE_FLOW));
    }

    public RateBasedBackPressure(Map<String, Object> args)
    {
        this(args, new SystemTimeSource(), DatabaseDescriptor.getWriteRpcTimeout(MILLISECONDS));
    }

    @VisibleForTesting
    public RateBasedBackPressure(Map<String, Object> args, TimeSource timeSource, long windowSize)
    {
        if (args.size() != 3)
        {
            throw new IllegalArgumentException(RateBasedBackPressure.class.getCanonicalName()
                    + " requires 3 arguments: high ratio, back-pressure factor and flow type.");
        }

        try
        {
            highRatio = Double.parseDouble(args.getOrDefault(HIGH_RATIO, "").toString().trim());
            factor = Integer.parseInt(args.getOrDefault(FACTOR, "").toString().trim());
            flow = Flow.valueOf(args.getOrDefault(FLOW, "").toString().trim().toUpperCase());
        }
        catch (Exception ex)
        {
            throw new IllegalArgumentException(ex.getMessage(), ex);
        }

        if (highRatio <= 0 || highRatio > 1)
        {
            throw new IllegalArgumentException("Back-pressure high ratio must be > 0 and <= 1");
        }
        if (factor < 1)
        {
            throw new IllegalArgumentException("Back-pressure factor must be >= 1");
        }
        if (windowSize < 10)
        {
            throw new IllegalArgumentException("Back-pressure window size must be >= 10");
        }

        this.timeSource = timeSource;
        this.windowSize = windowSize;

        logger.info("Initialized back-pressure with high ratio: {}, factor: {}, flow: {}, window size: {}.",
                    highRatio, factor, flow, windowSize);
    }

    @Override
    public void apply(Set<RateBasedBackPressureState> states, long timeout, TimeUnit unit)
    {
        // Go through the back-pressure states, try updating each of them and collect min/max rates:
        boolean isUpdated = false;
        double minRateLimit = Double.POSITIVE_INFINITY;
        double maxRateLimit = Double.NEGATIVE_INFINITY;
        double minIncomingRate = Double.POSITIVE_INFINITY;
        RateLimiter currentMin = null;
        RateLimiter currentMax = null;
        for (RateBasedBackPressureState backPressure : states)
        {
            // Get the incoming/outgoing rates:
            double incomingRate = backPressure.incomingRate.get(TimeUnit.SECONDS);
            double outgoingRate = backPressure.outgoingRate.get(TimeUnit.SECONDS);
            // Compute the min incoming rate:
            if (incomingRate < minIncomingRate)
                minIncomingRate = incomingRate;

            // Try acquiring the interval lock:
            if (backPressure.tryIntervalLock(windowSize))
            {
                // If acquired, proceed updating thi back-pressure state rate limit:
                isUpdated = true;
                try
                {
                    RateLimiter limiter = backPressure.rateLimiter;

                    // If we have sent any outgoing requests during this time window, go ahead with rate limiting
                    // (this is safe against concurrent back-pressure state updates thanks to the rw-locking in
                    // RateBasedBackPressureState):
                    if (outgoingRate > 0)
                    {
                        // Compute the incoming/outgoing ratio:
                        double actualRatio = incomingRate / outgoingRate;

                        // If the ratio is above the high mark, try growing by the back-pressure factor:
                        double limiterRate = limiter.getRate();
                        if (actualRatio >= highRatio)
                        {
                            // Only if the outgoing rate is able to keep up with the rate increase:
                            if (limiterRate <= outgoingRate)
                            {
                                double newRate = limiterRate + ((limiterRate * factor) / 100);
                                if (newRate > 0 && newRate != Double.POSITIVE_INFINITY)
                                {
                                    limiter.setRate(newRate);
                                }
                            }
                        }
                        // If below, set the rate limiter at the incoming rate, decreased by factor:
                        else
                        {
                            // Only if the new rate is actually less than the actual rate:
                            double newRate = incomingRate - ((incomingRate * factor) / 100);
                            if (newRate > 0 && newRate < limiterRate)
                            {
                                limiter.setRate(newRate);
                            }
                        }
                        if (logger.isTraceEnabled())
                        {
                            logger.trace("Back-pressure state for {}: incoming rate {}, outgoing rate {}, ratio {}, rate limiting {}",
                                         backPressure.getHost(), incomingRate, outgoingRate, actualRatio, limiter.getRate());
                        }
                    }
                    // Otherwise reset the rate limiter:
                    else
                    {
                        limiter.setRate(Double.POSITIVE_INFINITY);
                    }

                    // Housekeeping: pruning windows and resetting the last check timestamp!
                    backPressure.incomingRate.prune();
                    backPressure.outgoingRate.prune();
                }
                finally
                {
                    backPressure.releaseIntervalLock();
                }
            }
            if (backPressure.rateLimiter.getRate() <= minRateLimit)
            {
                minRateLimit = backPressure.rateLimiter.getRate();
                currentMin = backPressure.rateLimiter;
            }
            if (backPressure.rateLimiter.getRate() >= maxRateLimit)
            {
                maxRateLimit = backPressure.rateLimiter.getRate();
                currentMax = backPressure.rateLimiter;
            }
        }

        // Now find the rate limiter corresponding to the replica group represented by these back-pressure states:
        if (!states.isEmpty())
        {
            // Get the rate limiter:
            IntervalRateLimiter rateLimiter = rateLimiters.get(states, key -> new IntervalRateLimiter(timeSource));

            // If the back-pressure was updated and we acquire the interval lock for the rate limiter of this group:
            if (isUpdated && rateLimiter.tryIntervalLock(windowSize))
            {
                try
                {
                    // Update the rate limiter value based on the configured flow:
                    if (flow.equals(Flow.FAST))
                        rateLimiter.limiter = currentMax;
                    else
                        rateLimiter.limiter = currentMin;

                    tenSecsNoSpamLogger.info("{} currently applied for remote replicas: {}", rateLimiter.limiter, states);
                }
                finally
                {
                    rateLimiter.releaseIntervalLock();
                }
            }
            // Assigning a single rate limiter per replica group once per window size allows the back-pressure rate
            // limiting to be stable within the group itself.

            // Finally apply the rate limit with a max pause time equal to the provided timeout minus the
            // response time computed from the incoming rate, to reduce the number of client timeouts by taking into
            // account how long it could take to process responses after back-pressure:
            long responseTimeInNanos = (long) (TimeUnit.NANOSECONDS.convert(1, TimeUnit.SECONDS) / minIncomingRate);
            doRateLimit(rateLimiter.limiter, Math.max(0, TimeUnit.NANOSECONDS.convert(timeout, unit) - responseTimeInNanos));
        }
    }

    @Override
    public RateBasedBackPressureState newState(InetAddressAndPort host)
    {
        return new RateBasedBackPressureState(host, timeSource, windowSize);
    }

    @VisibleForTesting
    RateLimiter getRateLimiterForReplicaGroup(Set<RateBasedBackPressureState> states)
    {
        IntervalRateLimiter rateLimiter = rateLimiters.getIfPresent(states);
        return rateLimiter != null ? rateLimiter.limiter : RateLimiter.create(Double.POSITIVE_INFINITY);
    }

    @VisibleForTesting
    boolean doRateLimit(RateLimiter rateLimiter, long timeoutInNanos)
    {
        if (!rateLimiter.tryAcquire(1, timeoutInNanos, TimeUnit.NANOSECONDS))
        {
            timeSource.sleepUninterruptibly(timeoutInNanos, TimeUnit.NANOSECONDS);
            oneMinNoSpamLogger.info("Cannot apply {} due to exceeding write timeout, pausing {} nanoseconds instead.",
                                    rateLimiter, timeoutInNanos);

            return false;
        }

        return true;
    }

    private static class IntervalRateLimiter extends IntervalLock
    {
        public volatile RateLimiter limiter = RateLimiter.create(Double.POSITIVE_INFINITY);

        IntervalRateLimiter(TimeSource timeSource)
        {
            super(timeSource);
        }
    }
}
