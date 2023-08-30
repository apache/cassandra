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

package org.apache.cassandra.hints;

import java.util.UUID;

import com.google.common.util.concurrent.RateLimiter;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.service.StorageService;

import static org.apache.cassandra.config.CassandraRelevantProperties.CUSTOM_HINTS_RATE_LIMITER_FACTORY;

/**
 * The factory for creating {@link RateLimiter} for every hints dispatch task
 */
public interface HintsRateLimiterFactory
{

    HintsRateLimiterFactory instance = CUSTOM_HINTS_RATE_LIMITER_FACTORY.isPresent() ?
                                       make(CUSTOM_HINTS_RATE_LIMITER_FACTORY.getString()):
                                       new DefaultHintsRateLimiterFactory();

    /**
     * return {@link RateLimiter} for current dispatch task
     */
    RateLimiter create(UUID hostId);

    class DefaultHintsRateLimiterFactory implements HintsRateLimiterFactory
    {
        DefaultHintsRateLimiterFactory()
        {
        }

        @Override
        public RateLimiter create(UUID hostId)
        {
            // rate limit is in bytes per second. Uses Double.MAX_VALUE if disabled (set to 0 in cassandra.yaml).
            // max rate is scaled by the number of nodes in the cluster (CASSANDRA-5272).
            // the goal is to bound maximum hints traffic going towards a particular node from the rest of the cluster,
            // not total outgoing hints traffic from this node - this is why the rate limiter is not shared between
            // all the dispatch tasks (as there will be at most one dispatch task for a particular host id at a time).
            int nodesCount = Math.max(1, StorageService.instance.getTokenMetadata().getSizeOfAllEndpoints() - 1);
            int throttleInKB = DatabaseDescriptor.getHintedHandoffThrottleInKB() / nodesCount;
            return RateLimiter.create(throttleInKB == 0 ? Double.MAX_VALUE : throttleInKB * 1024);
        }
    }

    static HintsRateLimiterFactory make(String customImpl)
    {
        try
        {
            return (HintsRateLimiterFactory) Class.forName(customImpl).newInstance();
        }
        catch (Throwable ex)
        {
            throw new IllegalStateException("Unknown Hinted Handoff Rate Limiter Factory: " + customImpl);
        }
    }
}
