/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.utils;

import java.util.concurrent.atomic.AtomicReference;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.RateLimiter;

import org.jboss.byteman.rule.Rule;
import org.jboss.byteman.rule.helper.Helper;

/**
 * Helper class to apply rate limiting during fault injection testing;
 * for an example script, see test/resources/byteman/mutation_limiter.btm.
 */
@VisibleForTesting
public class TestRateLimiter extends Helper
{
    private static final AtomicReference<RateLimiter> ref = new AtomicReference<>();

    protected TestRateLimiter(Rule rule)
    {
        super(rule);
    }

    /**
     * Acquires a single unit at the given rate. If the rate changes between calls, a new rate limiter is created
     * and the old one is discarded.
     */
    public void acquire(double rate)
    {
        RateLimiter limiter = ref.get();
        if (limiter == null || limiter.getRate() != rate)
        {
            ref.compareAndSet(limiter, RateLimiter.create(rate));
            limiter = ref.get();
        }
        limiter.acquire(1);
    }
}
