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

import java.util.concurrent.atomic.AtomicLong;

import org.apache.cassandra.utils.ReflectionUtils;

/**
 * This type of Counter is not very efficient for updates but fast to read.
 * It should be used only when getCount performance is critical
 * and we can sucrifice the cost to update it by multiple threads
 */
public class AtomicLongCounter extends com.codahale.metrics.Counter implements Counter
{
    private final AtomicLong counter = new AtomicLong();

    public AtomicLongCounter()
    {
        ReflectionUtils.setFieldToNull(this, "count");
    }

    @Override
    public void inc()
    {
        counter.incrementAndGet();
    }

    @Override
    public void inc(long n)
    {
        counter.addAndGet(n);
    }

    @Override
    public void dec()
    {
        counter.decrementAndGet();
    }

    @Override
    public void dec(long n)
    {
        counter.addAndGet(-n);
    }

    @Override
    public long getCount()
    {
        return counter.get();
    }
}
