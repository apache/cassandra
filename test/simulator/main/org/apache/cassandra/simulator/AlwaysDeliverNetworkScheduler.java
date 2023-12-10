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

package org.apache.cassandra.simulator;

import java.util.concurrent.TimeUnit;

import org.apache.cassandra.simulator.systems.SimulatedTime;

/**
 * Action scheduler that simulates ideal networking conditions. Useful to rule out
 * timeouts in some of the verbs to test only a specific subset of Cassandra.
 */
public class AlwaysDeliverNetworkScheduler implements FutureActionScheduler
{
    private final SimulatedTime time;

    private final long delayNanos;

    public AlwaysDeliverNetworkScheduler(SimulatedTime time)
    {
        this(time, TimeUnit.MILLISECONDS.toNanos(10));
    }
    public AlwaysDeliverNetworkScheduler(SimulatedTime time, long dealayNanos)
    {
        this.time = time;
        this.delayNanos = dealayNanos;
    }
    public Deliver shouldDeliver(int from, int to)
    {
        return Deliver.DELIVER;
    }

    public long messageDeadlineNanos(int from, int to)
    {
        return time.nanoTime() + delayNanos;
    }

    public long messageTimeoutNanos(long expiresAfterNanos, long expirationIntervalNanos)
    {
        return expiresAfterNanos + 1;
    }

    public long messageFailureNanos(int from, int to)
    {
        throw new IllegalStateException();
    }

    public long schedulerDelayNanos()
    {
        return 1;
    }
}
