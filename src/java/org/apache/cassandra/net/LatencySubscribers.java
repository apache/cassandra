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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.apache.cassandra.locator.InetAddressAndPort;

/**
 * Callback that {@link org.apache.cassandra.locator.DynamicEndpointSnitch} listens to in order
 * to update host scores.
 *
 * FIXME: rename/specialise, since only used by DES?
 */
public class LatencySubscribers
{
    public interface Subscriber
    {
        void receiveTiming(InetAddressAndPort address, long latency, TimeUnit unit);
    }

    private volatile Subscriber subscribers;
    private static final AtomicReferenceFieldUpdater<LatencySubscribers, Subscriber> subscribersUpdater
        = AtomicReferenceFieldUpdater.newUpdater(LatencySubscribers.class, Subscriber.class, "subscribers");

    private static Subscriber merge(Subscriber a, Subscriber b)
    {
        if (a == null) return b;
        if (b == null) return a;
        return (address, latency, unit) -> {
            a.receiveTiming(address, latency, unit);
            b.receiveTiming(address, latency, unit);
        };
    }

    public void subscribe(Subscriber subscriber)
    {
        subscribersUpdater.accumulateAndGet(this, subscriber, LatencySubscribers::merge);
    }

    public void add(InetAddressAndPort address, long latency, TimeUnit unit)
    {
        Subscriber subscribers = this.subscribers;
        if (subscribers != null)
            subscribers.receiveTiming(address, latency, unit);
    }

    /**
     * Track latency information for the dynamic snitch
     *
     * @param cb      the callback associated with this message -- this lets us know if it's a message type we're interested in
     * @param address the host that replied to the message
     */
    public void maybeAdd(RequestCallback cb, InetAddressAndPort address, long latency, TimeUnit unit)
    {
        if (cb.trackLatencyForSnitch())
            add(address, latency, unit);
    }
}
