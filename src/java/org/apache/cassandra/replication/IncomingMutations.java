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
package org.apache.cassandra.replication;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.Clock;
import org.jctools.maps.NonBlockingHashMap;

/**
 * Listeners for incoming mutations requested by e.g. {@link org.apache.cassandra.service.reads.tracked.TrackedLocalReads}.
 * </p>
 * The mutations being requested may not necessarily have a corresponding {@link CoordinatorLog}
 * instance just yet - e.g. it's a mutation from a fresh coordinator log that we are requesting,
 * and we were only made aware of it via missing offsets in read reconciliation.
 * </p>
 * For the above reason it's not folded into an instance {@link CoordinatorLog} but instead lives
 * inside {@link MutationTrackingService}.
 */
public class IncomingMutations implements ExpiredStatePurger.Expireable
{
    private static final Logger logger = LoggerFactory.getLogger(IncomingMutations.class);

    private final NonBlockingHashMap<ShortMutationId, Listeners> listenersMap = new NonBlockingHashMap<>();

    /**
     * Register to be notified to an incoming mutation.
     * @return true if this is the first active listener added for this id
     */
    public boolean subscribe(ShortMutationId mutationId, Callback callback)
    {
        Listeners prev, next;
        do
        {
            prev = listenersMap.get(mutationId);
            next = Listeners.addListener(prev, callback);
        }
        while (listenersMap.putIfMatchAllowNull(mutationId, next, prev) != prev);
        return prev == null;
    }

    public void invokeListeners(ShortMutationId mutationId)
    {
        Listeners listeners = listenersMap.remove(mutationId);
        if (listeners != null) listeners.invokeListeners(mutationId);
    }

    @Override
    public int expire(long nanoTime)
    {
        int n = 0;
        for (Map.Entry<ShortMutationId, Listeners> entry : listenersMap.entrySet())
        {
            ShortMutationId id = entry.getKey();
            Listeners listeners = entry.getValue();
            if (listeners.isExpired(nanoTime) && listenersMap.remove(id, listeners))
            {
                listeners.expireListeners(id);
                n++;
            }
        }
        return n;
    }

    private static final class Listeners
    {
        private final long createdAt;
        private final Callback[] callbacks;

        Listeners(Callback callback)
        {
            this(new Callback[] { callback }, Clock.Global.nanoTime());
        }

        Listeners(Callback[] callbacks, long createdAt)
        {
            this.callbacks = callbacks;
            this.createdAt = createdAt;
        }

        Listeners addListener(Callback callback)
        {
            Callback[] newCallbacks = new Callback[callbacks.length + 1];
            System.arraycopy(callbacks, 0, newCallbacks, 0, callbacks.length);
            newCallbacks[callbacks.length] = callback;
            return new Listeners(newCallbacks, createdAt);
        }

        static Listeners addListener(Listeners listeners, Callback callback)
        {
            return listeners == null ? new Listeners(callback) : listeners.addListener(callback);
        }

        void invokeListeners(ShortMutationId mutationId)
        {
            for (Callback callback : callbacks)
            {
                try
                {
                    callback.onSuccess(mutationId);
                }
                catch (Throwable e)
                {
                    logger.error("Caught an error while processing onSuccess() callback for {}: {}", e, mutationId);
                }
            }
        }

        void expireListeners(ShortMutationId mutationId)
        {
            for (Callback callback : callbacks)
            {
                try
                {
                    callback.onTimeout(mutationId);
                }
                catch (Throwable e)
                {
                    logger.error("Caught an error while processing onTimeout() callback for {}: {}", e, mutationId);
                }
            }
        }

        boolean isExpired(long nanoTime)
        {
            return createdAt + DatabaseDescriptor.getWriteRpcTimeout(TimeUnit.NANOSECONDS) < nanoTime;
        }
    }

    public interface Callback
    {
        void onSuccess(ShortMutationId mutationId);

        default void onTimeout(ShortMutationId mutationId)
        {
        }
    }
}
