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
package org.apache.cassandra.tracing;

import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.exceptions.OverloadedException;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.FutureCombiner;

import static java.util.Collections.singletonList;
import static org.apache.cassandra.db.ConsistencyLevel.ANY;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

/**
 * ThreadLocal state for a tracing session. The presence of an instance of this class as a ThreadLocal denotes that an
 * operation is being traced.
 */
public class TraceStateImpl extends TraceState
{
    private static final Logger logger = LoggerFactory.getLogger(TraceStateImpl.class);

    @VisibleForTesting
    public static int WAIT_FOR_PENDING_EVENTS_TIMEOUT_SECS = CassandraRelevantProperties.WAIT_FOR_TRACING_EVENTS_TIMEOUT_SECS.getInt();

    private final Set<Future<?>> pendingFutures = ConcurrentHashMap.newKeySet();

    public TraceStateImpl(InetAddressAndPort coordinator, TimeUUID sessionId, Tracing.TraceType traceType)
    {
        super(coordinator, sessionId, traceType);
    }

    protected void traceImpl(String message)
    {
        final String threadName = Thread.currentThread().getName();
        final int elapsed = elapsed();

        executeMutation(TraceKeyspace.makeEventMutation(sessionIdBytes, message, elapsed, threadName, ttl));
        if (logger.isTraceEnabled())
            logger.trace("Adding <{}> to trace events", message);
    }

    /**
     * Wait on submitted futures
     */
    protected void waitForPendingEvents()
    {
        if (WAIT_FOR_PENDING_EVENTS_TIMEOUT_SECS <= 0)
            return;

        try
        {
            if (logger.isTraceEnabled())
                logger.trace("Waiting for up to {} seconds for {} trace events to complete",
                             +WAIT_FOR_PENDING_EVENTS_TIMEOUT_SECS, pendingFutures.size());

            FutureCombiner.allOf(Arrays.asList(pendingFutures.toArray(new Future<?>[pendingFutures.size()])))
                          .get(WAIT_FOR_PENDING_EVENTS_TIMEOUT_SECS, TimeUnit.SECONDS);
        }
        catch (TimeoutException ex)
        {
            if (logger.isTraceEnabled())
                logger.trace("Failed to wait for tracing events to complete in {} seconds",
                             WAIT_FOR_PENDING_EVENTS_TIMEOUT_SECS);
        }
        catch (Throwable t)
        {
            JVMStabilityInspector.inspectThrowable(t);
            logger.error("Got exception whilst waiting for tracing events to complete", t);
        }
    }


    void executeMutation(final Mutation mutation)
    {
        Future<Void> fut = Stage.TRACING.executor().submit(() -> mutateWithCatch(mutation), null);
        boolean ret = pendingFutures.add(fut);
        if (!ret)
            logger.warn("Failed to insert pending future, tracing synchronization may not work");
    }

    static void mutateWithCatch(Mutation mutation)
    {
        try
        {
            StorageProxy.mutate(singletonList(mutation), ANY, nanoTime());
        }
        catch (OverloadedException e)
        {
            Tracing.logger.warn("Too many nodes are overloaded to save trace events");
        }
    }

}
