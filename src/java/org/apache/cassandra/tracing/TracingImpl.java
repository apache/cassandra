/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
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
package org.apache.cassandra.tracing;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.utils.WrappedRunnable;


/**
 * A trace session context. Able to track and store trace sessions. A session is usually a user initiated query, and may
 * have multiple local and remote events before it is completed. All events and sessions are stored at keyspace.
 */
class TracingImpl extends Tracing
{
    public void stopSessionImpl()
    {
        final TraceStateImpl state = getStateImpl();
        if (state == null)
            return;

        int elapsed = state.elapsed();
        ByteBuffer sessionId = state.sessionIdBytes;
        int ttl = state.ttl;

        state.executeMutation(TraceKeyspace.makeStopSessionMutation(sessionId, elapsed, ttl));
    }

    public TraceState begin(final String request, final InetAddress client, final Map<String, String> parameters)
    {
        assert isTracing();

        final TraceStateImpl state = getStateImpl();
        assert state != null;

        final long startedAt = System.currentTimeMillis();
        final ByteBuffer sessionId = state.sessionIdBytes;
        final String command = state.traceType.toString();
        final int ttl = state.ttl;

        state.executeMutation(TraceKeyspace.makeStartSessionMutation(sessionId, client, parameters, request, startedAt, command, ttl));
        return state;
    }

    /**
     * Convert the abstract tracing state to its implementation.
     *
     * Expired states are not put in the sessions but the check is for extra safety.
     *
     * @return the state converted to its implementation, or null
     */
    private TraceStateImpl getStateImpl()
    {
        TraceState state = get();
        if (state == null)
            return null;

        if (state instanceof ExpiredTraceState)
        {
            ExpiredTraceState expiredTraceState = (ExpiredTraceState) state;
            state = expiredTraceState.getDelegate();
        }

        if (state instanceof TraceStateImpl)
        {
            return (TraceStateImpl)state;
        }

        assert false : "TracingImpl states should be of type TraceStateImpl";
        return null;
    }

    @Override
    protected TraceState newTraceState(InetAddress coordinator, UUID sessionId, TraceType traceType)
    {
        return new TraceStateImpl(coordinator, sessionId, traceType);
    }

    /**
     * Called from {@link org.apache.cassandra.net.OutboundTcpConnection} for non-local traces (traces
     * that are not initiated by local node == coordinator).
     */
    public void trace(final ByteBuffer sessionId, final String message, final int ttl)
    {
        final String threadName = Thread.currentThread().getName();

        StageManager.getStage(Stage.TRACING).execute(new WrappedRunnable()
        {
            public void runMayThrow()
            {
                TraceStateImpl.mutateWithCatch(TraceKeyspace.makeEventMutation(sessionId, message, -1, threadName, ttl));
            }
        });
    }
}
