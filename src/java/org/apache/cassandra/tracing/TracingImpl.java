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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/**
 * A trace session context. Able to track and store trace sessions. A session is usually a user initiated query, and may
 * have multiple local and remote events before it is completed. All events and sessions are stored at keyspace.
 */
class TracingImpl extends Tracing
{
    private static final Logger logger = LoggerFactory.getLogger(TracingImpl.class);

    public void stopSessionImpl() {
        TraceState state = get();
        int elapsed = state.elapsed();
        ByteBuffer sessionId = state.sessionIdBytes;
        int ttl = state.ttl;
        TraceStateImpl.executeMutation(TraceKeyspace.makeStopSessionMutation(sessionId, elapsed, ttl));
    }

    public TraceState begin(final String request, final InetAddress client, final Map<String, String> parameters)
    {
        assert isTracing();

        final TraceState state = get();
        final long startedAt = System.currentTimeMillis();
        final ByteBuffer sessionId = state.sessionIdBytes;
        final String command = state.traceType.toString();
        final int ttl = state.ttl;

        TraceStateImpl.executeMutation(TraceKeyspace.makeStartSessionMutation(sessionId, client, parameters, request, startedAt, command, ttl));

        return state;
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
