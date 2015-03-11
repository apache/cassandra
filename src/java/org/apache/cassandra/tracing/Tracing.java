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
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.exceptions.OverloadedException;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;


/**
 * A trace session context. Able to track and store trace sessions. A session is usually a user initiated query, and may
 * have multiple local and remote events before it is completed. All events and sessions are stored at keyspace.
 */
public class Tracing
{
    public static final String TRACE_HEADER = "TraceSession";
    public static final String TRACE_TYPE = "TraceType";

    public enum TraceType
    {
        NONE,
        QUERY,
        REPAIR;

        private static final TraceType[] ALL_VALUES = values();

        public static TraceType deserialize(byte b)
        {
            if (b < 0 || ALL_VALUES.length <= b)
                return NONE;
            return ALL_VALUES[b];
        }

        public static byte serialize(TraceType value)
        {
            return (byte) value.ordinal();
        }

        private static final int[] TTLS = { DatabaseDescriptor.getTracetypeQueryTTL(),
                                            DatabaseDescriptor.getTracetypeQueryTTL(),
                                            DatabaseDescriptor.getTracetypeRepairTTL() };

        public int getTTL()
        {
            return TTLS[ordinal()];
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(Tracing.class);

    private final InetAddress localAddress = FBUtilities.getLocalAddress();

    private final ThreadLocal<TraceState> state = new ThreadLocal<>();

    private final ConcurrentMap<UUID, TraceState> sessions = new ConcurrentHashMap<>();

    public static final Tracing instance = new Tracing();

    public UUID getSessionId()
    {
        assert isTracing();
        return state.get().sessionId;
    }

    public TraceType getTraceType()
    {
        assert isTracing();
        return state.get().traceType;
    }

    public int getTTL()
    {
        assert isTracing();
        return state.get().ttl;
    }

    /**
     * Indicates if the current thread's execution is being traced.
     */
    public static boolean isTracing()
    {
        return instance.state.get() != null;
    }

    public UUID newSession()
    {
        return newSession(TraceType.QUERY);
    }

    public UUID newSession(TraceType traceType)
    {
        return newSession(TimeUUIDType.instance.compose(ByteBuffer.wrap(UUIDGen.getTimeUUIDBytes())), traceType);
    }

    public UUID newSession(UUID sessionId)
    {
        return newSession(sessionId, TraceType.QUERY);
    }

    public UUID newSession(UUID sessionId, TraceType traceType)
    {
        assert state.get() == null;

        TraceState ts = new TraceState(localAddress, sessionId, traceType);
        state.set(ts);
        sessions.put(sessionId, ts);

        return sessionId;
    }

    public void doneWithNonLocalSession(TraceState state)
    {
        if (state.releaseReference() == 0)
            sessions.remove(state.sessionId);
    }

    /**
     * Stop the session and record its complete.  Called by coodinator when request is complete.
     */
    public void stopSession()
    {
        TraceState state = this.state.get();
        if (state == null) // inline isTracing to avoid implicit two calls to state.get()
        {
            logger.debug("request complete");
        }
        else
        {
            final int elapsed = state.elapsed();
            final ByteBuffer sessionId = state.sessionIdBytes;
            final int ttl = state.ttl;

            StageManager.getStage(Stage.TRACING).execute(new Runnable()
            {
                public void run()
                {
                    mutateWithCatch(TraceKeyspace.makeStopSessionMutation(sessionId, elapsed, ttl));
                }
            });

            state.stop();
            sessions.remove(state.sessionId);
            this.state.set(null);
        }
    }

    public TraceState get()
    {
        return state.get();
    }

    public TraceState get(UUID sessionId)
    {
        return sessions.get(sessionId);
    }

    public void set(final TraceState tls)
    {
        state.set(tls);
    }

    public TraceState begin(final String request, final Map<String, String> parameters)
    {
        return begin(request, null, parameters);
    }

    public TraceState begin(final String request, final InetAddress client, final Map<String, String> parameters)
    {
        assert isTracing();

        final TraceState state = this.state.get();
        final long startedAt = System.currentTimeMillis();
        final ByteBuffer sessionId = state.sessionIdBytes;
        final String command = state.traceType.toString();
        final int ttl = state.ttl;

        StageManager.getStage(Stage.TRACING).execute(new Runnable()
        {
            public void run()
            {
                mutateWithCatch(TraceKeyspace.makeStartSessionMutation(sessionId, client, parameters, request, startedAt, command, ttl));
            }
        });

        return state;
    }

    /**
     * Determines the tracing context from a message.  Does NOT set the threadlocal state.
     * 
     * @param message The internode message
     */
    public TraceState initializeFromMessage(final MessageIn<?> message)
    {
        final byte[] sessionBytes = message.parameters.get(TRACE_HEADER);

        if (sessionBytes == null)
            return null;

        assert sessionBytes.length == 16;
        UUID sessionId = UUIDGen.getUUID(ByteBuffer.wrap(sessionBytes));
        TraceState ts = sessions.get(sessionId);
        if (ts != null && ts.acquireReference())
            return ts;

        byte[] tmpBytes;
        TraceType traceType = TraceType.QUERY;
        if ((tmpBytes = message.parameters.get(TRACE_TYPE)) != null)
            traceType = TraceType.deserialize(tmpBytes[0]);

        if (message.verb == MessagingService.Verb.REQUEST_RESPONSE)
        {
            // received a message for a session we've already closed out.  see CASSANDRA-5668
            return new ExpiredTraceState(sessionId, traceType);
        }
        else
        {
            ts = new TraceState(message.from, sessionId, traceType);
            sessions.put(sessionId, ts);
            return ts;
        }
    }


    // repair just gets a varargs method since it's so heavyweight anyway
    public static void traceRepair(String format, Object... args)
    {
        final TraceState state = instance.get();
        if (state == null) // inline isTracing to avoid implicit two calls to state.get()
            return;

        state.trace(format, args);
    }

    // normal traces get zero-, one-, and two-argument overloads so common case doesn't need to create varargs array
    public static void trace(String message)
    {
        final TraceState state = instance.get();
        if (state == null) // inline isTracing to avoid implicit two calls to state.get()
            return;

        state.trace(message);
    }

    public static void trace(String format, Object arg)
    {
        final TraceState state = instance.get();
        if (state == null) // inline isTracing to avoid implicit two calls to state.get()
            return;

        state.trace(format, arg);
    }

    public static void trace(String format, Object arg1, Object arg2)
    {
        final TraceState state = instance.get();
        if (state == null) // inline isTracing to avoid implicit two calls to state.get()
            return;

        state.trace(format, arg1, arg2);
    }

    public static void trace(String format, Object[] args)
    {
        final TraceState state = instance.get();
        if (state == null) // inline isTracing to avoid implicit two calls to state.get()
            return;

        state.trace(format, args);
    }

    static void mutateWithCatch(Mutation mutation)
    {
        try
        {
            StorageProxy.mutate(Arrays.asList(mutation), ConsistencyLevel.ANY);
        }
        catch (OverloadedException e)
        {
            logger.warn("Too many nodes are overloaded to save trace events");
        }
    }
}
