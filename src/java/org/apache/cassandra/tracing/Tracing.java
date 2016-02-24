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
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ExecutorLocal;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.UUIDGen;


/**
 * A trace session context. Able to track and store trace sessions. A session is usually a user initiated query, and may
 * have multiple local and remote events before it is completed.
 */
public abstract class Tracing implements ExecutorLocal<TraceState>
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

    protected static final Logger logger = LoggerFactory.getLogger(Tracing.class);

    private final InetAddress localAddress = FBUtilities.getLocalAddress();

    private final ThreadLocal<TraceState> state = new ThreadLocal<>();

    protected final ConcurrentMap<UUID, TraceState> sessions = new ConcurrentHashMap<>();

    public static final Tracing instance;

    static {
        Tracing tracing = null;
        String customTracingClass = System.getProperty("cassandra.custom_tracing_class");
        if (null != customTracingClass)
        {
            try
            {
                tracing = FBUtilities.construct(customTracingClass, "Tracing");
                logger.info("Using {} as tracing queries (as requested with -Dcassandra.custom_tracing_class)", customTracingClass);
            }
            catch (Exception e)
            {
                JVMStabilityInspector.inspectThrowable(e);
                logger.error("Cannot use class {} for tracing ({}), ignoring by defaulting on normal tracing", customTracingClass, e.getMessage());
            }
        }
        instance = null != tracing ? tracing : new TracingImpl();
    }

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
        return instance.get() != null;
    }

    public UUID newSession(Map<String,ByteBuffer> customPayload)
    {
        return newSession(TraceType.QUERY);
    }

    public UUID newSession(TraceType traceType)
    {
        return newSession(
                TimeUUIDType.instance.compose(ByteBuffer.wrap(UUIDGen.getTimeUUIDBytes())),
                traceType,
                Collections.EMPTY_MAP);
    }

    public UUID newSession(UUID sessionId, Map<String,ByteBuffer> customPayload)
    {
        return newSession(sessionId, TraceType.QUERY, Collections.EMPTY_MAP);
    }

    protected UUID newSession(UUID sessionId, TraceType traceType, Map<String,ByteBuffer> customPayload)
    {
        assert get() == null;

        TraceState ts = newTraceState(localAddress, sessionId, traceType);
        set(ts);
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
        TraceState state = get();
        if (state == null) // inline isTracing to avoid implicit two calls to state.get()
        {
            logger.trace("request complete");
        }
        else
        {
            stopSessionImpl();

            state.stop();
            sessions.remove(state.sessionId);
            set(null);
        }
    }

    protected abstract void stopSessionImpl();

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

    public abstract TraceState begin(String request, InetAddress client, Map<String, String> parameters);

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
        TraceState ts = get(sessionId);
        if (ts != null && ts.acquireReference())
            return ts;

        byte[] tmpBytes;
        TraceType traceType = TraceType.QUERY;
        if ((tmpBytes = message.parameters.get(TRACE_TYPE)) != null)
            traceType = TraceType.deserialize(tmpBytes[0]);

        if (message.verb == MessagingService.Verb.REQUEST_RESPONSE)
        {
            // received a message for a session we've already closed out.  see CASSANDRA-5668
            return new ExpiredTraceState(newTraceState(message.from, sessionId, traceType));
        }
        else
        {
            ts = newTraceState(message.from, sessionId, traceType);
            sessions.put(sessionId, ts);
            return ts;
        }
    }

    public Map<String, byte[]> getTraceHeaders()
    {
        assert isTracing();

        return ImmutableMap.of(
                TRACE_HEADER, UUIDGen.decompose(Tracing.instance.getSessionId()),
                TRACE_TYPE, new byte[] { Tracing.TraceType.serialize(Tracing.instance.getTraceType()) });
    }

    protected abstract TraceState newTraceState(InetAddress coordinator, UUID sessionId, Tracing.TraceType traceType);

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

    public static void trace(String format, Object... args)
    {
        final TraceState state = instance.get();
        if (state == null) // inline isTracing to avoid implicit two calls to state.get()
            return;

        state.trace(format, args);
    }

    /**
     * Called from {@link org.apache.cassandra.net.OutboundTcpConnection} for non-local traces (traces
     * that are not initiated by local node == coordinator).
     */
    public abstract void trace(ByteBuffer sessionId, String message, int ttl);
}
