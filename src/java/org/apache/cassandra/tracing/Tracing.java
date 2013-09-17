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
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.cql3.ColumnNameBuilder;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.cassandra.utils.WrappedRunnable;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

/**
 * A trace session context. Able to track and store trace sessions. A session is usually a user initiated query, and may
 * have multiple local and remote events before it is completed. All events and sessions are stored at keyspace.
 */
public class Tracing
{
    public static final String TRACE_KS = "system_traces";
    public static final String EVENTS_CF = "events";
    public static final String SESSIONS_CF = "sessions";
    public static final String TRACE_HEADER = "TraceSession";

    private static final int TTL = 24 * 3600;

    private static final Logger logger = LoggerFactory.getLogger(Tracing.class);

    private final InetAddress localAddress = FBUtilities.getLocalAddress();

    private final ThreadLocal<TraceState> state = new ThreadLocal<TraceState>();

    private final ConcurrentMap<UUID, TraceState> sessions = new ConcurrentHashMap<UUID, TraceState>();

    public static final Tracing instance = new Tracing();

    public static void addColumn(ColumnFamily cf, ByteBuffer name, InetAddress address)
    {
        addColumn(cf, name, ByteBufferUtil.bytes(address));
    }

    public static void addColumn(ColumnFamily cf, ByteBuffer name, int value)
    {
        addColumn(cf, name, ByteBufferUtil.bytes(value));
    }

    public static void addColumn(ColumnFamily cf, ByteBuffer name, long value)
    {
        addColumn(cf, name, ByteBufferUtil.bytes(value));
    }

    public static void addColumn(ColumnFamily cf, ByteBuffer name, String value)
    {
        addColumn(cf, name, ByteBufferUtil.bytes(value));
    }

    private static void addColumn(ColumnFamily cf, ByteBuffer name, ByteBuffer value)
    {
        cf.addColumn(new ExpiringColumn(name, value, System.currentTimeMillis(), TTL));
    }

    public void addParameterColumns(ColumnFamily cf, Map<String, String> rawPayload)
    {
        for (Map.Entry<String, String> entry : rawPayload.entrySet())
        {
            cf.addColumn(new ExpiringColumn(buildName(cf.metadata(), bytes("parameters"), bytes(entry.getKey())),
                                            bytes(entry.getValue()), System.currentTimeMillis(), TTL));
        }
    }

    public static ByteBuffer buildName(CFMetaData meta, ByteBuffer... args)
    {
        ColumnNameBuilder builder = meta.getCfDef().getColumnNameBuilder();
        for (ByteBuffer arg : args)
            builder.add(arg);
        return builder.build();
    }

    public UUID getSessionId()
    {
        assert isTracing();
        return state.get().sessionId;
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
        return newSession(TimeUUIDType.instance.compose(ByteBuffer.wrap(UUIDGen.getTimeUUIDBytes())));
    }

    public UUID newSession(UUID sessionId)
    {
        assert state.get() == null;

        TraceState ts = new TraceState(localAddress, sessionId);
        state.set(ts);
        sessions.put(sessionId, ts);

        return sessionId;
    }

    public void stopNonLocal(TraceState state)
    {
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
            final ByteBuffer sessionIdBytes = state.sessionIdBytes;

            StageManager.getStage(Stage.TRACING).execute(new WrappedRunnable()
            {
                public void runMayThrow() throws Exception
                {
                    CFMetaData cfMeta = CFMetaData.TraceSessionsCf;
                    ColumnFamily cf = ArrayBackedSortedColumns.factory.create(cfMeta);
                    addColumn(cf, buildName(cfMeta, bytes("duration")), elapsed);
                    RowMutation mutation = new RowMutation(TRACE_KS, sessionIdBytes, cf);
                    StorageProxy.mutate(Arrays.asList(mutation), ConsistencyLevel.ANY);
                }
            });

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

    public void begin(final String request, final Map<String, String> parameters)
    {
        assert isTracing();

        final long started_at = System.currentTimeMillis();
        final ByteBuffer sessionIdBytes = state.get().sessionIdBytes;

        StageManager.getStage(Stage.TRACING).execute(new WrappedRunnable()
        {
            public void runMayThrow() throws Exception
            {
                CFMetaData cfMeta = CFMetaData.TraceSessionsCf;
                ColumnFamily cf = TreeMapBackedSortedColumns.factory.create(cfMeta);
                addColumn(cf, buildName(cfMeta, bytes("coordinator")), FBUtilities.getBroadcastAddress());
                addParameterColumns(cf, parameters);
                addColumn(cf, buildName(cfMeta, bytes("request")), request);
                addColumn(cf, buildName(cfMeta, bytes("started_at")), started_at);
                RowMutation mutation = new RowMutation(TRACE_KS, sessionIdBytes, cf);
                StorageProxy.mutate(Arrays.asList(mutation), ConsistencyLevel.ANY);
            }
        });
    }

    /**
     * Determines the tracing context from a message.  Does NOT set the threadlocal state.
     * 
     * @param message The internode message
     */
    public TraceState initializeFromMessage(final MessageIn<?> message)
    {
        final byte[] sessionBytes = message.parameters.get(Tracing.TRACE_HEADER);

        if (sessionBytes == null)
            return null;

        assert sessionBytes.length == 16;
        UUID sessionId = UUIDGen.getUUID(ByteBuffer.wrap(sessionBytes));
        TraceState ts = sessions.get(sessionId);
        if (ts != null)
            return ts;

        if (message.verb == MessagingService.Verb.REQUEST_RESPONSE)
        {
            // received a message for a session we've already closed out.  see CASSANDRA-5668
            return new ExpiredTraceState(sessionId);
        }
        else
        {
            ts = new TraceState(message.from, sessionId);
            sessions.put(sessionId, ts);
            return ts;
        }
    }

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
}
