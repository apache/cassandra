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

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.progress.ProgressEvent;
import org.apache.commons.lang3.StringUtils;

public final class TracingTest
{
    @BeforeClass
    public static void setupDD()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void test()
    {
        List<String> traces = new ArrayList<>();
        Tracing tracing = new TracingImpl(traces);
        tracing.newSession(Tracing.TraceType.NONE);
        TraceState state = tracing.begin("test-request", Collections.<String,String>emptyMap());
        state.trace("test-1");
        state.trace("test-2");
        state.trace("test-3");
        tracing.stopSession();

        assert null == tracing.get();
        assert 4 == traces.size();
        assert "test-request".equals(traces.get(0));
        assert "test-1".equals(traces.get(1));
        assert "test-2".equals(traces.get(2));
        assert "test-3".equals(traces.get(3));
    }

    @Test
    public void test_get()
    {
        List<String> traces = new ArrayList<>();
        Tracing tracing = new TracingImpl(traces);
        tracing.newSession(Tracing.TraceType.NONE);
        tracing.begin("test-request", Collections.<String,String>emptyMap());
        tracing.get().trace("test-1");
        tracing.get().trace("test-2");
        tracing.get().trace("test-3");
        tracing.stopSession();

        assert null == tracing.get();
        assert 4 == traces.size();
        assert "test-request".equals(traces.get(0));
        assert "test-1".equals(traces.get(1));
        assert "test-2".equals(traces.get(2));
        assert "test-3".equals(traces.get(3));
    }

    @Test
    public void test_get_uuid()
    {
        List<String> traces = new ArrayList<>();
        Tracing tracing = new TracingImpl(traces);
        UUID uuid = tracing.newSession(Tracing.TraceType.NONE);
        tracing.begin("test-request", Collections.<String,String>emptyMap());
        tracing.get(uuid).trace("test-1");
        tracing.get(uuid).trace("test-2");
        tracing.get(uuid).trace("test-3");
        tracing.stopSession();

        assert null == tracing.get();
        assert 4 == traces.size();
        assert "test-request".equals(traces.get(0));
        assert "test-1".equals(traces.get(1));
        assert "test-2".equals(traces.get(2));
        assert "test-3".equals(traces.get(3));
    }

    @Test
    public void test_customPayload()
    {
        List<String> traces = new ArrayList<>();
        ByteBuffer customPayloadValue = ByteBuffer.wrap("test-value".getBytes());

        Map<String,ByteBuffer> customPayload = Collections.singletonMap("test-key", customPayloadValue);

        TracingImpl tracing = new TracingImpl(traces);
        tracing.newSession(customPayload);
        TraceState state = tracing.begin("test-custom_payload", Collections.<String,String>emptyMap());
        state.trace("test-1");
        state.trace("test-2");
        state.trace("test-3");
        tracing.stopSession();

        assert null == tracing.get();
        assert 4 == traces.size();
        assert "test-custom_payload".equals(traces.get(0));
        assert "test-1".equals(traces.get(1));
        assert "test-2".equals(traces.get(2));
        assert "test-3".equals(traces.get(3));
        assert tracing.payloads.containsKey("test-key");
        assert customPayloadValue.equals(tracing.payloads.get("test-key"));
    }

    @Test
    public void test_states()
    {
        List<String> traces = new ArrayList<>();
        Tracing tracing = new TracingImpl(traces);
        tracing.newSession(Tracing.TraceType.REPAIR);
        tracing.begin("test-request", Collections.<String,String>emptyMap());
        tracing.get().enableActivityNotification("test-tag");
        assert TraceState.Status.IDLE == tracing.get().waitActivity(1);
        tracing.get().trace("test-1");
        assert TraceState.Status.ACTIVE == tracing.get().waitActivity(1);
        tracing.get().stop();
        assert TraceState.Status.STOPPED == tracing.get().waitActivity(1);
        tracing.stopSession();
        assert null == tracing.get();
    }

    @Test
    public void test_progress_listener()
    {
        List<String> traces = new ArrayList<>();
        Tracing tracing = new TracingImpl(traces);
        tracing.newSession(Tracing.TraceType.REPAIR);
        tracing.begin("test-request", Collections.<String,String>emptyMap());
        tracing.get().enableActivityNotification("test-tag");

        tracing.get().addProgressListener((String tag, ProgressEvent pe) -> {
            assert "test-tag".equals(tag);
            assert "test-trace".equals(pe.getMessage());
        });

        tracing.get().trace("test-trace");
        tracing.stopSession();
        assert null == tracing.get();
    }

    private static final class TracingImpl extends Tracing
    {
        private final List<String> traces;
        private final Map<String,ByteBuffer> payloads = new HashMap<>();

        public TracingImpl()
        {
            this(new ArrayList<>());
        }

        public TracingImpl(List<String> traces)
        {
            this.traces = traces;
        }

        public void stopSessionImpl()
        {}

        public TraceState begin(String request, InetAddress ia, Map<String, String> map)
        {
            traces.add(request);
            return get();
        }

        protected UUID newSession(UUID sessionId, TraceType traceType, Map<String,ByteBuffer> customPayload)
        {
            if (!customPayload.isEmpty())
                logger.info("adding custom payload items {}", StringUtils.join(customPayload.keySet(), ','));

            payloads.putAll(customPayload);
            return super.newSession(sessionId, traceType, customPayload);
        }

        protected TraceState newTraceState(InetAddress ia, UUID uuid, Tracing.TraceType tt)
        {
            return new TraceState(ia, uuid, tt)
            {
                protected void traceImpl(String string)
                {
                    traces.add(string);
                }

                protected void waitForPendingEvents()
                {
                }
            };
        }

        public void trace(ByteBuffer bb, String string, int i)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }
    }
}
