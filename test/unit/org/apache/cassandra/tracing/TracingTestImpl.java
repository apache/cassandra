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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.utils.TimeUUID;

public final class TracingTestImpl extends Tracing
{
    private final List<String> traces;

    private final Map<String, ByteBuffer> payloads = new HashMap<>();

    public TracingTestImpl()
    {
        this(new ArrayList<>());
    }

    public TracingTestImpl(List<String> traces)
    {
        this.traces = traces;
    }

    @Override
    public void stopSessionImpl()
    {}

    @Override
    public TraceState begin(String request, InetAddress ia, Map<String, String> map)
    {
        traces.add(request);
        return get();
    }

    @Override
    protected TimeUUID newSession(TimeUUID sessionId, TraceType traceType, Map<String,ByteBuffer> customPayload)
    {
        if (!customPayload.isEmpty())
            logger.info("adding custom payload items {}", StringUtils.join(customPayload.keySet(), ','));

        payloads.putAll(customPayload);
        return super.newSession(sessionId, traceType, customPayload);
    }

    @Override
    protected TraceState newTraceState(InetAddressAndPort ia, TimeUUID uuid, Tracing.TraceType tt)
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

    @Override
    public void trace(ByteBuffer bb, String message, int i)
    {
        traces.add(message);
    }

    public Map<String, ByteBuffer> getPayloads()
    {
        return payloads;
    }

    public List<String> getTraces()
    {
        return traces;
    }
}
