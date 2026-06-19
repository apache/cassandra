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

package org.apache.cassandra.telemetry.tracing;

import java.io.IOException;
import java.util.Map;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.SpanId;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceId;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.api.trace.TraceStateBuilder;
import io.opentelemetry.context.Context;

/**
 * Serializer for OpenTelemetry {@link Context}.
 * <p>
 * This is used to propagate Tracing Context across the nodes through inter-node messaging.
 * </p>
 */
public class TraceContextSerializer implements IVersionedSerializer<Context>
{
    public static final TraceContextSerializer serializer = new TraceContextSerializer();

    @Override
    public void serialize(Context context, DataOutputPlus out, int version) throws IOException
    {
        SpanContext spanContext = Span.fromContext(context).getSpanContext();
        out.write(spanContext.getTraceIdBytes());
        out.write(spanContext.getSpanIdBytes());
        out.write(spanContext.getTraceFlags().asByte());
        TraceState traceState = spanContext.getTraceState();
        out.writeUnsignedVInt32(traceState.size());
        for (Map.Entry<String, String> entry : traceState.asMap().entrySet())
        {
            out.writeUTF(entry.getKey());
            out.writeUTF(entry.getValue());
        }
    }

    @Override
    public Context deserialize(DataInputPlus in, int version) throws IOException
    {
        // Trace ID - 16 bytes
        byte[] traceIdBytes = new byte[16];
        in.readFully(traceIdBytes);
        String traceId = TraceId.fromBytes(traceIdBytes);
        // Span ID - 8 bytes
        byte[] spanIdBytes = new byte[8];
        in.readFully(spanIdBytes);
        String spanId = SpanId.fromBytes(spanIdBytes);
        // Trace flag - 1 byte
        byte traceFlagsByte = in.readByte();
        TraceFlags traceFlags = TraceFlags.fromByte(traceFlagsByte);

        TraceStateBuilder traceStateBuilder = TraceState.builder();
        int traceStateSize = in.readUnsignedVInt32();
        for (int i = 0; i < traceStateSize; i++)
            traceStateBuilder.put(in.readUTF(), in.readUTF());

        SpanContext remoteContext = SpanContext.createFromRemoteParent(traceId, spanId, traceFlags, traceStateBuilder.build());

        if (!remoteContext.isValid())
            return Context.current();

        return Context.current().with(Span.wrap(remoteContext));
    }

    @Override
    public long serializedSize(Context context, int version)
    {
        SpanContext spanContext = Span.fromContext(context).getSpanContext();
        TraceState traceState = spanContext.getTraceState();
        long size = 16 + 8 + 1 + TypeSizes.sizeofUnsignedVInt(traceState.size());
        for (Map.Entry<String, String> entry : traceState.asMap().entrySet())
            size += TypeSizes.sizeof(entry.getKey()) + TypeSizes.sizeof(entry.getValue());
        return size;
    }
}
