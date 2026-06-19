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

import org.junit.Test;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.context.Context;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TraceContextSerializerTest
{
    private static final int VERSION = MessagingService.current_version;

    @Test
    public void testRoundTripPreservesSpanContextAndMarksParentRemote() throws Exception
    {
        TraceState traceState = TraceState.builder()
                                          .put("tenant", "alpha")
                                          .put("sample-rate", "0.5")
                                          .build();
        SpanContext localSpanContext = SpanContext.create("0123456789abcdef0123456789abcdef",
                                                          "0123456789abcdef",
                                                          TraceFlags.getSampled(),
                                                          traceState);
        Context context = Context.root().with(Span.wrap(localSpanContext));
        long expectedSerializedSize = 25
                                      + TypeSizes.sizeofUnsignedVInt(traceState.size())
                                      + TypeSizes.sizeof("tenant")
                                      + TypeSizes.sizeof("alpha")
                                      + TypeSizes.sizeof("sample-rate")
                                      + TypeSizes.sizeof("0.5");

        assertFalse(localSpanContext.isRemote());
        assertEquals(expectedSerializedSize, TraceContextSerializer.serializer.serializedSize(context, VERSION));

        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            TraceContextSerializer.serializer.serialize(context, out, VERSION);
            assertEquals(expectedSerializedSize, out.getLength());

            try (DataInputBuffer in = new DataInputBuffer(out.buffer(), true))
            {
                Context deserialized = TraceContextSerializer.serializer.deserialize(in, VERSION);
                SpanContext deserializedSpanContext = Span.fromContext(deserialized).getSpanContext();

                assertEquals(localSpanContext.getTraceId(), deserializedSpanContext.getTraceId());
                assertEquals(localSpanContext.getSpanId(), deserializedSpanContext.getSpanId());
                assertEquals(localSpanContext.getTraceFlags(), deserializedSpanContext.getTraceFlags());
                assertEquals(localSpanContext.getTraceState().asMap(), deserializedSpanContext.getTraceState().asMap());
                assertTrue(deserializedSpanContext.isRemote());
                assertEquals(0, in.available());
            }
        }
    }
}
