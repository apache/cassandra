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

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.Map;

import org.apache.cassandra.utils.ByteBufferUtil;

import io.opentelemetry.context.propagation.TextMapGetter;

/**
 * TextMapGetter that extracts W3C Trace Context from native protocol's custom payload
 * <p>
 * In native protocol's custom payload, the following keys can be set to propagate tracing
 * from the applications.
 * <ul>
 *   <li>traceparent</li>
 *   <li>tracestate</li>
 * </ul>
 * OpenTelemetry only supports text format for propagating context right now.
 * So the values associated with the above keys are Strings.
 * </p>
 *
 * @see <a href="https://www.w3.org/TR/trace-context/">W3C Trace Context</a>
 */
public final class CustomPayloadGetter implements TextMapGetter<Map<String, ByteBuffer>>
{
    public static final CustomPayloadGetter instance = new CustomPayloadGetter();

    private CustomPayloadGetter() {}

    @Override
    public Iterable<String> keys(Map<String, ByteBuffer> carrier)
    {
        return carrier.keySet();
    }

    @Override
    public String get(Map<String, ByteBuffer> carrier, String key)
    {
        if (carrier == null || !carrier.containsKey(key))
        {
            return null;
        }
        ByteBuffer value = carrier.get(key);
        try
        {
            return ByteBufferUtil.string(value);
        }
        catch (CharacterCodingException e)
        {
            return null;
        }
    }
}
