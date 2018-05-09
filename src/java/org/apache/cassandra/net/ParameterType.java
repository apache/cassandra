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

package org.apache.cassandra.net;

import java.util.Map;

import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.io.DummyByteVersionedSerializer;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.ShortVersionedSerializer;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.UUIDSerializer;

/**
 * Type names and serializers for various parameters that
 */
public enum ParameterType
{
    FORWARD_TO("FORWARD_TO", ForwardToSerializer.instance),
    FORWARD_FROM("FORWARD_FROM", CompactEndpointSerializationHelper.instance),
    FAILURE_RESPONSE("FAIL", DummyByteVersionedSerializer.instance),
    FAILURE_REASON("FAIL_REASON", ShortVersionedSerializer.instance),
    FAILURE_CALLBACK("CAL_BAC", DummyByteVersionedSerializer.instance),
    TRACE_SESSION("TraceSession", UUIDSerializer.serializer),
    TRACE_TYPE("TraceType", Tracing.traceTypeSerializer),
    TRACK_REPAIRED_DATA("TrackRepaired", DummyByteVersionedSerializer.instance);

    public static final Map<String, ParameterType> byName;
    public final String key;
    public final IVersionedSerializer serializer;

    static
    {
        ImmutableMap.Builder<String, ParameterType> builder = ImmutableMap.builder();
        for (ParameterType type : values())
        {
            builder.put(type.key, type);
        }
        byName = builder.build();
    }

    ParameterType(String key, IVersionedSerializer serializer)
    {
        this.key = key;
        this.serializer = serializer;
    }

    public String key()
    {
        return key;
    }

}
