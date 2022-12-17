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

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.Int32Serializer;
import org.apache.cassandra.utils.Int64Serializer;
import org.apache.cassandra.utils.TimeUUID;

import static java.lang.Math.max;

import static org.apache.cassandra.locator.InetAddressAndPort.FwdFrmSerializer.fwdFrmSerializer;

/**
 * Type names and serializers for various parameters that can be put in {@link Message} params map.
 *
 * It should be safe to add new params without bumping messaging version - {@link Message} serializer
 * will skip over any params it doesn't recognise.
 *
 * Please don't add boolean params here. Extend and use {@link MessageFlag} instead.
 */
public enum ParamType
{
    FORWARD_TO          (0, "FWD_TO",        ForwardingInfo.serializer),
    RESPOND_TO          (1, "FWD_FRM",       fwdFrmSerializer),

    @Deprecated
    FAILURE_RESPONSE    (2, "FAIL",          LegacyFlag.serializer),
    @Deprecated
    FAILURE_REASON      (3, "FAIL_REASON",   RequestFailureReason.serializer),
    @Deprecated
    FAILURE_CALLBACK    (4, "CAL_BAC",       LegacyFlag.serializer),

    TRACE_SESSION       (5, "TraceSession",  TimeUUID.Serializer.instance),
    TRACE_TYPE          (6, "TraceType",     Tracing.traceTypeSerializer),

    @Deprecated
    TRACK_REPAIRED_DATA (7, "TrackRepaired", LegacyFlag.serializer),

    TOMBSTONE_FAIL(8, "TSF", Int32Serializer.serializer),
    TOMBSTONE_WARNING(9, "TSW", Int32Serializer.serializer),
    LOCAL_READ_SIZE_FAIL(10, "LRSF", Int64Serializer.serializer),
    LOCAL_READ_SIZE_WARN(11, "LRSW", Int64Serializer.serializer),
    ROW_INDEX_READ_SIZE_FAIL(12, "RIRSF", Int64Serializer.serializer),
    ROW_INDEX_READ_SIZE_WARN(13, "RIRSW", Int64Serializer.serializer),

    CUSTOM_MAP          (14, "CUSTOM",       CustomParamsSerializer.serializer);

    final int id;
    @Deprecated final String legacyAlias; // pre-4.0 we used to serialize entire param name string
    final IVersionedSerializer serializer;

    ParamType(int id, String legacyAlias, IVersionedSerializer serializer)
    {
        if (id < 0)
            throw new IllegalArgumentException("ParamType id must be non-negative");

        this.id = id;
        this.legacyAlias = legacyAlias;
        this.serializer = serializer;
    }

    private static final ParamType[] idToTypeMap;
    private static final Map<String, ParamType> aliasToTypeMap;

    static
    {
        ParamType[] types = values();

        int max = -1;
        for (ParamType t : types)
            max = max(t.id, max);

        ParamType[] idMap = new ParamType[max + 1];
        Map<String, ParamType> aliasMap = new HashMap<>();

        for (ParamType type : types)
        {
            if (idMap[type.id] != null)
                throw new RuntimeException("Two ParamType-s that map to the same id: " + type.id);
            idMap[type.id] = type;

            if (aliasMap.put(type.legacyAlias, type) != null)
                throw new RuntimeException("Two ParamType-s that map to the same legacy alias: " + type.legacyAlias);
        }

        idToTypeMap = idMap;
        aliasToTypeMap = aliasMap;
    }

    @Nullable
    static ParamType lookUpById(int id)
    {
        if (id < 0)
            throw new IllegalArgumentException("ParamType id must be non-negative (got " + id + ')');

        return id < idToTypeMap.length ? idToTypeMap[id] : null;
    }

    @Nullable
    static ParamType lookUpByAlias(String alias)
    {
        return aliasToTypeMap.get(alias);
    }
}
