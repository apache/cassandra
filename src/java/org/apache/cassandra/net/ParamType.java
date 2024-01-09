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

import javax.annotation.Nullable;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.Int32Serializer;
import org.apache.cassandra.utils.Int64Serializer;
import org.apache.cassandra.utils.RangesSerializer;
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
 *
 * Do not re-use old, nor fill gaps in the sequence of, ids.  New IDs must be higher.
 */
public enum ParamType
{
    FORWARD_TO                       (0,  ForwardingInfo.serializer),
    RESPOND_TO                       (1,  fwdFrmSerializer),

    TRACE_SESSION                    (5,  TimeUUID.Serializer.instance),
    TRACE_TYPE                       (6,  Tracing.traceTypeSerializer),

    TOMBSTONE_FAIL                   (8,  Int32Serializer.serializer),
    TOMBSTONE_WARNING                (9,  Int32Serializer.serializer),
    LOCAL_READ_SIZE_FAIL             (10, Int64Serializer.serializer),
    LOCAL_READ_SIZE_WARN             (11, Int64Serializer.serializer),
    ROW_INDEX_READ_SIZE_FAIL         (12, Int64Serializer.serializer),
    ROW_INDEX_READ_SIZE_WARN         (13, Int64Serializer.serializer),
    CUSTOM_MAP                       (14, CustomParamsSerializer.serializer),
    SNAPSHOT_RANGES                  (15, RangesSerializer.serializer),
    TOO_MANY_REFERENCED_INDEXES_WARN (16, Int32Serializer.serializer),
    TOO_MANY_REFERENCED_INDEXES_FAIL (17, Int32Serializer.serializer);

    final int id;
    final IVersionedSerializer serializer;

    ParamType(int id, IVersionedSerializer serializer)
    {
        if (id < 0)
            throw new IllegalArgumentException("ParamType id must be non-negative");

        this.id = id;
        this.serializer = serializer;
    }

    private static final ParamType[] idToTypeMap;

    static
    {
        ParamType[] types = values();

        int max = -1;
        for (ParamType t : types)
            max = max(t.id, max);

        ParamType[] idMap = new ParamType[max + 1];

        for (ParamType type : types)
        {
            if (idMap[type.id] != null)
                throw new RuntimeException("Two ParamType-s that map to the same id: " + type.id);
            idMap[type.id] = type;

        }

        idToTypeMap = idMap;
    }

    @Nullable
    static ParamType lookUpById(int id)
    {
        if (id < 0)
            throw new IllegalArgumentException("ParamType id must be non-negative (got " + id + ')');

        return id < idToTypeMap.length ? idToTypeMap[id] : null;
    }

}
