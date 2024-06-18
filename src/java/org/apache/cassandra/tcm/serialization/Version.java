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

package org.apache.cassandra.tcm.serialization;

import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.NodeVersion;

public enum Version
{
    OLD(-1),
    V0(0),
    /**
     *  - Moved Partitioner in ClusterMetadata serializer to be the first field
     *  - Added a counter to Directory serializer to keep track of NodeIds
     */
    V1(1),
    /**
     *  - Added version to PlacementForRange serializer
     *  - Serialize MemtableParams when serializing TableParams
     */
    V2(2),
    /**
     * - down nodes serialized in PrepareCMSReconfiguration
     */
    V3(3),
    /**
     * - Serialize allowAutoSnapshot and incrementalBackups when serializing TableParams
     */
    V4(4),
    /**
     * - AlterSchema includes execution timestamp
     * - PreInitialize includes datacenter (affects local serialization on first CMS node only)
     */
    V5(5),

    UNKNOWN(Integer.MAX_VALUE);

    private static Map<Integer, Version> values = new HashMap<>();
    static
    {
        for (Version v : values())
            values.put(v.version, v);
    }

    private final int version;
    Version(int version)
    {
        this.version = version;
    }

    /**
     * Minimum serialization version known to all nodes in the cluster.
     */
    public static Version minCommonSerializationVersion()
    {
        ClusterMetadata metadata = ClusterMetadata.currentNullable();
        if (metadata != null)
            return metadata.directory.clusterMinVersion.serializationVersion();
        return NodeVersion.CURRENT.serializationVersion();

    }

    public int asInt()
    {
        return version;
    }

    public boolean equals(Version other)
    {
        return version == other.version;
    }

    public boolean isAtLeast(Version other)
    {
        return version >= other.version;
    }

    public boolean isBefore(Version other)
    {
        return version < other.version;
    }

    public static Version fromInt(int i)
    {
        Version v = values.get(i);
        if (v != null)
            return v;

        throw new IllegalArgumentException("Unsupported metadata version (" + i + ")");
    }
}
