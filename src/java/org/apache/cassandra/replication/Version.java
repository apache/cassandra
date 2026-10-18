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
package org.apache.cassandra.replication;

import java.io.IOException;

import com.google.common.base.Preconditions;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.UnversionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;

/**
 * The enum used for versioning mutation-tracking-specific messages
 * and their nested data structures (plus certain on-disk structures).
 * <p>
 * This exists so that we can evolve Mutation Tracking (MT) serialization
 * without having to bump the {@link MessagingService} (MS) version,
 * as 1) that is somewhat a big deal, and 2) MT is still actively evolving.
 * <p>
 * Today, only the MS version is negotiated between nodes, so for serializing
 * messages we need to infer the appropriate MT version somehow when serializing,
 * and include the version that was used with the payload. When deserializing,
 * we'll read the MT version from the payload and use it explicitly to decode the message.
 * <p>
 * The current approach is to manually specify the cluster safe MT version
 * (see {@code CLUSTER_SAFE_VERSION}). If and when a new MT version is
 * introduced, once all the nodes have been upgraded to C* versions
 * that support it, {@code CLUSTER_SAFE_VERSION} can be bumped.
 */
public enum Version
{
    V1(1, MessagingService.VERSION_61),
    V2(2, MessagingService.VERSION_61);

    static final Version CURRENT = V2;

    private static final Version[] versionMap;
    static
    {
        Version[] values = values();

        int max = -1;
        for (Version version : values)
            max = Integer.max(version.version, max);
        Version[] intToVersionMap = new Version[max + 1];
        for (Version version : values)
            intToVersionMap[version.version] = version;
        versionMap = intToVersionMap;

        for (int i = 1; i < values.length; i++)
        {
            Version prev = values[i - 1];
            Version version = values[i];

            Preconditions.checkState(version.version > prev.version);
            Preconditions.checkState(version.messagingVersion >= prev.messagingVersion);
        }
    }

    private static volatile Version clusterSafeVersion =
        fromInt(CassandraRelevantProperties.MT_CLUSTER_SAFE_VERSION.getInt(CURRENT.version));

    public static Version getClusterSafeVersion()
    {
        return clusterSafeVersion;
    }

    public static void setClusterSafeVersion(Version version)
    {
        Preconditions.checkNotNull(version);
        clusterSafeVersion = version;
    }

    public static void setClusterSafeVersion(int versionInt)
    {
        setClusterSafeVersion(fromInt(versionInt));
    }

    private final int version;
    private final int messagingVersion;

    Version(int version, int messagingVersion)
    {
        this.version = version;
        this.messagingVersion = messagingVersion;
    }

    public int version()
    {
        return version;
    }

    public int messagingVersion()
    {
        return messagingVersion;
    }

    public boolean isAtLeast(Version other)
    {
        return this.version >= other.version;
    }

    private static Version fromInt(int v)
    {
        if (v < 1 || v >= versionMap.length || versionMap[v] == null)
            throw new IllegalArgumentException("version " + v + " is not recognized");
        return versionMap[v];
    }

    public static final UnversionedSerializer<Version> serializer = new UnversionedSerializer<>()
    {
        @Override
        public void serialize(Version v, DataOutputPlus out) throws IOException
        {
            out.writeUnsignedVInt32(v.version);
        }

        @Override
        public Version deserialize(DataInputPlus in) throws IOException
        {
            return fromInt(in.readUnsignedVInt32());
        }

        @Override
        public long serializedSize(Version v)
        {
            return TypeSizes.sizeofUnsignedVInt(v.version);
        }
    };
}
