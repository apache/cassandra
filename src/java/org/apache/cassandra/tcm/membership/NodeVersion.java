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

package org.apache.cassandra.tcm.membership;

import java.io.IOException;
import java.util.Objects;

import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.tcm.serialization.MetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.utils.CassandraVersion;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.db.TypeSizes.sizeof;
import static org.apache.cassandra.db.TypeSizes.sizeofUnsignedVInt;

public class NodeVersion implements Comparable<NodeVersion>
{
    public static final Serializer serializer = new Serializer();
    public static final Version CURRENT_METADATA_VERSION = Version.V4;
    public static final NodeVersion CURRENT = new NodeVersion(new CassandraVersion(FBUtilities.getReleaseVersionString()), CURRENT_METADATA_VERSION);
    private static final CassandraVersion SINCE_VERSION = CassandraVersion.CASSANDRA_5_0;

    public final CassandraVersion cassandraVersion;
    public final int serializationVersion;

    public NodeVersion(CassandraVersion cassandraVersion, Version serializationVersion)
    {
        assert serializationVersion != Version.UNKNOWN;
        this.cassandraVersion = cassandraVersion;
        this.serializationVersion = serializationVersion.asInt();
    }

    private NodeVersion(CassandraVersion cassandraVersion, int serializationVersion)
    {
        this.cassandraVersion = cassandraVersion;
        this.serializationVersion = serializationVersion;
    }

    public Version serializationVersion()
    {
        if (serializationVersion <= CURRENT.serializationVersion)
            return Version.fromInt(serializationVersion);

        return Version.UNKNOWN;
    }

    public boolean isUpgraded()
    {
        return serializationVersion >= Version.V0.asInt();
    }

    @Override
    public String toString()
    {
        return "NodeVersion{" +
               "cassandraVersion=" + cassandraVersion +
               ", serializationVersion=" + serializationVersion +
               '}';
    }

    @Override
    public int compareTo(NodeVersion o)
    {
        // only comparing cassandraVersion here - if we bump serializationVersion we need to release a new cassandra version
        return cassandraVersion.compareTo(o.cassandraVersion);
    }

    public static NodeVersion fromCassandraVersion(CassandraVersion cv)
    {
        if (cv == null)
            return CURRENT;
        Version version = Version.OLD;
        if (cv.compareTo(SINCE_VERSION, true) >= 0)
            version = CURRENT_METADATA_VERSION;
        return new NodeVersion(cv, version);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (!(o instanceof NodeVersion)) return false;
        NodeVersion that = (NodeVersion) o;
        return Objects.equals(cassandraVersion, that.cassandraVersion) && serializationVersion == that.serializationVersion;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(cassandraVersion, serializationVersion);
    }

    public static class Serializer implements MetadataSerializer<NodeVersion>
    {
        @Override
        public void serialize(NodeVersion t, DataOutputPlus out, Version version) throws IOException
        {
            out.writeUTF(t.cassandraVersion.toString());
            if (t.serializationVersion == Version.UNKNOWN.asInt())
                throw new IllegalStateException("Should not serialize UNKNOWN version");
            out.writeUnsignedVInt32(t.serializationVersion);
        }

        @Override
        public NodeVersion deserialize(DataInputPlus in, Version version) throws IOException
        {
            CassandraVersion cassandraVersion = new CassandraVersion(in.readUTF());
            int serializationVersion = in.readUnsignedVInt32();
            return new NodeVersion(cassandraVersion, serializationVersion);
        }

        @Override
        public long serializedSize(NodeVersion t, Version version)
        {
            return sizeof(t.cassandraVersion.toString()) +
                   sizeofUnsignedVInt(t.serializationVersion);
        }
    }
}
