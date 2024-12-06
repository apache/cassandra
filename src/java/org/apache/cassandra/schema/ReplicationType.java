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

package org.apache.cassandra.schema;

import java.io.IOException;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.tcm.serialization.MetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

public enum ReplicationType
{
    untracked, tracked;

    public static final MetadataSerializer<ReplicationType> serializer = new MetadataSerializer<>()
    {
        @Override
        public void serialize(ReplicationType t, DataOutputPlus out, Version version) throws IOException
        {
            if (version.isBefore(Version.V7))
                return;

            switch (t)
            {
                case untracked:
                    out.writeByte(0);
                    break;
                case tracked:
                    out.writeByte(1);
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported replication type: " + t);
            }
        }

        @Override
        public ReplicationType deserialize(DataInputPlus in, Version version) throws IOException
        {
            if (version.isBefore(Version.V7))
                return untracked;

            byte t = in.readByte();

            switch (t)
            {
                case 0:
                    return untracked;
                case 1:
                    return tracked;
                default:
                    throw new IllegalArgumentException("Unsupported replication type: " + t);
            }
        }

        @Override
        public long serializedSize(ReplicationType t, Version version)
        {
            if (version.isBefore(Version.V7))
                return 0;
            return TypeSizes.BYTE_SIZE;
        }
    };

    public boolean isTracked()
    {
        return this == tracked;
    }

    // FIXME: used in lieu of adding support for tracked reads in parameterized tests, fix usages of this method
    public static ReplicationType[] fixmeValues()
    {
        return new ReplicationType[]{ untracked };
    }
}
