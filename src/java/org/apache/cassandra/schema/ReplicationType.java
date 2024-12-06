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
    legacy, logged;

    public static final MetadataSerializer<ReplicationType> serializer = new MetadataSerializer<ReplicationType>()
    {
        @Override
        public void serialize(ReplicationType t, DataOutputPlus out, Version version) throws IOException
        {
            if (!version.isAtLeast(Version.V6))
                return;

            switch (t)
            {
                case legacy:
                    out.writeByte(0);
                    break;
                case logged:
                    out.writeByte(1);
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported replication type: " + t);
            }
        }

        @Override
        public ReplicationType deserialize(DataInputPlus in, Version version) throws IOException
        {
            if (!version.isAtLeast(Version.V6))
                return legacy;

            byte t = in.readByte();

            switch (t)
            {
                case 0:
                    return legacy;
                case 1:
                    return logged;
                default:
                    throw new IllegalArgumentException("Unsupported replication type: " + t);
            }
        }

        @Override
        public long serializedSize(ReplicationType t, Version version)
        {
            return version.isAtLeast(Version.V6) ? TypeSizes.BYTE_SIZE : 0;
        }
    };
}
