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

package org.apache.cassandra.tcm;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.tcm.serialization.VerboseMetadataSerializer;

public interface MetadataSnapshots
{
    ClusterMetadata getLatestSnapshotAfter(Epoch epoch);
    ClusterMetadata getSnapshot(Epoch epoch);
    void storeSnapshot(ClusterMetadata metadata);

    static ByteBuffer toBytes(ClusterMetadata metadata) throws IOException
    {
        long serializedSize = VerboseMetadataSerializer.serializedSize(ClusterMetadata.serializer, metadata);
        ByteBuffer bytes = ByteBuffer.allocate((int) serializedSize);
        try (DataOutputBuffer dob = new DataOutputBuffer(bytes))
        {
            VerboseMetadataSerializer.serialize(ClusterMetadata.serializer, metadata, dob);
        }
        bytes.flip().rewind();
        return bytes;
    }

    @SuppressWarnings("resource")
    static ClusterMetadata fromBytes(ByteBuffer serialized) throws IOException
    {
        if (serialized == null)
            return null;

        return VerboseMetadataSerializer.deserialize(ClusterMetadata.serializer,
                                                     new DataInputBuffer(serialized, false));
    }


    MetadataSnapshots NO_OP = new NoOp();

    public class NoOp implements MetadataSnapshots
    {
        @Override
        public ClusterMetadata getLatestSnapshotAfter(Epoch epoch)
        {
            return null;
        }

        @Override
        public ClusterMetadata getSnapshot(Epoch epoch)
        {
            return null;
        }

        @Override
        public void storeSnapshot(ClusterMetadata metadata) {}
    }

    class SystemKeyspaceMetadataSnapshots implements MetadataSnapshots
    {
        @Override
        public ClusterMetadata getLatestSnapshotAfter(Epoch epoch)
        {
            Sealed sealed = Sealed.lookupForSnapshot(epoch);
            return sealed.epoch.isAfter(epoch) ? getSnapshot(sealed.epoch) : null;
        }

        @Override
        public ClusterMetadata getSnapshot(Epoch epoch)
        {
            try
            {
                return fromBytes(SystemKeyspace.getSnapshot(epoch));
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void storeSnapshot(ClusterMetadata metadata)
        {
            try
            {
                SystemKeyspace.storeSnapshot(metadata.epoch, metadata.period, toBytes(metadata));
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }
    }
}
