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
package org.apache.cassandra.repair.messages;

import java.io.IOException;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.repair.RepairJobDesc;
import org.apache.cassandra.replication.ValidationOffsets;
import org.apache.cassandra.replication.Version;
import org.apache.cassandra.replication.VersionedSerializer;

/**
 * Request payload for the tracked keyspace validation. Carries the
 * {@link ValidationOffsets} so the participant can filter its SSTables and journal replay
 * to the same offset. The response is the classic {@link ValidationResponse}, sent on
 * {@link org.apache.cassandra.net.Verb#MT_VALIDATION_RSP}.
 */
public class MutationTrackingValidationRequest extends RepairMessage
{
    public final long nowInSec;
    public final boolean dontPurgeTombstones;
    public final ValidationOffsets offset;

    public MutationTrackingValidationRequest(RepairJobDesc desc, long nowInSec, boolean dontPurgeTombstones, ValidationOffsets offset)
    {
        super(desc);
        this.nowInSec = nowInSec;
        this.dontPurgeTombstones = dontPurgeTombstones;
        this.offset = offset;
    }

    @Override
    public String toString()
    {
        return "MutationTrackingValidationRequest{desc=" + desc + ", nowInSec=" + nowInSec + ", offset=" + offset + '}';
    }

    public static final VersionedSerializer<MutationTrackingValidationRequest> serializer = new VersionedSerializer<>()
    {
        public void serialize(MutationTrackingValidationRequest request, DataOutputPlus out, Version version) throws IOException
        {
            RepairJobDesc.serializer.serialize(request.desc, out, version.messagingVersion());
            out.writeLong(request.nowInSec);
            out.writeBoolean(request.dontPurgeTombstones);
            ValidationOffsets.serializer.serialize(request.offset, out);
        }

        public MutationTrackingValidationRequest deserialize(DataInputPlus in, Version version) throws IOException
        {
            RepairJobDesc desc = RepairJobDesc.serializer.deserialize(in, version.messagingVersion());
            long nowInSec = in.readLong();
            boolean dontPurgeTombstones = in.readBoolean();
            ValidationOffsets offset = ValidationOffsets.serializer.deserialize(in);
            return new MutationTrackingValidationRequest(desc, nowInSec, dontPurgeTombstones, offset);
        }

        public long serializedSize(MutationTrackingValidationRequest request, Version version)
        {
            return RepairJobDesc.serializer.serializedSize(request.desc, version.messagingVersion())
                   + Long.BYTES
                   + TypeSizes.sizeof(request.dontPurgeTombstones)
                   + ValidationOffsets.serializer.serializedSize(request.offset);
        }
    };
}
