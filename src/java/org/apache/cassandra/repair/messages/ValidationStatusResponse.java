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
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.repair.RepairJobDesc;
import org.apache.cassandra.repair.ValidationState;

public class ValidationStatusResponse extends RepairMessage
{
    // progress
    public final ValidationState.State state;
    public final float progress;
    public final String failureCause;
    public final long lastUpdatedAtMillis;
    public final long durationNanos;

    public ValidationStatusResponse(RepairJobDesc desc,
                                    ValidationState.State state,
                                    float progress,
                                    String failureCause,
                                    long lastUpdatedAtMillis,
                                    long durationNanos)
    {
        super(desc);
        this.state = state;
        this.progress = progress;
        this.failureCause = failureCause;
        this.lastUpdatedAtMillis = lastUpdatedAtMillis;
        this.durationNanos = durationNanos;
    }

    public static ValidationStatusResponse create(RepairJobDesc desc, ValidationState progress)
    {
        long updatedAtMillis = progress.getLastUpdatedAtMillis();
        long durationNanos = progress.getDurationNanos();
        String cause = null;
        if (progress.getFailureCause() != null)
            cause = progress.getFailureCause();
        return new ValidationStatusResponse(desc, progress.getState(), progress.getProgress(), cause, updatedAtMillis, durationNanos);
    }

    public static ValidationStatusResponse notFound(RepairJobDesc desc)
    {
        String cause = "Unable to find validation for job";
        return new ValidationStatusResponse(desc, ValidationState.State.UNKNOWN, -1, cause, System.currentTimeMillis(), 0);
    }

    public static final IVersionedSerializer<ValidationStatusResponse> serializer = new IVersionedSerializer<ValidationStatusResponse>()
    {

        public void serialize(ValidationStatusResponse t, DataOutputPlus out, int version) throws IOException
        {
            RepairJobDesc.serializer.serialize(t.desc, out, version);

            // progress
            out.writeUTF(t.state.name());
            out.writeFloat(t.progress);
            out.writeBoolean(t.failureCause != null);
            if (t.failureCause != null)
                out.writeUTF(t.failureCause);
            out.writeLong(t.lastUpdatedAtMillis);
            out.writeLong(t.durationNanos);
        }

        public ValidationStatusResponse deserialize(DataInputPlus in, int version) throws IOException
        {
            RepairJobDesc desc = RepairJobDesc.serializer.deserialize(in, version);

            // progress
            ValidationState.State state = ValidationState.State.valueOf(in.readUTF().toUpperCase());
            float progress = in.readFloat();
            String failureCause = in.readBoolean() ? in.readUTF() : null;
            long lastUpdatedAtMillis = in.readLong();
            long durationNanos = in.readLong();
            return new ValidationStatusResponse(desc, state, progress, failureCause, lastUpdatedAtMillis, durationNanos);
        }

        public long serializedSize(ValidationStatusResponse t, int version)
        {
            int size = 0;
            size += RepairJobDesc.serializer.serializedSize(t.desc, version);

            // progress
            size += TypeSizes.sizeof(t.state.name()); //TODO should make this a byte
            size += TypeSizes.sizeof(t.progress);
            size += TypeSizes.sizeof(t.failureCause != null);
            if (t.failureCause != null)
                size += TypeSizes.sizeof(t.failureCause);
            size += TypeSizes.sizeof(t.lastUpdatedAtMillis);
            size += TypeSizes.sizeof(t.durationNanos);
            return size;
        }
    };
}
