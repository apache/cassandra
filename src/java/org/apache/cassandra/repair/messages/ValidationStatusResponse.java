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

import com.google.common.base.Throwables;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.repair.RepairJobDesc;
import org.apache.cassandra.repair.ValidationProgress;

public class ValidationStatusResponse extends RepairMessage
{
    // progress
    public final ValidationProgress.State state;
    public final float progress;
    public final String failureCause;
    public final long lastUpdatedAtMicro;

    public ValidationStatusResponse(RepairJobDesc desc,
                                    ValidationProgress.State state,
                                    float progress,
                                    String failureCause,
                                    long lastUpdatedAtMicro)
    {
        super(desc);
        this.state = state;
        this.progress = progress;
        this.failureCause = failureCause;
        this.lastUpdatedAtMicro = lastUpdatedAtMicro;
    }

    public static ValidationStatusResponse create(RepairJobDesc desc, ValidationProgress progress)
    {
        long updatedAtMicro = progress.getLastUpdatedAtMicro();
        String cause = null;
        if (progress.getFailureCause() != null)
            cause = progress.getFailureCause();
        return new ValidationStatusResponse(desc, progress.getState(), progress.getProgress(), cause, updatedAtMicro);
    }

    public static ValidationStatusResponse notFound(RepairJobDesc desc)
    {
        long updatedAtMicro = System.currentTimeMillis() * 1000;
        String cause = "Unable to find validation for job";
        return new ValidationStatusResponse(desc, ValidationProgress.State.UNKNOWN, -1, cause, updatedAtMicro);
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
            out.writeLong(t.lastUpdatedAtMicro);
        }

        public ValidationStatusResponse deserialize(DataInputPlus in, int version) throws IOException
        {
            RepairJobDesc desc = RepairJobDesc.serializer.deserialize(in, version);

            // progress
            ValidationProgress.State state = ValidationProgress.State.valueOf(in.readUTF().toUpperCase());
            float progress = in.readFloat();
            String failureCause = in.readBoolean() ? in.readUTF() : null;
            long lastUpdatedAtMicro = in.readLong();
            return new ValidationStatusResponse(desc, state, progress, failureCause, lastUpdatedAtMicro);
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
            size += TypeSizes.sizeof(t.lastUpdatedAtMicro);
            return size;
        }
    };
}
