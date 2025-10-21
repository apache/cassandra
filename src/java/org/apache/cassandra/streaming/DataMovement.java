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

package org.apache.cassandra.streaming;

import java.io.IOException;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.tcm.ownership.MovementMap;

public class DataMovement
{
    public static final IVersionedSerializer<DataMovement> serializer = new Serializer();
    public final String operationId;
    public final String streamOperation;
    public final MovementMap movements;

    public DataMovement(String operationId, String streamOperation, MovementMap movements)
    {
        this.operationId = operationId;
        this.streamOperation = streamOperation;
        this.movements = movements;
    }

    public static class Serializer implements IVersionedSerializer<DataMovement>
    {
        @Override
        public void serialize(DataMovement t, DataOutputPlus out, int version) throws IOException
        {
            out.writeUTF(t.operationId);
            out.writeUTF(t.streamOperation);
            MovementMap.serializer.serialize(t.movements, out, version);
        }

        @Override
        public DataMovement deserialize(DataInputPlus in, int version) throws IOException
        {
            return new DataMovement(in.readUTF(),
                                    in.readUTF(),
                                    MovementMap.serializer.deserialize(in, version));
        }

        @Override
        public long serializedSize(DataMovement t, int version)
        {
            return TypeSizes.sizeof(t.operationId) +
                   TypeSizes.sizeof(t.streamOperation) +
                   MovementMap.serializer.serializedSize(t.movements, version);
        }
    }

    public static class Status
    {
        public static final Serializer serializer = new Serializer();
        public final boolean success;
        public final String operationType;
        public final String operationId;

        public Status(boolean success, String operationType, String operationId)
        {
            this.success = success;
            this.operationType = operationType;
            this.operationId = operationId;
        }

        static class Serializer implements IVersionedSerializer<Status>
        {
            @Override
            public void serialize(Status t, DataOutputPlus out, int version) throws IOException
            {
                out.writeBoolean(t.success);
                out.writeUTF(t.operationType);
                out.writeUTF(t.operationId);
            }

            @Override
            public Status deserialize(DataInputPlus in, int version) throws IOException
            {
                return new Status(in.readBoolean(),
                                  in.readUTF(),
                                  in.readUTF());
            }

            @Override
            public long serializedSize(Status t, int version)
            {
                return TypeSizes.sizeof(t.success) +
                       TypeSizes.sizeof(t.operationType) +
                       TypeSizes.sizeof(t.operationId);
            }
        }
    }
}
