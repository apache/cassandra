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

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.repair.RepairJobDesc;

public class ValidationStatusRequest extends RepairMessage
{
    public ValidationStatusRequest(RepairJobDesc desc)
    {
        super(desc);
    }

    public static final IVersionedSerializer<ValidationStatusRequest> serializer = new IVersionedSerializer<ValidationStatusRequest>()
    {
        public void serialize(ValidationStatusRequest t, DataOutputPlus out, int version) throws IOException
        {
            RepairJobDesc.serializer.serialize(t.desc, out, version);
        }

        public ValidationStatusRequest deserialize(DataInputPlus in, int version) throws IOException
        {
            return new ValidationStatusRequest(RepairJobDesc.serializer.deserialize(in, version));
        }

        public long serializedSize(ValidationStatusRequest t, int version)
        {
            return RepairJobDesc.serializer.serializedSize(t.desc, version);
        }
    };
}
