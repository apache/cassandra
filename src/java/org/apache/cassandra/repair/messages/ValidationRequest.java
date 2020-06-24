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

/**
 * ValidationRequest
 *
 * @since 2.0
 */
public class ValidationRequest extends RepairMessage
{
    public final int nowInSec;

    public ValidationRequest(RepairJobDesc desc, int nowInSec)
    {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-15163
        super(desc);
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-13671
        this.nowInSec = nowInSec;
    }

    @Override
    public String toString()
    {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-7586
        return "ValidationRequest{" +
               "nowInSec=" + nowInSec +
               "} " + super.toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ValidationRequest that = (ValidationRequest) o;
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-13671
        return nowInSec == that.nowInSec;
    }

    @Override
    public int hashCode()
    {
        return nowInSec;
    }

    public static final IVersionedSerializer<ValidationRequest> serializer = new IVersionedSerializer<ValidationRequest>()
    {
        public void serialize(ValidationRequest message, DataOutputPlus out, int version) throws IOException
        {
            RepairJobDesc.serializer.serialize(message.desc, out, version);
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-13671
            out.writeInt(message.nowInSec);
        }

        public ValidationRequest deserialize(DataInputPlus dis, int version) throws IOException
        {
            RepairJobDesc desc = RepairJobDesc.serializer.deserialize(dis, version);
            return new ValidationRequest(desc, dis.readInt());
        }

        public long serializedSize(ValidationRequest message, int version)
        {
            long size = RepairJobDesc.serializer.serializedSize(message.desc, version);
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-13671
            size += TypeSizes.sizeof(message.nowInSec);
            return size;
        }
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-15163
    };
}
