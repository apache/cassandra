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

package org.apache.cassandra.exceptions;

import java.io.IOException;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

public class CasWriteUnknownResultException extends RequestExecutionException
{
    public final ConsistencyLevel consistency;
    public final int received;
    public final int blockFor;

    public CasWriteUnknownResultException(ConsistencyLevel consistency, int received, int blockFor)
    {
        super(ExceptionCode.CAS_WRITE_UNKNOWN, String.format("CAS operation result is unknown - proposal accepted by %d but not a quorum.", received));
        this.consistency = consistency;
        this.received = received;
        this.blockFor = blockFor;
    }

    @Override
    protected void serializeSpecificFields(DataOutputPlus out, int version) throws IOException
    {
        out.writeByte(consistency.code);
        out.writeUnsignedVInt32(received);
        out.writeUnsignedVInt32(blockFor);
    }

    @Override
    protected long serializedSizeSpecificFields(int version)
    {
        return TypeSizes.BYTE_SIZE + // consistency
               TypeSizes.sizeofUnsignedVInt(received) +
               TypeSizes.sizeofUnsignedVInt(blockFor);
    }

    static CasWriteUnknownResultException deserializeFields(String message, DataInputPlus in, int version) throws IOException
    {
        ConsistencyLevel consistency = ConsistencyLevel.fromCode(in.readUnsignedByte());
        int received = in.readUnsignedVInt32();
        int blockFor = in.readUnsignedVInt32();
        return new CasWriteUnknownResultException(consistency, received, blockFor);
    }

    @Override
    public CassandraExceptionCode getCassandraExceptionCode()
    {
        return CassandraExceptionCode.CAS_WRITE_UNKNOWN;
    }
}
