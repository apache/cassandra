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

public class ReadTimeoutException extends RequestTimeoutException
{
    public final boolean dataPresent;

    public ReadTimeoutException(ConsistencyLevel consistency, int received, int blockFor, boolean dataPresent)
    {
        super(ExceptionCode.READ_TIMEOUT, consistency, received, blockFor);
        this.dataPresent = dataPresent;
    }

    public ReadTimeoutException(ConsistencyLevel consistency, int received, int blockFor, boolean dataPresent, String msg)
    {
        super(ExceptionCode.READ_TIMEOUT, consistency, received, blockFor, msg);
        this.dataPresent = dataPresent;
    }

    public ReadTimeoutException(ConsistencyLevel consistency, int received, int blockFor, boolean dataPresent, Throwable cause)
    {
        super(ExceptionCode.READ_TIMEOUT, consistency, received, blockFor, cause);
        this.dataPresent = dataPresent;
    }

    public ReadTimeoutException(ReadFailureException rfe)
    {
        super(ExceptionCode.READ_TIMEOUT, rfe.consistency, rfe.received, rfe.blockFor, rfe);
        this.dataPresent = rfe.dataPresent;
    }

    @Override
    protected void serializeSpecificFields(DataOutputPlus out, int version) throws IOException
    {
        out.writeByte(consistency.code);
        out.writeUnsignedVInt32(received);
        out.writeUnsignedVInt32(blockFor);
        out.writeBoolean(dataPresent);
    }

    @Override
    protected long serializedSizeSpecificFields(int version)
    {
        return TypeSizes.BYTE_SIZE + // consistency
               TypeSizes.sizeofUnsignedVInt(received) +
               TypeSizes.sizeofUnsignedVInt(blockFor) +
               TypeSizes.BOOL_SIZE;   // dataPresent
    }

    static ReadTimeoutException deserializeFields(String message, DataInputPlus in, int version) throws IOException
    {
        ConsistencyLevel consistency = ConsistencyLevel.fromCode(in.readUnsignedByte());
        int received = in.readUnsignedVInt32();
        int blockFor = in.readUnsignedVInt32();
        boolean dataPresent = in.readBoolean();
        return new ReadTimeoutException(consistency, received, blockFor, dataPresent, message);
    }

    @Override
    public CassandraExceptionCode getCassandraExceptionCode()
    {
        return CassandraExceptionCode.READ_TIMEOUT;
    }
}
