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

package org.apache.cassandra.service.accord.exceptions;

import java.io.IOException;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.exceptions.CassandraExceptionCode;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

import static org.apache.cassandra.db.ConsistencyLevel.SERIAL;

public class AccordWriteExhaustedException extends WriteTimeoutException
{
    public AccordWriteExhaustedException(int received, int blockFor)
    {
        super(WriteType.CAS, SERIAL, received, blockFor);
    }

    public AccordWriteExhaustedException(int received, int blockFor, String msg)
    {
        super(WriteType.CAS, SERIAL, received, blockFor, msg);
    }

    @Override
    protected void serializeSpecificFields(DataOutputPlus out, int version) throws IOException
    {
        // Only serialize the fields that vary - consistency is always SERIAL, writeType is always CAS
        out.writeUnsignedVInt32(received);
        out.writeUnsignedVInt32(blockFor);
    }

    @Override
    protected long serializedSizeSpecificFields(int version)
    {
        return TypeSizes.sizeofUnsignedVInt(received) +
               TypeSizes.sizeofUnsignedVInt(blockFor);
    }

    public static AccordWriteExhaustedException deserializeFields(String message, DataInputPlus in, int version) throws IOException
    {
        int received = in.readUnsignedVInt32();
        int blockFor = in.readUnsignedVInt32();
        return new AccordWriteExhaustedException(received, blockFor, message);
    }

    @Override
    public CassandraExceptionCode getCassandraExceptionCode()
    {
        return CassandraExceptionCode.ACCORD_WRITE_EXHAUSTED;
    }
}
