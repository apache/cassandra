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
import java.util.Map;

import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.utils.CollectionSerializers;

public class WriteFailureException extends RequestFailureException
{
    public final WriteType writeType;

    public WriteFailureException(ConsistencyLevel consistency, int received, int blockFor, WriteType writeType, Map<InetAddressAndPort, RequestFailureReason> failureReasonByEndpoint)
    {
        super(ExceptionCode.WRITE_FAILURE, consistency, received, blockFor, ImmutableMap.copyOf(failureReasonByEndpoint));
        this.writeType = writeType;
    }

    @Override
    protected void serializeSpecificFields(DataOutputPlus out, int version) throws IOException
    {
        out.writeByte(consistency.code);
        out.writeUnsignedVInt32(received);
        out.writeUnsignedVInt32(blockFor);

        // Serialize failure reason map
        CollectionSerializers.serializeMap(failureReasonByEndpoint, out, version,
                                           InetAddressAndPort.Serializer.inetAddressAndPortSerializer,
                                           RequestFailureReason.serializer);

        WriteType.serializer.serialize(writeType, out);
    }

    @Override
    protected long serializedSizeSpecificFields(int version)
    {
        long size = TypeSizes.BYTE_SIZE + // consistency
                    TypeSizes.sizeofUnsignedVInt(received) +
                    TypeSizes.sizeofUnsignedVInt(blockFor);

        size += CollectionSerializers.serializedMapSize(failureReasonByEndpoint, version,
                                                        InetAddressAndPort.Serializer.inetAddressAndPortSerializer,
                                                        RequestFailureReason.serializer);

        size += WriteType.serializer.serializedSize(writeType);
        return size;
    }

    static WriteFailureException deserializeFields(String message, DataInputPlus in, int version) throws IOException
    {
        ConsistencyLevel consistency = ConsistencyLevel.fromCode(in.readUnsignedByte());
        int received = in.readUnsignedVInt32();
        int blockFor = in.readUnsignedVInt32();

        // Deserialize failure reason map
        Map<InetAddressAndPort, RequestFailureReason> failures =
            CollectionSerializers.deserializeMap(in, version,
                                                 InetAddressAndPort.Serializer.inetAddressAndPortSerializer,
                                                 RequestFailureReason.serializer);

        WriteType writeType = WriteType.serializer.deserialize(in);
        return new WriteFailureException(consistency, received, blockFor, writeType, failures);
    }

    @Override
    public CassandraExceptionCode getCassandraExceptionCode()
    {
        return CassandraExceptionCode.WRITE_FAILURE;
    }
}
