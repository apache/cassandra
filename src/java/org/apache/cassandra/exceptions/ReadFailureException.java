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
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.utils.CollectionSerializers;

public class ReadFailureException extends RequestFailureException
{
    public final boolean dataPresent;

    public ReadFailureException(ConsistencyLevel consistency, int received, int blockFor, boolean dataPresent, Map<InetAddressAndPort, RequestFailureReason> failureReasonByEndpoint)
    {
        super(ExceptionCode.READ_FAILURE, consistency, received, blockFor, ImmutableMap.copyOf(failureReasonByEndpoint));
        this.dataPresent = dataPresent;
    }

    protected ReadFailureException(String msg, ConsistencyLevel consistency, int received, int blockFor, boolean dataPresent, Map<InetAddressAndPort, RequestFailureReason> failureReasonByEndpoint)
    {
        super(ExceptionCode.READ_FAILURE, msg, consistency, received, blockFor, failureReasonByEndpoint);
        this.dataPresent = dataPresent;
    }

    public ReadFailureException(String msg, ConsistencyLevel consistency, int received, int blockFor, boolean dataPresent, Map<InetAddressAndPort, RequestFailureReason> failureReasonByEndpoint, Throwable cause)
    {
        super(ExceptionCode.READ_FAILURE, msg, consistency, received, blockFor, failureReasonByEndpoint, cause);
        this.dataPresent = dataPresent;
    }

    public ReadFailureException(ReadFailureException rfe)
    {
        super(ExceptionCode.READ_FAILURE, rfe.getMessage(), rfe.consistency, rfe.received, rfe.blockFor, rfe.failureReasonByEndpoint, rfe);
        this.dataPresent = rfe.dataPresent;
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

        out.writeBoolean(dataPresent);
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

        size += TypeSizes.BOOL_SIZE; // dataPresent
        return size;
    }

    static ReadFailureException deserializeFields(String message, DataInputPlus in, int version) throws IOException
    {
        DeserializedFields fields = deserializeBaseFields(in, version);
        return new ReadFailureException(message, fields.consistency, fields.received, fields.blockFor, fields.dataPresent, fields.failures);
    }

    /**
     * Helper class to hold deserialized base fields for subclasses to use.
     */
    static class DeserializedFields
    {
        final ConsistencyLevel consistency;
        final int received;
        final int blockFor;
        final Map<InetAddressAndPort, RequestFailureReason> failures;
        final boolean dataPresent;

        DeserializedFields(ConsistencyLevel consistency, int received, int blockFor,
                          Map<InetAddressAndPort, RequestFailureReason> failures, boolean dataPresent)
        {
            this.consistency = consistency;
            this.received = received;
            this.blockFor = blockFor;
            this.failures = failures;
            this.dataPresent = dataPresent;
        }
    }

    /**
     * Deserialize the base fields common to ReadFailureException and its subclasses.
     * Subclasses should call this method and then read any additional fields.
     */
    static DeserializedFields deserializeBaseFields(DataInputPlus in, int version) throws IOException
    {
        ConsistencyLevel consistency = ConsistencyLevel.fromCode(in.readUnsignedByte());
        int received = in.readUnsignedVInt32();
        int blockFor = in.readUnsignedVInt32();

        // Deserialize failure reason map
        Map<InetAddressAndPort, RequestFailureReason> failures =
            CollectionSerializers.deserializeMap(in, version,
                                                 InetAddressAndPort.Serializer.inetAddressAndPortSerializer,
                                                 RequestFailureReason.serializer);

        boolean dataPresent = in.readBoolean();
        return new DeserializedFields(consistency, received, blockFor, failures, dataPresent);
    }

    @Override
    public CassandraExceptionCode getCassandraExceptionCode()
    {
        return CassandraExceptionCode.READ_FAILURE;
    }
}
