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

import com.google.common.base.Preconditions;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.repair.RepairJobDesc;

/**
 * Base class of all repair related request/response messages.
 *
 * @since 2.0
 */
public abstract class RepairMessage
{
    public static final IVersionedSerializer<RepairMessage> serializer = new RepairMessageSerializer();

    public static interface MessageSerializer<T extends RepairMessage> extends IVersionedSerializer<T> {}

    public static final int MIN_MESSAGING_VERSION = MessagingService.VERSION_40;
    private static final String MIXED_MODE_ERROR = "Some nodes involved in repair are on an incompatible major version. " +
                                                   "Repair is not supported in mixed major version clusters.";

    public enum Type
    {
        VALIDATION_REQUEST(0, ValidationRequest.serializer),
        VALIDATION_COMPLETE(1, ValidationComplete.serializer),
        SYNC_REQUEST(2, SyncRequest.serializer),
        SYNC_COMPLETE(3, SyncComplete.serializer),
        PREPARE_MESSAGE(5, PrepareMessage.serializer),
        SNAPSHOT(6, SnapshotMessage.serializer),
        CLEANUP(7, CleanupMessage.serializer),

        CONSISTENT_REQUEST(8, PrepareConsistentRequest.serializer),
        CONSISTENT_RESPONSE(9, PrepareConsistentResponse.serializer),
        FINALIZE_PROPOSE(10, FinalizePropose.serializer),
        FINALIZE_PROMISE(11, FinalizePromise.serializer),
        FINALIZE_COMMIT(12, FinalizeCommit.serializer),
        FAILED_SESSION(13, FailSession.serializer),
        STATUS_REQUEST(14, StatusRequest.serializer),
        STATUS_RESPONSE(15, StatusResponse.serializer),
        ASYMMETRIC_SYNC_REQUEST(16, AsymmetricSyncRequest.serializer);

        private final byte type;
        private final MessageSerializer<RepairMessage> serializer;

        private Type(int type, MessageSerializer<RepairMessage> serializer)
        {
            this.type = (byte) type;
            this.serializer = serializer;
        }

        public static Type fromByte(byte b)
        {
            for (Type t : values())
            {
               if (t.type == b)
                   return t;
            }
            throw new IllegalArgumentException("Unknown RepairMessage.Type: " + b);
        }
    }

    public final Type messageType;
    public final RepairJobDesc desc;

    protected RepairMessage(Type messageType, RepairJobDesc desc)
    {
        this.messageType = messageType;
        this.desc = desc;
    }

    public MessageOut<RepairMessage> createMessage()
    {
        return new MessageOut<>(MessagingService.Verb.REPAIR_MESSAGE, this, RepairMessage.serializer);
    }


    public static class RepairMessageSerializer implements MessageSerializer<RepairMessage>
    {
        public void serialize(RepairMessage message, DataOutputPlus out, int version) throws IOException
        {
            Preconditions.checkArgument(version >= MIN_MESSAGING_VERSION, MIXED_MODE_ERROR);
            out.write(message.messageType.type);
            message.messageType.serializer.serialize(message, out, version);
        }

        public RepairMessage deserialize(DataInputPlus in, int version) throws IOException
        {
            Preconditions.checkArgument(version >= MIN_MESSAGING_VERSION, MIXED_MODE_ERROR);
            RepairMessage.Type messageType = RepairMessage.Type.fromByte(in.readByte());
            return messageType.serializer.deserialize(in, version);
        }

        public long serializedSize(RepairMessage message, int version)
        {
            Preconditions.checkArgument(version >= MIN_MESSAGING_VERSION, MIXED_MODE_ERROR);
            long size = 1; // for messageType byte
            size += message.messageType.serializer.serializedSize(message, version);
            return size;
        }
    }
}
