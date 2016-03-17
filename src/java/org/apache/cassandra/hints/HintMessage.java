/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.hints;

import java.io.IOException;
import java.util.Objects;
import java.util.UUID;

import javax.annotation.Nullable;

import com.google.common.primitives.Ints;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.UnknownColumnFamilyException;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.io.util.TrackedDataInputPlus;
import org.apache.cassandra.utils.UUIDSerializer;

/**
 * The message we use to dispatch and forward hints.
 *
 * Encodes the host id the hint is meant for and the hint itself.
 * We use the host id to determine whether we should store or apply the hint:
 * 1. If host id equals to the receiving node host id, then we apply the hint
 * 2. If host id is different from the receiving node's host id, then we store the hint
 *
 * Scenario (1) means that we are dealing with regular hint dispatch.
 * Scenario (2) means that we got a hint from a node that's going through decommissioning and is streaming its hints
 * elsewhere first.
 */
public final class HintMessage
{
    public static final IVersionedSerializer<HintMessage> serializer = new Serializer();

    final UUID hostId;

    @Nullable // can be null if we fail do decode the hint because of an unknown table id in it
    final Hint hint;

    @Nullable // will usually be null, unless a hint deserialization fails due to an unknown table id
    final UUID unknownTableID;

    HintMessage(UUID hostId, Hint hint)
    {
        this.hostId = hostId;
        this.hint = hint;
        this.unknownTableID = null;
    }

    HintMessage(UUID hostId, UUID unknownTableID)
    {
        this.hostId = hostId;
        this.hint = null;
        this.unknownTableID = unknownTableID;
    }

    public MessageOut<HintMessage> createMessageOut()
    {
        return new MessageOut<>(MessagingService.Verb.HINT, this, serializer);
    }

    public static class Serializer implements IVersionedSerializer<HintMessage>
    {
        public long serializedSize(HintMessage message, int version)
        {
            long size = UUIDSerializer.serializer.serializedSize(message.hostId, version);

            long hintSize = Hint.serializer.serializedSize(message.hint, version);
            size += TypeSizes.sizeofUnsignedVInt(hintSize);
            size += hintSize;

            return size;
        }

        public void serialize(HintMessage message, DataOutputPlus out, int version) throws IOException
        {
            Objects.requireNonNull(message.hint); // we should never *send* a HintMessage with null hint

            UUIDSerializer.serializer.serialize(message.hostId, out, version);

            /*
             * We are serializing the hint size so that the receiver of the message could gracefully handle
             * deserialize failure when a table had been dropped, by simply skipping the unread bytes.
             */
            out.writeUnsignedVInt(Hint.serializer.serializedSize(message.hint, version));

            Hint.serializer.serialize(message.hint, out, version);
        }

        /*
         * It's not an exceptional scenario to have a hints file streamed that have partition updates for tables
         * that don't exist anymore. We want to handle that case gracefully instead of dropping the connection for every
         * one of them.
         */
        public HintMessage deserialize(DataInputPlus in, int version) throws IOException
        {
            UUID hostId = UUIDSerializer.serializer.deserialize(in, version);

            long hintSize = in.readUnsignedVInt();
            TrackedDataInputPlus countingIn = new TrackedDataInputPlus(in);
            try
            {
                return new HintMessage(hostId, Hint.serializer.deserialize(countingIn, version));
            }
            catch (UnknownColumnFamilyException e)
            {
                in.skipBytes(Ints.checkedCast(hintSize - countingIn.getBytesRead()));
                return new HintMessage(hostId, e.cfId);
            }
        }
    }
}
