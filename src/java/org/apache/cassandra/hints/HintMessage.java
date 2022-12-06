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
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.UUID;

import com.google.common.primitives.Ints;

import javax.annotation.Nullable;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.exceptions.UnknownTableException;
import org.apache.cassandra.io.IVersionedAsymmetricSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.TrackedDataInputPlus;
import org.apache.cassandra.schema.TableId;
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
public final class HintMessage implements SerializableHintMessage
{
    public static final IVersionedAsymmetricSerializer<SerializableHintMessage, HintMessage> serializer = new Serializer();

    final UUID hostId;

    @Nullable // can be null if we fail do decode the hint because of an unknown table id in it
    final Hint hint;

    @Nullable // will usually be null, unless a hint deserialization fails due to an unknown table id
    final TableId unknownTableID;

    HintMessage(UUID hostId, Hint hint)
    {
        assert hint != null;
        this.hostId = hostId;
        this.hint = hint;
        this.unknownTableID = null;
    }

    HintMessage(UUID hostId, TableId unknownTableID)
    {
        this.hostId = hostId;
        this.hint = null;
        this.unknownTableID = unknownTableID;
    }

    public static class Serializer implements IVersionedAsymmetricSerializer<SerializableHintMessage, HintMessage>
    {
        public long serializedSize(SerializableHintMessage obj, int version)
        {
            if (obj instanceof HintMessage)
            {
                HintMessage message = (HintMessage) obj;

                Objects.requireNonNull(message.hint); // we should never *send* a HintMessage with null hint

                long size = UUIDSerializer.serializer.serializedSize(message.hostId, version);
                long hintSize = Hint.serializer.serializedSize(message.hint, version);
                size += TypeSizes.sizeofUnsignedVInt(hintSize);
                size += hintSize;

                return size;
            }
            else if (obj instanceof Encoded)
            {
                Encoded message = (Encoded) obj;

                if (version != message.version)
                    throw new IllegalArgumentException("serializedSize() called with non-matching version " + version);

                long size = UUIDSerializer.serializer.serializedSize(message.hostId, version);
                size += TypeSizes.sizeofUnsignedVInt(message.hint.remaining());
                size += message.hint.remaining();
                return size;
            }
            else
            {
                throw new IllegalStateException("Unexpected type: " + obj);
            }
        }

        public void serialize(SerializableHintMessage obj, DataOutputPlus out, int version) throws IOException
        {
            if (obj instanceof HintMessage)
            {
                HintMessage message = (HintMessage) obj;

                Objects.requireNonNull(message.hint); // we should never *send* a HintMessage with null hint

                UUIDSerializer.serializer.serialize(message.hostId, out, version);

                /*
                 * We are serializing the hint size so that the receiver of the message could gracefully handle
                 * deserialize failure when a table had been dropped, by simply skipping the unread bytes.
                 */
                out.writeUnsignedVInt(Hint.serializer.serializedSize(message.hint, version));

                Hint.serializer.serialize(message.hint, out, version);
            }
            else if (obj instanceof Encoded)
            {
                Encoded message = (Encoded) obj;

                if (version != message.version)
                    throw new IllegalArgumentException("serialize() called with non-matching version " + version);

                UUIDSerializer.serializer.serialize(message.hostId, out, version);
                out.writeUnsignedVInt32(message.hint.remaining());
                out.write(message.hint);
            }
            else
            {
                throw new IllegalStateException("Unexpected type: " + obj);
            }
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
            catch (UnknownTableException e)
            {
                in.skipBytes(Ints.checkedCast(hintSize - countingIn.getBytesRead()));
                return new HintMessage(hostId, e.id);
            }
        }
    }

    /**
     * A specialized version of {@link HintMessage} that takes an already encoded in a bytebuffer hint and sends it verbatim.
     *
     * An optimization for when dispatching a hint file of the current messaging version to a node of the same messaging version,
     * which is the most common case. Saves on extra ByteBuffer allocations one redundant hint deserialization-serialization cycle.
     *
     * Never deserialized as an HintMessage.Encoded - the receiving side will always deserialize the message as vanilla
     * {@link HintMessage}.
     */
    static final class Encoded implements SerializableHintMessage
    {
        private final UUID hostId;
        private final ByteBuffer hint;
        private final int version;

        Encoded(UUID hostId, ByteBuffer hint, int version)
        {
            this.hostId = hostId;
            this.hint = hint;
            this.version = version;
        }

        public long getHintCreationTime()
        {
            return Hint.serializer.getHintCreationTime(hint, version);
        }
    }
}
