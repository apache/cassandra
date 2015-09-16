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
package org.apache.cassandra.hints;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.UUIDSerializer;

/**
 * A specialized version of {@link HintMessage} that takes an already encoded in a bytebuffer hint and sends it verbatim.
 *
 * An optimization for when dispatching a hint file of the current messaging version to a node of the same messaging version,
 * which is the most common case. Saves on extra ByteBuffer allocations one redundant hint deserialization-serialization cycle.
 *
 * Never deserialized as an EncodedHintMessage - the receiving side will always deserialize the message as vanilla
 * {@link HintMessage}.
 */
final class EncodedHintMessage
{
    private static final IVersionedSerializer<EncodedHintMessage> serializer = new Serializer();

    private final UUID hostId;
    private final ByteBuffer hint;
    private final int version;

    EncodedHintMessage(UUID hostId, ByteBuffer hint, int version)
    {
        this.hostId = hostId;
        this.hint = hint;
        this.version = version;
    }

    MessageOut<EncodedHintMessage> createMessageOut()
    {
        return new MessageOut<>(MessagingService.Verb.HINT, this, serializer);
    }

    private static class Serializer implements IVersionedSerializer<EncodedHintMessage>
    {
        public long serializedSize(EncodedHintMessage message, int version)
        {
            if (version != message.version)
                throw new IllegalArgumentException("serializedSize() called with non-matching version " + version);

            long size = UUIDSerializer.serializer.serializedSize(message.hostId, version);
            size += TypeSizes.sizeofUnsignedVInt(message.hint.remaining());
            size += message.hint.remaining();
            return size;
        }

        public void serialize(EncodedHintMessage message, DataOutputPlus out, int version) throws IOException
        {
            if (version != message.version)
                throw new IllegalArgumentException("serialize() called with non-matching version " + version);

            UUIDSerializer.serializer.serialize(message.hostId, out, version);
            out.writeUnsignedVInt(message.hint.remaining());
            out.write(message.hint);
        }

        public EncodedHintMessage deserialize(DataInputPlus in, int version) throws IOException
        {
            throw new UnsupportedOperationException();
        }
    }
}
