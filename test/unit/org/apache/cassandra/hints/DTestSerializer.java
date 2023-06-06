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
import java.util.UUID;

import com.google.common.primitives.Ints;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.exceptions.UnknownTableException;
import org.apache.cassandra.io.IVersionedAsymmetricSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.TrackedDataInputPlus;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.utils.UUIDSerializer;

// Fake serializer for dtests. Located in hints package to avoid publishing package-private fields.
public class DTestSerializer implements IVersionedAsymmetricSerializer<SerializableHintMessage, HintMessage>
{
    public void serialize(SerializableHintMessage obj, DataOutputPlus out, int version) throws IOException
    {
        HintMessage message;
        if (!(obj instanceof HintMessage) || (message = (HintMessage) obj).hint != null)
        {
            HintMessage.serializer.serialize(obj, out, version);
            return;
        }

        UUIDSerializer.serializer.serialize(message.hostId, out, version);
        out.writeUnsignedVInt32(0);
        message.unknownTableID.serialize(out);
    }

    public HintMessage deserialize(DataInputPlus in, int version) throws IOException
    {
        UUID hostId = UUIDSerializer.serializer.deserialize(in, version);

        long hintSize = in.readUnsignedVInt();
        TrackedDataInputPlus countingIn = new TrackedDataInputPlus(in);
        if (hintSize == 0)
            return new HintMessage(hostId, TableId.deserialize(countingIn));

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

    public long serializedSize(SerializableHintMessage obj, int version)
    {
        HintMessage message;
        if (!(obj instanceof HintMessage) || (message = (HintMessage) obj).hint != null)
            return HintMessage.serializer.serializedSize(obj, version);

        long size = UUIDSerializer.serializer.serializedSize(message.hostId, version);
        size += TypeSizes.sizeofUnsignedVInt(0);
        size += UUIDSerializer.serializer.serializedSize(message.unknownTableID.asUUID(), version);
        return size;
    }
}
