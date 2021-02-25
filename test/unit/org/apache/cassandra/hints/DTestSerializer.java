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
import java.util.Objects;
import java.util.UUID;

import com.google.common.primitives.Ints;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.UnknownColumnFamilyException;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.TrackedDataInputPlus;
import org.apache.cassandra.utils.UUIDSerializer;

// Fake serializer for dtests. Located in hints package to avoid publishing package-private fields.
public class DTestSerializer implements IVersionedSerializer<HintMessage>
{
    public long serializedSize(HintMessage message, int version)
    {
        if (message.hint != null)
            return HintMessage.serializer.serializedSize(message, version);

        long size = UUIDSerializer.serializer.serializedSize(message.hostId, version);
        size += TypeSizes.sizeofUnsignedVInt(0);
        size += UUIDSerializer.serializer.serializedSize(message.unknownTableID, version);
        return size;
    }

    public void serialize(HintMessage message, DataOutputPlus out, int version) throws IOException
    {
        if (message.hint != null)
        {
            HintMessage.serializer.serialize(message, out, version);
            return;
        }

        UUIDSerializer.serializer.serialize(message.hostId, out, version);
        out.writeUnsignedVInt(0);
        UUIDSerializer.serializer.serialize(message.unknownTableID, out, version);
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

        if (hintSize == 0)
            return new HintMessage(hostId, UUIDSerializer.serializer.deserialize(in, version));

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