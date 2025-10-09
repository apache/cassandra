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

package org.apache.cassandra.db;

import java.io.IOException;

import org.apache.cassandra.io.UnversionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

public enum ReadKind
{
    UNTRACKED       (0),
    TRACKED_DATA    (1),
    TRACKED_SUMMARY (2);

    public final int id;

    ReadKind(int id)
    {
        this.id = id;
    }

    public boolean isTracked()
    {
        return this != UNTRACKED;
    }

    private static final ReadKind[] idToKindMapping;
    static
    {
        int maxId = -1;
        for (ReadKind kind : ReadKind.values())
            maxId = Math.max(maxId, kind.id);
        idToKindMapping = new ReadKind[maxId + 1];
        for (ReadKind kind : ReadKind.values())
            idToKindMapping[kind.id] = kind;
    }

    public static ReadKind fromId(int id)
    {
        if (id < 0 || id >= idToKindMapping.length || idToKindMapping[id] == null)
            throw new IllegalArgumentException("Unknown Kind id: " + id);
        return idToKindMapping[id];
    }

    public static final UnversionedSerializer<ReadKind> serializer = new UnversionedSerializer<>()
    {
        @Override
        public void serialize(ReadKind kind, DataOutputPlus out) throws IOException
        {
            out.writeByte(kind.id);
        }

        @Override
        public ReadKind deserialize(DataInputPlus in) throws IOException
        {
            return fromId(in.readUnsignedByte());
        }

        @Override
        public long serializedSize(ReadKind kind)
        {
            return TypeSizes.BYTE_SIZE;
        }
    };
}
