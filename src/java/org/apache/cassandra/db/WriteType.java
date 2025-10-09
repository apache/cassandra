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

/**
 * Identifier for what type of operation timed out.  This type is driver facing as a String, but some drivers convert
 * this to an enum, meaning any changes to this type require protocol changes and driver support.
 */
public enum WriteType
{
    SIMPLE         (0),
    BATCH          (1),
    UNLOGGED_BATCH (2),
    COUNTER        (3),
    BATCH_LOG      (4),
    CAS            (5),
    VIEW           (6),
    CDC            (7);
    //TODO update client protocol to support "TRANSACTION"

    // used by the messaging service
    public final int id;

    private static final WriteType[] idToTypeMapping;
    static
    {
        int maxId = -1;
        for (WriteType type : WriteType.values())
            maxId = Math.max(maxId, type.id);
        idToTypeMapping = new WriteType[maxId + 1];
        for (WriteType type : WriteType.values())
        {
            if (idToTypeMapping[type.id] != null)
                throw new IllegalStateException("Duplicate id " + type.id);
            idToTypeMapping[type.id] = type;
        }
    }

    WriteType(int id)
    {
        this.id = id;
    }

    public static WriteType fromId(int id) throws IOException
    {
        if (id < 0 || id >= idToTypeMapping.length)
            throw new IllegalArgumentException("Unknown WriteType id: " + id);
        return idToTypeMapping[id];
    }

    public static final UnversionedSerializer<WriteType> serializer = new UnversionedSerializer<>()
    {
        @Override
        public void serialize(WriteType writeType, DataOutputPlus out) throws IOException
        {
            out.writeByte(writeType.id);
        }

        @Override
        public WriteType deserialize(DataInputPlus in) throws IOException
        {
            return fromId(in.readUnsignedByte());
        }

        @Override
        public long serializedSize(WriteType writeType)
        {
            return TypeSizes.BYTE_SIZE;
        }
    };
}
