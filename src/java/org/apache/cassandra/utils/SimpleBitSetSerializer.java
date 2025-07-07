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

package org.apache.cassandra.utils;

import java.io.IOException;

import accord.utils.SimpleBitSet;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.UnversionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

public class SimpleBitSetSerializer implements UnversionedSerializer<SimpleBitSet>
{
    public static final SimpleBitSetSerializer instance = new SimpleBitSetSerializer();

    @Override
    public void serialize(SimpleBitSet t, DataOutputPlus out) throws IOException
    {
        long[] raw = SimpleBitSet.SerializationSupport.getArray(t);
        out.writeUnsignedVInt32(raw.length);
        for (int i = 0; i < raw.length; i++)
            out.writeVInt(raw[i]);
    }

    @Override
    public SimpleBitSet deserialize(DataInputPlus in) throws IOException
    {
        int size = in.readUnsignedVInt32();
        long[] raw = new long[size];
        for (int i = 0; i < size; i++)
            raw[i] = in.readVInt();
        return SimpleBitSet.SerializationSupport.construct(raw);
    }

    @Override
    public long serializedSize(SimpleBitSet t)
    {
        long[] raw = SimpleBitSet.SerializationSupport.getArray(t);
        long size = TypeSizes.sizeofVInt(raw.length);
        for (int i = 0; i < raw.length; i++)
            size += TypeSizes.sizeofVInt(raw[i]);
        return size;
    }
}
