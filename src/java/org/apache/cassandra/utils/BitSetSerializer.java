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
import java.util.BitSet;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.UnversionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

public class BitSetSerializer implements UnversionedSerializer<BitSet>
{
    public static final BitSetSerializer instance = new BitSetSerializer();

    @Override
    public void serialize(BitSet t, DataOutputPlus out) throws IOException
    {
        byte[] bytes = t.toByteArray();
        out.writeUnsignedVInt32(bytes.length);
        out.write(bytes);
    }

    @Override
    public BitSet deserialize(DataInputPlus in) throws IOException
    {
        int size = in.readUnsignedVInt32();
        byte[] bytes = new byte[size];
        in.readFully(bytes);
        return BitSet.valueOf(bytes);
    }

    @Override
    public long serializedSize(BitSet t)
    {
        int size = t.toByteArray().length;
        return TypeSizes.sizeofUnsignedVInt(size) + size;
    }
}
