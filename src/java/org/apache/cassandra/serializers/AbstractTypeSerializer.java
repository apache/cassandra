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

package org.apache.cassandra.serializers;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ByteBufferUtil;

public class AbstractTypeSerializer
{
    public void serialize(AbstractType<?> type, DataOutputPlus out) throws IOException
    {
        ByteBufferUtil.writeWithVIntLength(UTF8Type.instance.decompose(type.toString()), out);
    }

    public void serializeList(List<AbstractType<?>> types, DataOutputPlus out) throws IOException
    {
        out.writeUnsignedVInt32(types.size());
        for (AbstractType<?> type : types)
            serialize(type, out);
    }

    public AbstractType<?> deserialize(DataInputPlus in) throws IOException
    {
        ByteBuffer raw = ByteBufferUtil.readWithVIntLength(in);
        return TypeParser.parse(UTF8Type.instance.compose(raw));
    }

    public List<AbstractType<?>> deserializeList(DataInputPlus in) throws IOException
    {
        int size = (int) in.readUnsignedVInt();
        List<AbstractType<?>> types = new ArrayList<>(size);
        for (int i = 0; i < size; i++)
            types.add(deserialize(in));
        return types;
    }

    public long serializedSize(AbstractType<?> type)
    {
        return ByteBufferUtil.serializedSizeWithVIntLength(UTF8Type.instance.decompose(type.toString()));
    }

    public long serializedListSize(List<AbstractType<?>> types)
    {
        long size = TypeSizes.sizeofUnsignedVInt(types.size());
        for (AbstractType<?> type : types)
            size += serializedSize(type);
        return size;
    }
}