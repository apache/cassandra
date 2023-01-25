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

package org.apache.cassandra.service.accord.serializers;

import java.io.IOException;

import accord.messages.SimpleReply;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

public class EnumSerializer<E extends Enum<E>> implements IVersionedSerializer<E>
{
    public static final EnumSerializer<SimpleReply> simpleReply = new EnumSerializer<>(SimpleReply.class);

    // TODO: should use something other than ordinal for ser/deser
    final E[] values;

    public EnumSerializer(Class<E> clazz)
    {
        this.values = clazz.getEnumConstants();
    }

    @Override
    public void serialize(E t, DataOutputPlus out, int version) throws IOException
    {
        out.writeUnsignedVInt32(t.ordinal());
    }

    @Override
    public E deserialize(DataInputPlus in, int version) throws IOException
    {
        return values[in.readUnsignedVInt32()];
    }

    @Override
    public long serializedSize(E t, int version)
    {
        return TypeSizes.sizeofUnsignedVInt(t.ordinal());
    }
}
