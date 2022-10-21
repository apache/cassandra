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

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

public class NullableSerializer
{
    private NullableSerializer() {}

    public static <T> void serializeNullable(T t, DataOutputPlus out, int version, IVersionedSerializer<T> serializer) throws IOException
    {
        out.writeBoolean(t != null);
        if (t != null)
            serializer.serialize(t, out, version);
    }

    public static <T> T deserializeNullable(DataInputPlus in, int version, IVersionedSerializer<T> serializer) throws IOException
    {
        return in.readBoolean() ? serializer.deserialize(in, version) : null;
    }

    public static <T> long serializedSizeNullable(T t, int version, IVersionedSerializer<T> serializer)
    {
        return TypeSizes.BOOL_SIZE + (t != null ? serializer.serializedSize(t, version) : 0);
    }
}
