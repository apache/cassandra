

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
package org.apache.cassandra.net;

import java.io.IOException;
import java.util.Map;

import com.google.common.collect.Maps;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ByteArrayUtil;


class CustomParamsSerializer implements IVersionedSerializer<Map<String,byte[]>>
{
    public static final CustomParamsSerializer serializer = new CustomParamsSerializer();

    @Override
    public void serialize(Map<String, byte[]> t, DataOutputPlus out, int version) throws IOException
    {
        out.writeUnsignedVInt32(t.size());
        for (Map.Entry<String, byte[]> e : t.entrySet())
        {
            out.writeUTF(e.getKey());
            ByteArrayUtil.writeWithVIntLength(e.getValue(), out);
        }
    }

    @Override
    public long serializedSize(Map<String, byte[]> t, int version)
    {
        int size = TypeSizes.sizeofUnsignedVInt(t.size());
        for (Map.Entry<String,byte[]> e : t.entrySet())
        {
            size += TypeSizes.sizeof(e.getKey());
            size += ByteArrayUtil.serializedSizeWithVIntLength(e.getValue());
        }
        return size;
    }

    @Override
    public Map<String, byte[]> deserialize(DataInputPlus in, int version) throws IOException
    {
        int entries = in.readUnsignedVInt32();
        Map<String, byte[]> customParams = Maps.newHashMapWithExpectedSize(entries);

        for (int i = 0 ; i < entries ; ++i)
            customParams.put(in.readUTF(), ByteArrayUtil.readWithVIntLength(in));

        return customParams;
    }

}
