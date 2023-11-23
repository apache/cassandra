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

package org.apache.cassandra.tcm.extensions;

import java.io.IOException;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.tcm.MetadataKey;
import org.apache.cassandra.tcm.serialization.MetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.utils.FBUtilities;

public class ExtensionKey<V, K extends ExtensionValue<V>> extends MetadataKey
{
    public static final Serializer serializer = new Serializer();
    public final Class<K> valueType;

    public ExtensionKey(String id, Class<K> valueType)
    {
        super(id);
        this.valueType = valueType;
    }

    public K newValue()
    {
        return valueType.cast(FBUtilities.construct(valueType.getName(), "extension value"));
    }

    public static final class Serializer implements MetadataSerializer<ExtensionKey<?, ?>>
    {
        @Override
        public void serialize(ExtensionKey<?, ?> t, DataOutputPlus out, Version version) throws IOException
        {
            out.writeUTF(t.id);
            out.writeUTF(t.valueType.getName());
        }

        @Override
        public ExtensionKey<?, ?> deserialize(DataInputPlus in, Version version) throws IOException
        {
            String id = in.readUTF();
            String valType = in.readUTF();
            return new ExtensionKey(id, FBUtilities.classForName(valType, "value type"));
        }

        @Override
        public long serializedSize(ExtensionKey<?, ?> t, Version version)
        {
            return TypeSizes.sizeof(t.id) + TypeSizes.sizeof(t.valueType.getName());
        }
    }
}

