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

package org.apache.cassandra.tcm;

import java.io.IOException;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.tcm.serialization.MetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

public class MetadataKey
{
    public static final Serializer serializer = new Serializer();

    public final String id;

    public MetadataKey(String id)
    {
        this.id = id;
    }

    @Override
    public String toString()
    {
        return id;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (!(o instanceof MetadataKey)) return false;
        MetadataKey that = (MetadataKey) o;
        return id.equals(that.id);
    }

    @Override
    public int hashCode()
    {
        return id.hashCode();
    }

    public static final class Serializer implements MetadataSerializer<MetadataKey>
    {
        @Override
        public void serialize(MetadataKey t, DataOutputPlus out, Version version) throws IOException
        {
            out.writeUTF(t.id);
        }

        @Override
        public MetadataKey deserialize(DataInputPlus in, Version version) throws IOException
        {
            return new MetadataKey(in.readUTF());
        }

        @Override
        public long serializedSize(MetadataKey t, Version version)
        {
            return TypeSizes.sizeof(t.id);
        }
    }
}
