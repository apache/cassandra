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

import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.serialization.Version;

public abstract class AbstractExtensionValue<V> implements ExtensionValue<V>
{
    private Epoch lastModified = Epoch.EMPTY;
    private V value = null;

    @Override
    public V withLastModified(Epoch lastModified)
    {
        this.lastModified = lastModified;
        return (V)this;
    }

    public Epoch lastModified()
    {
        return lastModified;
    }

    @Override
    public void setValue(V value)
    {
        this.value = value;
    }

    @Override
    public V getValue()
    {
        return value;
    }

    public void serialize(DataOutputPlus out, Version version) throws IOException
    {
        Epoch.serializer.serialize(lastModified(), out, version);
        serializeInternal(out, version);

    }
    abstract void serializeInternal(DataOutputPlus out, Version version) throws IOException;

    public void deserialize(DataInputPlus in, Version v) throws IOException
    {
        Epoch e = Epoch.serializer.deserialize(in, v);
        withLastModified(e);
        deserializeInternal(in, v);
    }
    abstract void deserializeInternal(DataInputPlus in, Version version) throws IOException;

    public long serializedSize(Version v)
    {
        long size = Epoch.serializer.serializedSize(lastModified(), v);
        size += serializedSizeInternal(v);
        return size;
    }
    abstract long serializedSizeInternal(Version v);
}
