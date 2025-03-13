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

package org.apache.cassandra.io;

import java.io.IOException;

import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

public class EmbeddedAsymmetricVersionedSerializer<In, Out, Version> implements IVersionedAsymmetricSerializer<In, Out>, AsymmetricUnversionedSerializer<In, Out>
{
    private final Version version;
    private final UnversionedSerializer<Version> versionSerializer;
    private final AsymmetricVersionedSerializer<In, Out, Version> delegate;

    public EmbeddedAsymmetricVersionedSerializer(Version version,
                                                 UnversionedSerializer<Version> versionSerializer,
                                                 AsymmetricVersionedSerializer<In, Out, Version> delegate)
    {
        this.version = version;
        this.versionSerializer = versionSerializer;
        this.delegate = delegate;
    }

    @Override
    public void serialize(In t, DataOutputPlus out, int msgVersion) throws IOException
    {
        serialize(t, out);
    }

    @Override
    public void serialize(In t, DataOutputPlus out) throws IOException
    {
        versionSerializer.serialize(version, out);
        delegate.serialize(t, out, version);
    }

    @Override
    public Out deserialize(DataInputPlus in, int msgVersion) throws IOException
    {
        return deserialize(in);
    }

    @Override
    public Out deserialize(DataInputPlus in) throws IOException
    {
        Version version = versionSerializer.deserialize(in);
        return delegate.deserialize(in, version);
    }

    public Version deserializeVersion(DataInputPlus in) throws IOException
    {
        return versionSerializer.deserialize(in);
    }

    @Override
    public long serializedSize(In t, int msgVersion)
    {
        return serializedSize(t);
    }

    @Override
    public long serializedSize(In t)
    {
        return versionSerializer.serializedSize(version)
               + delegate.serializedSize(t, version);
    }
}
