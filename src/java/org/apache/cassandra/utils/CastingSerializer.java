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

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

/**
 * Utility for serializing/deserializing from/into generic interface fields where we know (and require) the
 * generic fields to be implementation specific classes
 * @param <Generic>
 * @param <Specific>
 */
public class CastingSerializer<Generic, Specific extends Generic> implements IVersionedSerializer<Generic>
{
    private final Class<Specific> specificClass;
    private final IVersionedSerializer<Specific> specificSerializer;

    public CastingSerializer(Class<Specific> specificClass, IVersionedSerializer<Specific> specificSerializer)
    {
        this.specificClass = specificClass;
        this.specificSerializer = specificSerializer;
    }

    @Override
    public void serialize(Generic generic, DataOutputPlus out, int version) throws IOException
    {
        specificSerializer.serialize(specificClass.cast(generic), out, version);
    }

    @Override
    public Generic deserialize(DataInputPlus in, int version) throws IOException
    {
        Generic result = specificSerializer.deserialize(in, version);
        if (result != null && !specificClass.isInstance(result))
            throw new IllegalStateException("Expected instance of " + specificClass.getName());
        return result;
    }

    @Override
    public long serializedSize(Generic generic, int version)
    {
        return specificSerializer.serializedSize(specificClass.cast(generic), version);
    }
}
