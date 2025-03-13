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

import org.apache.cassandra.io.UnversionedSerializer;
import org.apache.cassandra.io.VersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

/**
 * Utility for serializing/deserializing from/into generic interface fields where we know (and require) the
 * generic fields to be implementation specific classes
 */
public class CastingSerializer
{
    public static <Generic, Specific extends Generic, Version> VersionedSerializer<Generic, Version> create(Class<Specific> specificClass, VersionedSerializer<Specific, Version> specificSerializer)
    {
        return new Versioned<>(specificClass, specificSerializer);
    }

    public static <Generic, Specific extends Generic> UnversionedSerializer<Generic> create(Class<Specific> specificClass, UnversionedSerializer<Specific> specificSerializer)
    {
        return new Unversioned<>(specificClass, specificSerializer);
    }

    private static final class Versioned<Generic, Specific extends Generic, Version> implements VersionedSerializer<Generic, Version>
    {
        private final Class<Specific> specificClass;
        private final VersionedSerializer<Specific, Version> specificSerializer;

        private Versioned(Class<Specific> specificClass, VersionedSerializer<Specific, Version> specificSerializer)
        {
            this.specificClass = specificClass;
            this.specificSerializer = specificSerializer;
        }

        @Override
        public void serialize(Generic generic, DataOutputPlus out, Version version) throws IOException
        {
            specificSerializer.serialize(specificClass.cast(generic), out, version);
        }

        @Override
        public Generic deserialize(DataInputPlus in, Version version) throws IOException
        {
            Generic result = specificSerializer.deserialize(in, version);
            if (result != null && !specificClass.isInstance(result))
                throw new IllegalStateException("Expected instance of " + specificClass.getName());
            return result;
        }

        @Override
        public long serializedSize(Generic generic, Version version)
        {
            return specificSerializer.serializedSize(specificClass.cast(generic), version);
        }
    }

    private static final class Unversioned<Generic, Specific extends Generic> implements UnversionedSerializer<Generic>
    {
        private final Class<Specific> specificClass;
        private final UnversionedSerializer<Specific> specificSerializer;

        private Unversioned(Class<Specific> specificClass, UnversionedSerializer<Specific> specificSerializer)
        {
            this.specificClass = specificClass;
            this.specificSerializer = specificSerializer;
        }

        @Override
        public void serialize(Generic generic, DataOutputPlus out) throws IOException
        {
            specificSerializer.serialize(specificClass.cast(generic), out);
        }

        @Override
        public Generic deserialize(DataInputPlus in) throws IOException
        {
            Generic result = specificSerializer.deserialize(in);
            if (result != null && !specificClass.isInstance(result))
                throw new IllegalStateException("Expected instance of " + specificClass.getName());
            return result;
        }

        @Override
        public long serializedSize(Generic generic)
        {
            return specificSerializer.serializedSize(specificClass.cast(generic));
        }
    }
}
