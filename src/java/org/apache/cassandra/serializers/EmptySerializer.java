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

import org.apache.cassandra.db.marshal.ValueAccessor;

public class EmptySerializer extends TypeSerializer<Void>
{
    public static final EmptySerializer instance = new EmptySerializer();

    public <V> Void deserialize(V value, ValueAccessor<V> handle)
    {
        validate(value, handle);
        return null;
    }

    public <V> V serialize(Void value, ValueAccessor<V> handle)
    {
        return handle.empty();
    }

    public <T> void validate(T value, ValueAccessor<T> handle) throws MarshalException
    {
        if (handle.size(value) > 0)
            throw new MarshalException("EmptyType only accept empty values");
    }

    public String toString(Void value)
    {
        return "";
    }

    public Class<Void> getType()
    {
        return Void.class;
    }
}
