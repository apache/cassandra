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

import java.util.List;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.marshal.ValueAccessor;

public class TupleSerializer extends BytesSerializer
{
    public final List<TypeSerializer<?>> fields;

    public TupleSerializer(List<TypeSerializer<?>> fields)
    {
        this.fields = fields;
    }

    public <T> void validate(T input, ValueAccessor<T> handle) throws MarshalException
    {
        int offset = 0;
        for (int i = 0; i < fields.size(); i++)
        {
            // we allow the input to have less fields than declared so as to support field addition.
            if (handle.sizeFromOffset(input, offset) == 0)
                return;

            if (handle.sizeFromOffset(input, offset) < Integer.BYTES)
                throw new MarshalException(String.format("Not enough bytes to read size of %dth component", i));

            int size = handle.getInt(input, offset);
            offset += TypeSizes.sizeof(size);

            // size < 0 means null value
            if (size < 0)
                continue;

            if (handle.sizeFromOffset(input, offset) < size)
                throw new MarshalException(String.format("Not enough bytes to read %dth component", i));

            T field = handle.slice(input, offset, size);
            offset += size;
            fields.get(i).validate(field, handle);
        }

        // We're allowed to get less fields than declared, but not more
        if (handle.sizeFromOffset(input, offset) != 0)
            throw new MarshalException("Invalid remaining data after end of tuple value");
    }
}
