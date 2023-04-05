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
package org.apache.cassandra.schema;

import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.cql3.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UserType;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

public final class CQLTypeParser
{
    private static final ImmutableSet<String> PRIMITIVE_TYPES;

    static
    {
        ImmutableSet.Builder<String> builder = ImmutableSet.builder();
        for (CQL3Type.Native primitive : CQL3Type.Native.values())
            builder.add(primitive.name().toLowerCase());
        PRIMITIVE_TYPES = builder.build();
    }

    public static AbstractType<?> parse(String keyspace, String unparsed, Types userTypes)
    {
        String lowercased = unparsed.toLowerCase();

        // fast path for the common case of a primitive type
        if (PRIMITIVE_TYPES.contains(lowercased))
            return CQL3Type.Native.valueOf(unparsed.toUpperCase()).getType();

        // special-case top-level UDTs
        UserType udt = userTypes.getNullable(bytes(lowercased));
        if (udt != null)
            return udt;

        return parseRaw(unparsed).prepareInternal(keyspace, userTypes).getType();
    }

    static CQL3Type.Raw parseRaw(String type)
    {
        return CQLFragmentParser.parseAny(CqlParser::comparatorType, type, "CQL type");
    }
}
