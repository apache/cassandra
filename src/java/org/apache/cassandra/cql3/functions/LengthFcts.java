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

package org.apache.cassandra.cql3.functions;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Lengths of stored data without returning the data.
 */
public class LengthFcts
{
    public static void addFunctionsTo(NativeFunctions functions)
    {
        // As all types ultimately end up as bytebuffers they should all be compatible with
        // octet_length
        Set<AbstractType<?>> types = new HashSet<>();
        for (CQL3Type type : CQL3Type.Native.values())
        {
            AbstractType<?> udfType = type.getType().udfType();
            if (!types.add(udfType))
                continue;
            functions.add(makeOctetLengthFunction(type.getType().udfType()));
        }

        // Special handling for string length which is number of UTF-8 code units
        functions.add(length);
    }


    public static NativeFunction makeOctetLengthFunction(AbstractType<?> fromType)
    {
        // Matches SQL99 OCTET_LENGTH functions defined on bytestring and char strings
        return new NativeScalarFunction("octet_length", Int32Type.instance, fromType)
        {
            // Do not deserialize
            @Override
            public Arguments newArguments(ProtocolVersion version)
            {
                return FunctionArguments.newNoopInstance(version, 1);
            }

            @Override
            public ByteBuffer execute(Arguments arguments) throws InvalidRequestException
            {
                if (arguments.get(0) == null)
                    return null;

                final ByteBuffer buffer = arguments.get(0);
                return ByteBufferUtil.bytes(buffer.remaining());
            }
        };
    }

    // Matches PostgreSQL length function defined as returning the number of UTF-8 code units in the text string
    public static final NativeFunction length = new NativeScalarFunction("length", Int32Type.instance, UTF8Type.instance)
    {
        @Override
        public ByteBuffer execute(Arguments arguments) throws InvalidRequestException
        {
            if (arguments.get(0) == null)
                return null;

            final String value = arguments.get(0);
            return ByteBufferUtil.bytes(value.length());
        }
    };
}
