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
import java.util.List;

import com.google.common.reflect.TypeToken;

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.functions.types.*;
import org.apache.cassandra.cql3.functions.types.exceptions.InvalidTypeException;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.transport.ProtocolVersion;

/**
 * Helper class for User Defined Functions, Types and Aggregates.
 */
public final class UDHelper
{
    private static final CodecRegistry codecRegistry = new CodecRegistry();

    static TypeCodec<Object>[] codecsFor(DataType[] dataType)
    {
        TypeCodec<Object>[] codecs = new TypeCodec[dataType.length];
        for (int i = 0; i < dataType.length; i++)
            codecs[i] = codecFor(dataType[i]);
        return codecs;
    }

    public static TypeCodec<Object> codecFor(DataType dataType)
    {
        return codecRegistry.codecFor(dataType);
    }

    /**
     * Construct an array containing the Java classes for the given {@link DataType}s.
     *
     * @param dataTypes  array with UDF argument types
     * @param calledOnNullInput whether to allow {@code null} as an argument value
     * @return array of same size with UDF arguments
     */
    public static TypeToken<?>[] typeTokens(TypeCodec<Object>[] dataTypes, boolean calledOnNullInput)
    {
        TypeToken<?>[] paramTypes = new TypeToken[dataTypes.length];
        for (int i = 0; i < paramTypes.length; i++)
        {
            TypeToken<?> typeToken = dataTypes[i].getJavaType();
            if (!calledOnNullInput)
            {
                // only care about classes that can be used in a data type
                Class<?> clazz = typeToken.getRawType();
                if (clazz == Integer.class)
                    typeToken = TypeToken.of(int.class);
                else if (clazz == Long.class)
                    typeToken = TypeToken.of(long.class);
                else if (clazz == Byte.class)
                    typeToken = TypeToken.of(byte.class);
                else if (clazz == Short.class)
                    typeToken = TypeToken.of(short.class);
                else if (clazz == Float.class)
                    typeToken = TypeToken.of(float.class);
                else if (clazz == Double.class)
                    typeToken = TypeToken.of(double.class);
                else if (clazz == Boolean.class)
                    typeToken = TypeToken.of(boolean.class);
            }
            paramTypes[i] = typeToken;
        }
        return paramTypes;
    }

    /**
     * Construct an array containing the {@link DataType}s for the
     * C* internal types.
     *
     * @param abstractTypes list with UDF argument types
     * @return array with argument types as {@link DataType}
     */
    public static DataType[] driverTypes(List<AbstractType<?>> abstractTypes)
    {
        DataType[] argDataTypes = new DataType[abstractTypes.size()];
        for (int i = 0; i < argDataTypes.length; i++)
            argDataTypes[i] = driverType(abstractTypes.get(i));
        return argDataTypes;
    }

    /**
     * Returns the {@link DataType} for the C* internal type.
     */
    public static DataType driverType(AbstractType abstractType)
    {
        CQL3Type cqlType = abstractType.asCQL3Type();
        String abstractTypeDef = cqlType.getType().toString();
        return driverTypeFromAbstractType(abstractTypeDef);
    }

    public static DataType driverTypeFromAbstractType(String abstractTypeDef)
    {
        return DataTypeClassNameParser.parseOne(abstractTypeDef,
                                                ProtocolVersion.CURRENT,
                                                codecRegistry);
    }

    public static Object deserialize(TypeCodec<?> codec, ProtocolVersion protocolVersion, ByteBuffer value)
    {
        return codec.deserialize(value, protocolVersion);
    }

    public static ByteBuffer serialize(TypeCodec<?> codec, ProtocolVersion protocolVersion, Object value)
    {
        if (!codec.getJavaType().getRawType().isAssignableFrom(value.getClass()))
            throw new InvalidTypeException("Invalid value for CQL type " + codec.getCqlType().getName());

        return ((TypeCodec)codec).serialize(value, protocolVersion);
    }

    public static Class<?> asJavaClass(TypeCodec<?> codec)
    {
        return codec.getJavaType().getRawType();
    }

    public static boolean isNullOrEmpty(AbstractType<?> type, ByteBuffer bb)
    {
        return bb == null ||
               (bb.remaining() == 0 && type.isEmptyValueMeaningless());
    }
}
