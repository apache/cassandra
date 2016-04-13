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

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.List;

import com.google.common.reflect.TypeToken;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.transport.Server;

/**
 * Helper class for User Defined Functions, Types and Aggregates.
 */
public final class UDHelper
{
    // TODO make these c'tors and methods public in Java-Driver - see https://datastax-oss.atlassian.net/browse/JAVA-502
    private static final MethodHandle methodParseOne;
    private static final CodecRegistry codecRegistry;
    static
    {
        try
        {
            Class<?> cls = Class.forName("com.datastax.driver.core.DataTypeClassNameParser");
            Method m = cls.getDeclaredMethod("parseOne", String.class, ProtocolVersion.class, CodecRegistry.class);
            m.setAccessible(true);
            methodParseOne = MethodHandles.lookup().unreflect(m);
            codecRegistry = new CodecRegistry();
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

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
     * Construct an array containing the Java classes for the given Java Driver {@link com.datastax.driver.core.DataType}s.
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
                    clazz = int.class;
                else if (clazz == Long.class)
                    clazz = long.class;
                else if (clazz == Byte.class)
                    clazz = byte.class;
                else if (clazz == Short.class)
                    clazz = short.class;
                else if (clazz == Float.class)
                    clazz = float.class;
                else if (clazz == Double.class)
                    clazz = double.class;
                else if (clazz == Boolean.class)
                    clazz = boolean.class;
                typeToken = TypeToken.of(clazz);
            }
            paramTypes[i] = typeToken;
        }
        return paramTypes;
    }

    /**
     * Construct an array containing the Java Driver {@link com.datastax.driver.core.DataType}s for the
     * C* internal types.
     *
     * @param abstractTypes list with UDF argument types
     * @return array with argument types as {@link com.datastax.driver.core.DataType}
     */
    public static DataType[] driverTypes(List<AbstractType<?>> abstractTypes)
    {
        DataType[] argDataTypes = new DataType[abstractTypes.size()];
        for (int i = 0; i < argDataTypes.length; i++)
            argDataTypes[i] = driverType(abstractTypes.get(i));
        return argDataTypes;
    }

    /**
     * Returns the Java Driver {@link com.datastax.driver.core.DataType} for the C* internal type.
     */
    public static DataType driverType(AbstractType abstractType)
    {
        CQL3Type cqlType = abstractType.asCQL3Type();
        String abstractTypeDef = cqlType.getType().toString();
        return driverTypeFromAbstractType(abstractTypeDef);
    }

    public static DataType driverTypeFromAbstractType(String abstractTypeDef)
    {
        try
        {
            return (DataType) methodParseOne.invoke(abstractTypeDef,
                                                    ProtocolVersion.fromInt(Server.CURRENT_VERSION),
                                                    codecRegistry);
        }
        catch (RuntimeException | Error e)
        {
            // immediately rethrow these...
            throw e;
        }
        catch (Throwable e)
        {
            throw new RuntimeException("cannot parse driver type " + abstractTypeDef, e);
        }
    }

    public static Object deserialize(TypeCodec<?> codec, int protocolVersion, ByteBuffer value)
    {
        return codec.deserialize(value, ProtocolVersion.fromInt(protocolVersion));
    }

    public static ByteBuffer serialize(TypeCodec<?> codec, int protocolVersion, Object value)
    {
        if (!codec.getJavaType().getRawType().isAssignableFrom(value.getClass()))
            throw new InvalidTypeException("Invalid value for CQL type " + codec.getCqlType().getName().toString());

        return ((TypeCodec)codec).serialize(value, ProtocolVersion.fromInt(protocolVersion));
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
