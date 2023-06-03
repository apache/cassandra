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

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;

import org.apache.cassandra.cql3.functions.types.CodecRegistry;
import org.apache.cassandra.cql3.functions.types.DataType;
import org.apache.cassandra.cql3.functions.types.TypeCodec;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.transport.ProtocolVersion;

/**
 * Utility methods to convert from {@link AbstractType} to {@link DataType} or to {@link TypeCodec}.
 */
public final class JavaDriverUtils
{
    private static final CodecRegistry codecRegistry = new CodecRegistry();

    private static final MethodHandle methodParseOne;
    static
    {
        try
        {
            Class<?> cls = Class.forName("org.apache.cassandra.cql3.functions.types.DataTypeClassNameParser");
            Method m = cls.getDeclaredMethod("parseOne", String.class, ProtocolVersion.class, CodecRegistry.class);
            m.setAccessible(true);
            methodParseOne = MethodHandles.lookup().unreflect(m);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public static TypeCodec<Object> codecFor(AbstractType<?> abstractType)
    {
        return codecFor(driverType(abstractType));
    }

    public static TypeCodec<Object> codecFor(DataType dataType)
    {
        return codecRegistry.codecFor(dataType);
    }

    /**
     * Returns the Java Driver {@link DataType} for the C* internal type.
     */
    public static DataType driverType(AbstractType<?> abstractType)
    {
        try
        {
            return (DataType) methodParseOne.invoke(abstractType.toString(), ProtocolVersion.CURRENT, codecRegistry);
        }
        catch (RuntimeException | Error e)
        {
            // immediately rethrow these...
            throw e;
        }
        catch (Throwable e)
        {
            throw new RuntimeException("cannot parse driver type " + abstractType, e);
        }
    }

    /**
     * The class should never be instantiated as it contains only static methods.
     */
    private JavaDriverUtils()
    {
    }
}
