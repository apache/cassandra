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

import com.datastax.driver.core.DataType;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.db.marshal.AbstractType;

/**
 * Helper class for User Defined Functions + Aggregates.
 */
public final class UDHelper
{
    // TODO make these c'tors and methods public in Java-Driver - see https://datastax-oss.atlassian.net/browse/JAVA-502
    static final MethodHandle methodParseOne;
    static
    {
        try
        {
            Class<?> cls = Class.forName("com.datastax.driver.core.CassandraTypeParser");
            Method m = cls.getDeclaredMethod("parseOne", String.class);
            m.setAccessible(true);
            methodParseOne = MethodHandles.lookup().unreflect(m);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Construct an array containing the Java classes for the given Java Driver {@link com.datastax.driver.core.DataType}s.
     *
     * @param dataTypes  array with UDF argument types
     * @param calledOnNullInput whether to allow {@code null} as an argument value
     * @return array of same size with UDF arguments
     */
    public static Class<?>[] javaTypes(DataType[] dataTypes, boolean calledOnNullInput)
    {
        Class<?>[] paramTypes = new Class[dataTypes.length];
        for (int i = 0; i < paramTypes.length; i++)
        {
            Class<?> clazz = dataTypes[i].asJavaClass();
            if (!calledOnNullInput)
            {
                // only care about classes that can be used in a data type
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
            }
            paramTypes[i] = clazz;
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
        try
        {
            return (DataType) methodParseOne.invoke(cqlType.getType().toString());
        }
        catch (RuntimeException | Error e)
        {
            // immediately rethrow these...
            throw e;
        }
        catch (Throwable e)
        {
            throw new RuntimeException("cannot parse driver type " + cqlType.getType().toString(), e);
        }
    }

    public static boolean isNullOrEmpty(AbstractType<?> type, ByteBuffer bb)
    {
        return bb == null ||
               (bb.remaining() == 0 && type.isEmptyValueMeaningless());
    }
}
