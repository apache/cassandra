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

package org.apache.cassandra.config.registry;

import javax.annotation.Nonnull;

import org.apache.cassandra.exceptions.ConfigurationException;

/**
 * Converter do type conversion from the specified Object  value to the given {@code T}. If the class is
 * a primitive type (e.g. Boolean.TYPE, Long.TYPE etc), the value returned will use the corresponding
 * wrapper type (Long.class, Boolean.class, etc).
 *
 * @see TypeConverter
 * @see org.apache.cassandra.config.StringConverters
 */
public class PrimitiveUnaryConverter
{
    private static Object to(Class<?> cls, @Nonnull Object value)
    {
        if (cls.isInstance(value))
            return value;
        else if (Boolean.class.equals(cls) || Boolean.TYPE.equals(cls))
        {
            if (value instanceof Boolean)
                return value;
            throw typeMismatchEx(cls, value);
        }
        else if (Integer.class.equals(cls) || Integer.TYPE.equals(cls))
        {
            if (value instanceof Integer)
                return value;
            throw typeMismatchEx(cls, value);
        }
        else if (Long.class.equals(cls) || Long.TYPE.equals(cls))
        {
            if (value instanceof Long)
                return value;
            throw typeMismatchEx(cls, value);
        }
        else if (Double.class.equals(cls) || Double.TYPE.equals(cls))
        {
            if (value instanceof Double)
                return value;
            throw typeMismatchEx(cls, value);
        }
        else if (Float.class.equals(cls) || Float.TYPE.equals(cls))
        {
            if (value instanceof Float)
                return value;
            throw typeMismatchEx(cls, value);
        }
        throw typeMismatchEx(cls, value);
    }

    private static ConfigurationException typeMismatchEx(final Class<?> cls, final Object value)
    {
        return new ConfigurationException("Cannot convert " + value.getClass() + " to " + cls);
    }

    @SuppressWarnings("unchecked")
    public static <T> T convertSafe(Class<?> cls, Object value)
    {
        if (value == null)
            return null;
        return (T) to(cls, value);
    }
}
