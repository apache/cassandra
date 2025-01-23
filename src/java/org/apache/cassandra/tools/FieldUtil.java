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

package org.apache.cassandra.tools;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

public class FieldUtil
{
    public static void setInstanceUnsafe(Class<?> klass, Object v, String fieldName)
    {
        try
        {
            setInstanceUnsafeThrowing(klass, v, fieldName);
        }
        catch (Throwable e)
        {
            throw new RuntimeException(e);
        }
    }

    private static void setInstanceUnsafeThrowing(Class<?> klass, Object v, String fieldName) throws Throwable
    {
        Field field = klass.getDeclaredField(fieldName);
        field.setAccessible(true);

        try
        {
            Field modifiers = Field.class.getDeclaredField("modifiers");
            modifiers.setAccessible(true);
            modifiers.setInt(field, field.getModifiers() & ~Modifier.FINAL);
        }
        catch (NoSuchFieldException t)
        {
            // jdk17 fallback
            Method getDeclaredFields0 = Class.class.getDeclaredMethod("getDeclaredFields0", boolean.class);
            getDeclaredFields0.setAccessible(true);
            Field[] fields = (Field[]) getDeclaredFields0.invoke(Field.class, false);

            for (Field f : fields)
            {
                if ("modifiers".equals(f.getName()))
                {
                    f.setAccessible(true);
                    f.setInt(field, field.getModifiers() & ~Modifier.FINAL);
                    break;
                }
            }
        }

        field.set(null, v);
    }

    public static void transferFields(Object sourceInstance, Class<?> klass)
    {
        for (Field sourceField : sourceInstance.getClass().getDeclaredFields())
        {
            sourceField.setAccessible(true);
            try
            {
                setInstanceUnsafe(klass, sourceField.get(sourceInstance), sourceField.getName());
            }
            catch (Throwable e)
            {
                throw new RuntimeException("Failed to transfer field: " + sourceField.getName(), e);
            }
        }
    }
}