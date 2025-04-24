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
import java.lang.reflect.Modifier;

import org.apache.cassandra.utils.ReflectionUtils;

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
        Field field = ReflectionUtils.getField(klass, fieldName);
        field.setAccessible(true);

        Field modifiers = ReflectionUtils.getModifiersField();
        modifiers.setAccessible(true);
        modifiers.setInt(field, field.getModifiers() & ~Modifier.FINAL);

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