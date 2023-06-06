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

import java.lang.reflect.Field;
import java.lang.reflect.Method;

public class ReflectionUtils
{
    private ReflectionUtils()
    {

    }

    public static Field getModifiersField() throws NoSuchFieldException
    {
        return getField(Field.class, "modifiers");
    }

    public static Field getField(Class<?> clazz, String fieldName) throws NoSuchFieldException
    {
        // below code works before Java 12
        try
        {
            return clazz.getDeclaredField(fieldName);
        }
        catch (NoSuchFieldException e)
        {
            // this is mitigation for JDK 17 (https://bugs.openjdk.org/browse/JDK-8210522)
            try
            {
                Method getDeclaredFields0 = Class.class.getDeclaredMethod("getDeclaredFields0", boolean.class);
                getDeclaredFields0.setAccessible(true);
                Field[] fields = (Field[]) getDeclaredFields0.invoke(clazz, false);
                for (Field field : fields)
                {
                    if (fieldName.equals(field.getName()))
                    {
                        return field;
                    }
                }
            }
            catch (ReflectiveOperationException ex)
            {
                e.addSuppressed(ex);
            }
            throw e;
        }
    }
}