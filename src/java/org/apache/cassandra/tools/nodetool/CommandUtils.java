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

package org.apache.cassandra.tools.nodetool;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;

import org.apache.cassandra.tools.nodetool.layout.CassandraUsage;
import org.apache.cassandra.utils.Pair;

/**
 * Utility methods for nodetool commands.
 */
public final class CommandUtils
{
    /**
     * Returns a string with the given number of leading spaces.
     *
     * @param num the number of leading spaces
     * @return the string with the given number of leading spaces
     */
    public static String leadingSpaces(int num)
    {
        char[] buff = new char[num];
        Arrays.fill(buff, ' ');
        return new String(buff);
    }

    public static int maxLength(Collection<?> any)
    {
        int result = 0;
        for (Object value : any)
            result = Math.max(result, String.valueOf(value).length());
        return result;
    }

    public static Pair<String, String> findBackwardCompatibleArgument(Object userObject)
    {
        Class<?> clazz = userObject.getClass();
        for (Field field : clazz.getFields())
        {
            if (field.isAnnotationPresent(CassandraUsage.class))
            {
                CassandraUsage ann = field.getAnnotation(CassandraUsage.class);
                return Pair.create(ann.usage(), ann.description());
            }
        }
        return null;
    }

    public static String[] sortShortestFirst(String[] names)
    {
        Arrays.sort(names, Comparator.comparing(String::length));
        return names;
    }
}
