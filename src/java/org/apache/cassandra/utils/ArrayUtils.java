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

import java.util.Arrays;

public class ArrayUtils
{
    public static int hashCode(Object object)
    {
        if (object.getClass().isArray())
        {
            Class<?> klass = object.getClass();
            if (klass == Object[].class) return Arrays.hashCode((Object[]) object);
            else if (klass == byte[].class) return Arrays.hashCode((byte[]) object);
            else if (klass == char[].class) return Arrays.hashCode((char[]) object);
            else if (klass == short[].class) return Arrays.hashCode((short[]) object);
            else if (klass == int[].class) return Arrays.hashCode((int[]) object);
            else if (klass == long[].class) return Arrays.hashCode((long[]) object);
            else if (klass == double[].class) return Arrays.hashCode((double[]) object);
            else if (klass == float[].class) return Arrays.hashCode((float[]) object);
            else if (klass == boolean[].class) return Arrays.hashCode((boolean[]) object);
            else  throw new IllegalArgumentException("Unknown type: " + klass);
        }
        else
        {
            return object.hashCode();
        }
    }

    public static String toString(Object object)
    {
        if (object.getClass().isArray())
        {
            Class<?> klass = object.getClass();
            if (klass == Object[].class) return Arrays.toString((Object[]) object);
            else if (klass == byte[].class) return Arrays.toString((byte[]) object);
            else if (klass == char[].class) return Arrays.toString((char[]) object);
            else if (klass == short[].class) return Arrays.toString((short[]) object);
            else if (klass == int[].class) return Arrays.toString((int[]) object);
            else if (klass == long[].class) return Arrays.toString((long[]) object);
            else if (klass == double[].class) return Arrays.toString((double[]) object);
            else if (klass == float[].class) return Arrays.toString((float[]) object);
            else if (klass == boolean[].class) return Arrays.toString((boolean[]) object);
            else  throw new IllegalArgumentException("Unknown type: " + klass);
        }
        else
        {
            return object.toString();
        }
    }
}
