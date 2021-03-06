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

package org.apache.cassandra.db.marshal;

import java.nio.ByteBuffer;

public class ValueAccessors
{
    public static final ValueAccessor[] ACCESSORS = new ValueAccessor[]{ ByteBufferAccessor.instance, ByteArrayAccessor.instance };

    public static <V1, V2> void assertDataEquals(V1 expected, ValueAccessor<V1> expectedAccessor, V2 actual, ValueAccessor<V2> actualAccessor)
    {
        if (expectedAccessor.compare(expected, actual, actualAccessor) != 0)
        {
            throw new AssertionError(String.format("%s != %s", expectedAccessor.toHex(expected), actualAccessor.toHex(actual)));
        }
    }

    public static <V1, V2> void assertDataNotEquals(V1 expected, ValueAccessor<V1> expectedAccessor, V2 actual, ValueAccessor<V2> actualAccessor)
    {
        if (expectedAccessor.compare(expected, actual, actualAccessor) == 0)
        {
            throw new AssertionError(String.format("unexpected data equality: %s", expectedAccessor.toHex(expected)));
        }
    }

    private static ValueAccessor getAccessor(Object o)
    {
        if (o instanceof ByteBuffer)
            return ByteBufferAccessor.instance;
        if (o instanceof byte[])
            return ByteArrayAccessor.instance;

        throw new AssertionError("Unhandled type " + o.getClass().getName());
    }

    public static void assertDataEquals(Object expected, Object actual)
    {
        assertDataEquals(expected, getAccessor(expected), actual, getAccessor(actual));
    }

    public static void assertDataNotEquals(Object expected, Object actual)
    {
        assertDataNotEquals(expected, getAccessor(expected), actual, getAccessor(actual));
    }
}
