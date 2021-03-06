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

import org.quicktheories.core.Gen;
import org.quicktheories.generators.Generate;

import static org.apache.cassandra.db.marshal.ValueAccessors.ACCESSORS;
import static org.quicktheories.generators.SourceDSL.arbitrary;
import static org.quicktheories.generators.SourceDSL.integers;

/**
 * Base class for testing {@code ValueAccessor} classes.
 */
public class ValueAccessorTester
{
    /**
     * This method should be used to test values to be processed by {@link ValueAccessor}
     * in order to make sure we do not assume position == 0 in the underlying {@link ByteBuffer}
     */
    public static <V> V leftPad(V value, int padding)
    {
        if (!(value instanceof ByteBuffer))
            return value;

        ByteBuffer original = (ByteBuffer) value;
        ByteBuffer buf = ByteBuffer.allocate(original.remaining() + padding);
        buf.position(padding);
        buf.put(original);
        buf.position(padding);
        return (V) buf;
    }

    public static Gen<Integer> bbPadding()
    {
        return integers().between(0, 2);
    }

    public static Gen<ValueAccessor> accessors()
    {
        return arbitrary().pick(ACCESSORS)
                          .describedAs(a -> a.getClass().getSimpleName());
    }

    public static Gen<byte[]> byteArrays()
    {
        return byteArrays(Generate.constant(2));
    }

    public static Gen<byte[]> byteArrays(Gen<Integer> sizes)
    {
        return Generate.byteArrays(sizes, Generate.bytes(Byte.MIN_VALUE, Byte.MAX_VALUE, (byte) 0));
    }
}
