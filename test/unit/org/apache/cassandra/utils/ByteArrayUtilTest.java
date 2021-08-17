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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

import org.junit.Test;

import org.quicktheories.core.Gen;
import org.quicktheories.generators.SourceDSL;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.quicktheories.QuickTheory.qt;

public class ByteArrayUtilTest
{
    private static final ByteOrder ORDER = ByteOrder.BIG_ENDIAN;

    @Test
    public void putGetBoolean()
    {
        byte[] bytes = new byte[10];
        for (int i = 0; i < bytes.length; i++)
        {
            for (boolean b : Arrays.asList(Boolean.TRUE, Boolean.FALSE))
            {
                ByteArrayUtil.putBoolean(bytes, i, b);
                assertThat(ByteArrayUtil.getBoolean(bytes, i))
                          .as("get(put(b)) == b")
                          .isEqualTo(b);
            }
        }
    }

    @Test
    public void putBooleanArrayTooSmall()
    {
        putArrayToSmall(1, bytes -> ByteArrayUtil.putBoolean(bytes, 0, true));
    }

    @Test
    public void putBooleanArrayOutOfBounds()
    {
        byte[] bytes = new byte[16];
        assertThatThrownBy(() -> ByteArrayUtil.putBoolean(bytes, bytes.length + 10, true))
            .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    public void putGetShort()
    {
        Gen<Short> gen = SourceDSL.integers().between(Short.MIN_VALUE, Short.MAX_VALUE).map(Integer::shortValue);
        byte[] bytes = new byte[Short.BYTES + 1];
        ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ORDER);
        qt().forAll(gen).checkAssert(jnum -> {
            short value = jnum.shortValue();
            ByteArrayUtil.putShort(bytes, 1, value);
            assertThat(ByteArrayUtil.getShort(bytes, 1))
                      .as("get(put(b)) == b")
                      .isEqualTo(value)
                      .isEqualTo(buffer.getShort(1));
        });
    }

    @Test
    public void putShortArrayTooSmall()
    {
        putArrayToSmall(Short.BYTES, bytes -> ByteArrayUtil.putShort(bytes, 0, (short) 42));
    }

    @Test
    public void putShortArrayOutOfBounds()
    {
        byte[] bytes = new byte[16];
        assertThatThrownBy(() -> ByteArrayUtil.putInt(bytes, bytes.length + 10, (short) 42))
        .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    public void putGetInt()
    {
        Gen<Integer> gen = SourceDSL.integers().all();
        byte[] bytes = new byte[Integer.BYTES + 1];
        ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ORDER);
        qt().forAll(gen).checkAssert(jnum -> {
            int value = jnum.intValue();
            ByteArrayUtil.putInt(bytes, 1, value);
            assertThat(ByteArrayUtil.getInt(bytes, 1))
                      .as("get(put(b)) == b")
                      .isEqualTo(value)
                      .isEqualTo(buffer.getInt(1));
        });
    }

    @Test
    public void putIntArrayTooSmall()
    {
        putArrayToSmall(Integer.BYTES, bytes -> ByteArrayUtil.putInt(bytes, 0, 42));
    }

    @Test
    public void putIntArrayOutOfBounds()
    {
        byte[] bytes = new byte[16];
        assertThatThrownBy(() -> ByteArrayUtil.putInt(bytes, bytes.length + 10, 42))
            .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    public void putGetLong()
    {
        Gen<Long> gen = SourceDSL.longs().all();
        byte[] bytes = new byte[Long.BYTES + 1];
        ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ORDER);
        qt().forAll(gen).checkAssert(jnum -> {
            long value = jnum.longValue();
            ByteArrayUtil.putLong(bytes, 1, value);
            assertThat(ByteArrayUtil.getLong(bytes, 1))
                      .as("get(put(b)) == b")
                      .isEqualTo(value)
                      .isEqualTo(buffer.getLong(1));
        });
    }

    @Test
    public void putLongArrayTooSmall()
    {
        putArrayToSmall(Long.BYTES, bytes -> ByteArrayUtil.putLong(bytes, 0, 42L));
    }

    @Test
    public void putLongArrayOutOfBounds()
    {
        byte[] bytes = new byte[16];
        assertThatThrownBy(() -> ByteArrayUtil.putLong(bytes, bytes.length + 10, 42))
            .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    public void putGetFloat()
    {
        Gen<Float> gen = SourceDSL.floats().any();
        byte[] bytes = new byte[Float.BYTES + 1];
        ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ORDER);
        qt().forAll(gen).checkAssert(jnum -> {
            float value = jnum.floatValue();
            ByteArrayUtil.putFloat(bytes, 1, value);
            assertThat(ByteArrayUtil.getFloat(bytes, 1))
                      .as("get(put(b)) == b")
                      .isEqualTo(value)
                      .isEqualTo(buffer.getFloat(1));
        });
    }

    @Test
    public void putFloatArrayTooSmall()
    {
        putArrayToSmall(Float.BYTES, bytes -> ByteArrayUtil.putFloat(bytes, 0, 42f));
    }

    @Test
    public void putFloatArrayOutOfBounds()
    {
        byte[] bytes = new byte[16];
        assertThatThrownBy(() -> ByteArrayUtil.putFloat(bytes, bytes.length + 10, 42.0f))
            .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    public void putGetDouble()
    {
        Gen<Double> gen = SourceDSL.doubles().any();
        byte[] bytes = new byte[Double.BYTES + 1];
        ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ORDER);
        qt().forAll(gen).checkAssert(jnum -> {
            double value = jnum.doubleValue();
            ByteArrayUtil.putDouble(bytes, 1, value);
            assertThat(ByteArrayUtil.getDouble(bytes, 1))
                      .as("get(put(b)) == b")
                      .isEqualTo(value)
                      .isEqualTo(buffer.getDouble(1));
        });
    }

    @Test
    public void putDoubleArrayTooSmall()
    {
        putArrayToSmall(Double.BYTES, bytes -> ByteArrayUtil.putDouble(bytes, 0, 42.0));
    }

    @Test
    public void putDoubleArrayOutOfBounds()
    {
        byte[] bytes = new byte[16];
        assertThatThrownBy(() -> ByteArrayUtil.putDouble(bytes, bytes.length + 10, 42.0))
            .isInstanceOf(IndexOutOfBoundsException.class);
    }

    private static void putArrayToSmall(int targetBytes, FailingConsumer<byte[]> fn)
    {
        for (int i = 0; i < targetBytes - 1; i++)
        {
            byte[] bytes = new byte[i];
            assertThatThrownBy(() -> fn.doAccept(bytes)).isInstanceOf(IndexOutOfBoundsException.class);
            assertThat(bytes).isEqualTo(new byte[i]);
        }
    }
}
