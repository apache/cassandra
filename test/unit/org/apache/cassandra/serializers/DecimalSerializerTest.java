/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.serializers;

import java.math.BigDecimal;
import java.nio.ByteBuffer;

import org.junit.Test;

import org.apache.cassandra.db.marshal.ByteArrayAccessor;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

public class DecimalSerializerTest
{
    @Test
    public void testDecimalScaleStringify()
    {
        BigDecimal d = new BigDecimal("1");
        assertEquals("1", DecimalSerializer.instance.toString(d));

        d = new BigDecimal("-1");
        assertEquals("-1", DecimalSerializer.instance.toString(d));

        int scale = Integer.MAX_VALUE - 100;
        d = new BigDecimal("1e" + scale);
        assertEquals("1E+" + scale, DecimalSerializer.instance.toString(d));

        d = new BigDecimal("-1e" + scale);
        assertEquals("-1E+" + scale, DecimalSerializer.instance.toString(d));

        d = new BigDecimal("1e-" + scale);
        assertEquals("1E-" + scale, DecimalSerializer.instance.toString(d));

        d = new BigDecimal("-1e-" + scale);
        assertEquals("-1E-" + scale, DecimalSerializer.instance.toString(d));
    }

    @Test
    public void testValidateAcceptsEmptyAndSerializedValues()
    {
        DecimalSerializer.instance.validate(ByteBufferUtil.EMPTY_BYTE_BUFFER);

        // zero is the shortest serialized value: a 4-byte scale and a 1-byte unscaled value
        ByteBuffer zero = DecimalSerializer.instance.serialize(BigDecimal.ZERO);
        assertEquals(5, zero.remaining());
        DecimalSerializer.instance.validate(zero);
        assertEquals(BigDecimal.ZERO, DecimalSerializer.instance.deserialize(zero));

        for (String value : new String[]{ "1", "-1", "12.34", "-0.001" })
        {
            ByteBuffer bytes = DecimalSerializer.instance.serialize(new BigDecimal(value));
            DecimalSerializer.instance.validate(bytes);
            assertEquals(new BigDecimal(value), DecimalSerializer.instance.deserialize(bytes));
        }
    }

    @Test
    public void testValidateRejectsMissingUnscaledValue()
    {
        // 4 bytes hold only the scale, so deserialize would build a BigInteger from no bytes and fail.
        // 1 to 3 bytes cannot even hold the scale.
        for (int size = 4; size >= 1; size--)
        {
            ByteBuffer buffer = ByteBuffer.allocate(size);
            assertThatThrownBy(() -> DecimalSerializer.instance.validate(buffer))
            .isInstanceOf(MarshalException.class).hasMessageContaining("Expected 0 or at least 5 bytes");
            assertThatThrownBy(() -> DecimalSerializer.instance.validate(new byte[buffer.capacity()], ByteArrayAccessor.instance))
            .isInstanceOf(MarshalException.class).hasMessageContaining("Expected 0 or at least 5 bytes");
        }
    }
}
