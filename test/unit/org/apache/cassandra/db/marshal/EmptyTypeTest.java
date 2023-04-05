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

import org.junit.Test;

import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class EmptyTypeTest
{
    @Test
    public void isFixed()
    {
        assertThat(EmptyType.instance.valueLengthIfFixed()).isEqualTo(0);
    }

    @Test
    public void writeEmptyAllowed()
    {
        DataOutputPlus output = Mockito.mock(DataOutputPlus.class);
        EmptyType.instance.writeValue(ByteBufferUtil.EMPTY_BYTE_BUFFER, output);

        Mockito.verifyNoInteractions(output);
    }

    @Test
    public void writeNonEmpty()
    {
        DataOutputPlus output = Mockito.mock(DataOutputPlus.class);
        ByteBuffer rejected = ByteBuffer.wrap("this better fail".getBytes());

        assertThatThrownBy(() -> EmptyType.instance.writeValue(rejected, output))
                  .isInstanceOf(AssertionError.class);
        Mockito.verifyNoInteractions(output);
    }

    @Test
    public void read()
    {
        DataInputPlus input = Mockito.mock(DataInputPlus.class);

        ByteBuffer buffer = EmptyType.instance.readBuffer(input);
        assertThat(buffer)
                  .isNotNull()
                  .matches(b -> !b.hasRemaining());

        buffer = EmptyType.instance.readBuffer(input, 42);
        assertThat(buffer)
                  .isNotNull()
                  .matches(b -> !b.hasRemaining());

        Mockito.verifyNoInteractions(input);
    }

    @Test
    public void decompose()
    {
        ByteBuffer buffer = EmptyType.instance.decompose(null);
        assertThat(buffer.remaining()).isEqualTo(0);
    }

    @Test
    public void composeEmptyInput()
    {
        Void result = EmptyType.instance.compose(ByteBufferUtil.EMPTY_BYTE_BUFFER);
        assertThat(result).isNull();
    }

    @Test
    public void composeNonEmptyInput()
    {
        assertThatThrownBy(() -> EmptyType.instance.compose(ByteBufferUtil.bytes("should fail")))
                  .isInstanceOf(MarshalException.class)
                  .hasMessage("EmptyType only accept empty values");
    }
}
