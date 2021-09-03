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

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ObjectSizesTest
{
    @Test
    public void heapByteBuffer()
    {
        byte[] bytes = {0, 1, 2, 3, 4};
        ByteBuffer buffer = ByteBuffer.wrap(bytes);

        long empty = ObjectSizes.sizeOfEmptyHeapByteBuffer();
        long actual = ObjectSizes.measureDeep(buffer);

        assertThat(actual).isEqualTo(empty + ObjectSizes.sizeOfArray(bytes));
        assertThat(ObjectSizes.sizeOnHeapOf(buffer)).isEqualTo(actual);
    }

    @Test
    public void shouldIgnoreArrayOverheadForSubBuffer()
    {
        byte[] bytes = {0, 1, 2, 3, 4};
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.position(1);
        assertThat(ObjectSizes.sizeOnHeapOf(buffer)).isEqualTo(ObjectSizes.sizeOfEmptyHeapByteBuffer() + 4);
    }
}