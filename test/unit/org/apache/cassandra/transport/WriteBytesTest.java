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

package org.apache.cassandra.transport;

import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.Generators;
import org.assertj.core.api.Assertions;

import static org.quicktheories.QuickTheory.qt;


public class WriteBytesTest
{
    @Test
    public void test()
    {
        int maxBytes = 10_000;
        ByteBuf buf = Unpooled.buffer(maxBytes);
        qt().forAll(Generators.bytesAnyType(0, maxBytes)).checkAssert(bb -> {
            buf.clear();

            int size = bb.remaining();
            int pos = bb.position();

            CBUtil.addBytes(bb, buf);

            // test for consumption
            Assertions.assertThat(bb.remaining()).isEqualTo(size);
            Assertions.assertThat(bb.position()).isEqualTo(pos);

            Assertions.assertThat(buf.writerIndex()).isEqualTo(size);
            for (int i = 0; i < size; i++)
                Assertions.assertThat(buf.getByte(buf.readerIndex() + i)).describedAs("byte mismatch at index %d", i).isEqualTo(bb.get(bb.position() + i));
            FileUtils.clean(bb);
        });
    }

}
