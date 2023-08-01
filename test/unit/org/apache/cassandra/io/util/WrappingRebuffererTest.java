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

package org.apache.cassandra.io.util;

import java.nio.ByteBuffer;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class WrappingRebuffererTest
{
    @Test
    public void testRebufferRelease()
    {
        TestRebufferer mock = new TestRebufferer();
        try (WrappingRebufferer rebufferer = new WrappingRebufferer(mock)
        {
        })
        {
            Rebufferer.BufferHolder ret = rebufferer.rebuffer(0);
            assertNotNull(ret);
            assertEquals(mock.buffer(), ret.buffer());
            assertEquals(mock.offset(), ret.offset());

            ret.release();
            assertTrue(mock.released);
        }
    }

    @Test
    public void testRebufferReleaseFailingContract()
    {
        TestRebufferer mock = new TestRebufferer();
        try (WrappingRebufferer rebufferer = new WrappingRebufferer(mock)
        {
        })
        {
            Rebufferer.BufferHolder ret1 = rebufferer.rebuffer(0);
            assertNotNull(ret1);
            assertEquals(mock.buffer(), ret1.buffer());
            assertEquals(mock.offset(), ret1.offset());

            assertThatExceptionOfType(AssertionError.class).isThrownBy(() -> rebufferer.rebuffer(1));
            ret1.release();

            assertTrue(mock.released);
            assertThatExceptionOfType(AssertionError.class).isThrownBy(ret1::buffer);
            assertThatExceptionOfType(AssertionError.class).isThrownBy(ret1::offset);
        }
    }

    @Test
    public void testRebufferReleaseFailingContractWhenClosing()
    {
        assertThatExceptionOfType(AssertionError.class).isThrownBy(() -> {
            TestRebufferer mock = new TestRebufferer();
            try (WrappingRebufferer rebufferer = new WrappingRebufferer(mock)
            {
            })
            {
                Rebufferer.BufferHolder ret1 = rebufferer.rebuffer(0);
                assertNotNull(ret1);
                assertEquals(mock.buffer(), ret1.buffer());
                assertEquals(mock.offset(), ret1.offset());
            }
        });
    }


    private static class TestRebufferer implements Rebufferer, Rebufferer.BufferHolder
    {
        final ByteBuffer buffer;
        boolean released;
        long offset;

        TestRebufferer()
        {
            this.buffer = ByteBuffer.allocate(0);
            this.released = false;
            this.offset = 0;
        }

        @Override
        public ChannelProxy channel()
        {
            return null;
        }

        @Override
        public ByteBuffer buffer()
        {
            return buffer;
        }

        public long fileLength()
        {
            return buffer.remaining();
        }

        public double getCrcCheckChance()
        {
            return 0;
        }

        public BufferHolder rebuffer(long position)
        {
            offset = position;
            return this;
        }

        public long offset()
        {
            return offset;
        }

        public void release()
        {
            released = true;
        }

        public long adjustExternal(long position)
        {
            return position;
        }

        public long adjustInternal(long position)
        {
            return position;
        }

        public void close()
        {
            // nothing
        }

        public void closeReader()
        {
            // nothing
        }
    }
}