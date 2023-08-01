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

import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TailOverridingRebuffererTest
{
    ByteBuffer head = ByteBuffer.wrap(new byte[]{ 1, 2, 3, 4, 5, 6, 7, 8 });
    ByteBuffer tail = ByteBuffer.wrap(new byte[]{ 9, 10 });

    Rebufferer r = Mockito.mock(Rebufferer.class);
    Rebufferer.BufferHolder bh = Mockito.mock(Rebufferer.BufferHolder.class);

    public void before()
    {
        reset(r, bh);
    }

    @Test
    public void testAccessLeftToTailFully()
    {
        when(r.rebuffer(anyLong())).thenReturn(bh);
        when(bh.buffer()).thenReturn(head.duplicate());
        when(bh.offset()).thenReturn(0L);
        Rebufferer tor = new TailOverridingRebufferer(r, 8, tail.duplicate());

        for (int i = 0; i < 8; i++)
        {
            Rebufferer.BufferHolder bh = tor.rebuffer(i);
            assertEquals(head, bh.buffer());
            bh.release();
        }

        assertEquals(10, tor.fileLength());
    }

    @Test
    public void testAccessLeftToTailPartial()
    {
        when(r.rebuffer(anyLong())).thenReturn(bh);
        when(bh.buffer()).thenReturn(head.duplicate());
        when(bh.offset()).thenReturn(2L);
        Rebufferer tor = new TailOverridingRebufferer(r, 8, tail.duplicate());

        for (int i = 2; i < 8; i++)
        {
            Rebufferer.BufferHolder bh = tor.rebuffer(i);
            assertEquals(head.limit(6), bh.buffer());
            bh.release();
        }

        assertEquals(10, tor.fileLength());
    }

    @Test
    public void testAccessRightToTail()
    {
        when(r.rebuffer(anyLong())).thenReturn(bh);
        when(bh.buffer()).thenReturn(head.duplicate());
        when(bh.offset()).thenReturn(0L);
        Rebufferer tor = new TailOverridingRebufferer(r, 8, tail.duplicate());

        for (int i = 8; i < 10; i++)
        {
            Rebufferer.BufferHolder bh = tor.rebuffer(i);
            assertEquals(tail, bh.buffer());
            bh.release();
        }

        assertEquals(10, tor.fileLength());
    }

    @Test
    public void testOtherMethods()
    {
        Rebufferer tor = new TailOverridingRebufferer(r, 8, tail.duplicate());

        File tmp = FileUtils.createTempFile("fakeChannelProxy", "");
        try (ChannelProxy channelProxy = new ChannelProxy(tmp))
        {
            when(r.channel()).thenReturn(channelProxy);
            assertSame(channelProxy, tor.channel());
            verify(r).channel();
            reset(r);
        }

        tor.closeReader();
        verify(r).closeReader();
        reset(r);

        tor.close();
        verify(r).close();
        reset(r);

        when(r.getCrcCheckChance()).thenReturn(0.123d);
        assertEquals(0.123d, tor.getCrcCheckChance(), 0);
        verify(r).getCrcCheckChance();
        reset(r);
    }
}