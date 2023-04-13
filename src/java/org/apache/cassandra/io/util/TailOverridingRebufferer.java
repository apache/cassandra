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
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Special rebufferer that replaces the tail of the file (from the specified cutoff point) with the given buffer.
 * <p>
 * Instantiated once per RandomAccessReader, thread-unsafe.
 * The instances reuse themselves as the BufferHolder to avoid having to return a new object for each rebuffer call.
 * Only one BufferHolder can be active at a time. Calling {@link #rebuffer(long)} before the previously obtained
 * buffer holder is released will throw {@link AssertionError}.
 */
@NotThreadSafe
public class TailOverridingRebufferer extends WrappingRebufferer
{
    private final long cutoff;
    private final ByteBuffer tail;

    public TailOverridingRebufferer(Rebufferer source, long cutoff, ByteBuffer tail)
    {
        super(source);
        this.cutoff = cutoff;
        this.tail = tail;
    }

    @Override
    public Rebufferer.BufferHolder rebuffer(long position)
    {
        assert buffer == null : "Buffer holder has been already acquired and has been not released yet";
        if (position < cutoff)
        {
            super.rebuffer(position);
            if (offset + buffer.limit() > cutoff)
                buffer.limit((int) (cutoff - offset));
        }
        else
        {
            buffer = tail.duplicate();
            offset = cutoff;
        }
        return this;
    }

    @Override
    public long fileLength()
    {
        return cutoff + tail.limit();
    }

    @Override
    public String toString()
    {
        return String.format("%s[+%d@%d]:%s", getClass().getSimpleName(), tail.limit(), cutoff, wrapped.toString());
    }
}
