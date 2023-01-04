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
 * Instantiated once per RandomAccessReader, thread-unsafe.
 * The instances reuse themselves as the BufferHolder to avoid having to return a new object for each rebuffer call.
 * Only one buffer holder can be active at a time. Calling {@link #rebuffer(long)} before the previously obtained
 * buffer holder is released will throw {@link AssertionError}. We will get that exception also in case we try to close
 * the rebufferer without closing the recently obtained buffer holder.
 * <p>
 * Calling methods of {@link BufferHolder} will also produce {@link AssertionError} if buffer holder is not acquired.
 * <p>
 * The overriding classes must conform to the aforementioned rules.
 */
@NotThreadSafe
public abstract class WrappingRebufferer implements Rebufferer, Rebufferer.BufferHolder
{
    protected final Rebufferer wrapped;

    protected BufferHolder bufferHolder;
    protected ByteBuffer buffer;
    protected long offset;

    public WrappingRebufferer(Rebufferer wrapped)
    {
        this.wrapped = wrapped;
    }

    @Override
    public BufferHolder rebuffer(long position)
    {
        assert buffer == null;
        bufferHolder = wrapped.rebuffer(position);
        buffer = bufferHolder.buffer();
        offset = bufferHolder.offset();

        return this;
    }

    @Override
    public ChannelProxy channel()
    {
        return wrapped.channel();
    }

    @Override
    public long fileLength()
    {
        return wrapped.fileLength();
    }

    @Override
    public double getCrcCheckChance()
    {
        return wrapped.getCrcCheckChance();
    }

    @Override
    public void close()
    {
        assert buffer == null : "Rebufferer is attempted to be closed but the buffer holder has not been released";
        wrapped.close();
    }

    @Override
    public void closeReader()
    {
        wrapped.closeReader();
    }

    @Override
    public String toString()
    {
        return String.format("%s[]:%s", getClass().getSimpleName(), wrapped.toString());
    }

    @Override
    public ByteBuffer buffer()
    {
        assert buffer != null : "Buffer holder has not been acquired";
        return buffer;
    }

    @Override
    public long offset()
    {
        assert buffer != null : "Buffer holder has not been acquired";
        return offset;
    }

    @Override
    public void release()
    {
        assert buffer != null;
        if (bufferHolder != null)
        {
            bufferHolder.release();
            bufferHolder = null;
        }
        buffer = null;
    }
}