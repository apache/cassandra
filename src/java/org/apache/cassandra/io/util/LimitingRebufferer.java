/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.io.util;

import java.nio.ByteBuffer;

import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.RateLimiter;

/**
 * Rebufferer wrapper that applies rate limiting.
 *
 * Instantiated once per RandomAccessReader, thread-unsafe.
 * The instances reuse themselves as the BufferHolder to avoid having to return a new object for each rebuffer call.
 */
public class LimitingRebufferer implements Rebufferer, Rebufferer.BufferHolder
{
    final private Rebufferer wrapped;
    final private RateLimiter limiter;
    final private int limitQuant;

    private BufferHolder bufferHolder;
    private ByteBuffer buffer;
    private long offset;

    public LimitingRebufferer(Rebufferer wrapped, RateLimiter limiter, int limitQuant)
    {
        this.wrapped = wrapped;
        this.limiter = limiter;
        this.limitQuant = limitQuant;
    }

    @Override
    public BufferHolder rebuffer(long position)
    {
        bufferHolder = wrapped.rebuffer(position);
        buffer = bufferHolder.buffer();
        offset = bufferHolder.offset();
        int posInBuffer = Ints.checkedCast(position - offset);
        int remaining = buffer.limit() - posInBuffer;
        if (remaining == 0)
            return this;

        if (remaining > limitQuant)
        {
            buffer.limit(posInBuffer + limitQuant); // certainly below current limit
            remaining = limitQuant;
        }
        limiter.acquire(remaining);
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
        return "LimitingRebufferer[" + limiter.toString() + "]:" + wrapped.toString();
    }

    // BufferHolder methods

    @Override
    public ByteBuffer buffer()
    {
        return buffer;
    }

    @Override
    public long offset()
    {
        return offset;
    }

    @Override
    public void release()
    {
        bufferHolder.release();
    }
}
