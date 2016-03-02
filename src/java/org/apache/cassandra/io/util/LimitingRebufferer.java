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
