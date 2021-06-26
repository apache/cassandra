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
package org.apache.cassandra.net;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.apache.cassandra.utils.memory.BufferPools;

/**
 * A wrapper for possibly sharing portions of a single, {@link BufferPools#forNetworking()} managed, {@link ByteBuffer};
 * optimised for the case where no sharing is necessary.
 *
 * When sharing is necessary, {@link #share()} method must be invoked by the owning thread
 * before a {@link ShareableBytes} instance can be shared with another thread.
 */
public class ShareableBytes
{
    private final ByteBuffer bytes;
    private final ShareableBytes owner;
    private volatile int count;

    private static final int UNSHARED = -1;
    private static final int RELEASED = 0;
    private static final AtomicIntegerFieldUpdater<ShareableBytes> countUpdater =
        AtomicIntegerFieldUpdater.newUpdater(ShareableBytes.class, "count");

    private ShareableBytes(ByteBuffer bytes)
    {
        this.count = UNSHARED;
        this.owner = this;
        this.bytes = bytes;
    }

    private ShareableBytes(ShareableBytes owner, ByteBuffer bytes)
    {
        this.owner = owner;
        this.bytes = bytes;
    }

    public ByteBuffer get()
    {
        assert owner.count != 0;
        return bytes;
    }

    public boolean hasRemaining()
    {
        return bytes.hasRemaining();
    }

    public int remaining()
    {
        return bytes.remaining();
    }

    void skipBytes(int skipBytes)
    {
        bytes.position(bytes.position() + skipBytes);
    }

    void consume()
    {
        bytes.position(bytes.limit());
    }

    /**
     * Ensure this ShareableBytes will use atomic operations for updating its count from now on.
     * The first invocation must occur while the calling thread has exclusive access (though there may be more
     * than one 'owner', these must all either be owned by the calling thread or otherwise not being used)
     */
    public ShareableBytes share()
    {
        int count = owner.count;
        if (count < 0)
            owner.count = -count;
        return this;
    }

    private ShareableBytes retain()
    {
        owner.doRetain();
        return this;
    }

    private void doRetain()
    {
        int count = this.count;
        if (count < 0)
        {
            countUpdater.lazySet(this, count - 1);
            return;
        }

        while (true)
        {
            if (count == RELEASED)
                throw new IllegalStateException("Attempted to reference an already released SharedByteBuffer");

            if (countUpdater.compareAndSet(this, count, count + 1))
                return;

            count = this.count;
        }
    }

    public void release()
    {
        owner.doRelease();
    }

    private void doRelease()
    {
        int count = this.count;

        if (count < 0)
            countUpdater.lazySet(this, count += 1);
        else if (count > 0)
            count = countUpdater.decrementAndGet(this);
        else
            throw new IllegalStateException("Already released");

        if (count == RELEASED)
            BufferPools.forNetworking().put(bytes);
    }

    boolean isReleased()
    {
        return owner.count == RELEASED;
    }

    /**
     * Create a slice over the next {@code length} bytes, consuming them from our buffer, and incrementing the owner count
     */
    public ShareableBytes sliceAndConsume(int length)
    {
        int begin = bytes.position();
        int end = begin + length;
        ShareableBytes result = slice(begin, end);
        bytes.position(end);
        return result;
    }

    /**
     * Create a new slice, incrementing the number of owners (making it shared if it was previously unshared)
     */
    ShareableBytes slice(int begin, int end)
    {
        ByteBuffer slice = bytes.duplicate();
        slice.position(begin).limit(end);
        return new ShareableBytes(owner.retain(), slice);
    }

    public static ShareableBytes wrap(ByteBuffer buffer)
    {
        return new ShareableBytes(buffer);
    }
}

