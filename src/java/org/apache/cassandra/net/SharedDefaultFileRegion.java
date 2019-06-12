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

import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.channel.DefaultFileRegion;
import org.apache.cassandra.utils.concurrent.Ref;
import org.apache.cassandra.utils.concurrent.RefCounted;

/**
 * Netty's DefaultFileRegion closes the underlying FileChannel as soon as
 * the refCnt() for the region drops to zero, this is an implementation of
 * the DefaultFileRegion that doesn't close the FileChannel.
 *
 * See {@link AsyncChannelOutputPlus} for its usage.
 */
public class SharedDefaultFileRegion extends DefaultFileRegion
{
    public static class SharedFileChannel
    {
        // we don't call .ref() on this, because it would generate a lot of PhantomReferences and GC overhead,
        // but we use it to ensure we can spot memory leaks
        final Ref<FileChannel> ref;
        final AtomicInteger refCount = new AtomicInteger(1);

        SharedFileChannel(FileChannel fileChannel)
        {
            this.ref = new Ref<>(fileChannel, new RefCounted.Tidy()
            {
                public void tidy() throws Exception
                {
                    // don't mind invoking this on eventLoop, as only used with sendFile which is also blocking
                    // so must use streaming eventLoop
                    fileChannel.close();
                }

                public String name()
                {
                    return "SharedFileChannel[" + fileChannel.toString() + ']';
                }
            });
        }

        public void release()
        {
            if (0 == refCount.decrementAndGet())
                ref.release();
        }
    }

    private final SharedFileChannel shared;
    private boolean deallocated = false;

    SharedDefaultFileRegion(SharedFileChannel shared, long position, long count)
    {
        super(shared.ref.get(), position, count);
        this.shared = shared;
        if (1 >= this.shared.refCount.incrementAndGet())
            throw new IllegalStateException();
    }

    @Override
    protected void deallocate()
    {
        if (deallocated)
            return;
        deallocated = true;
        shared.release();
    }

    public static SharedFileChannel share(FileChannel fileChannel)
    {
        return new SharedFileChannel(fileChannel);
    }
}
