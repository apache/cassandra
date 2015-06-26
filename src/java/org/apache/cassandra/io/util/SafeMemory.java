/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.io.util;

import net.nicoulaj.compilecommand.annotations.Inline;
import org.apache.cassandra.utils.concurrent.Ref;
import org.apache.cassandra.utils.concurrent.RefCounted;
import org.apache.cassandra.utils.concurrent.SharedCloseable;
import org.apache.cassandra.utils.memory.MemoryUtil;

public class SafeMemory extends Memory implements SharedCloseable
{
    private final Ref<?> ref;
    public SafeMemory(long size)
    {
        super(size);
        ref = new Ref<>(null, new MemoryTidy(peer, size));
    }

    private SafeMemory(SafeMemory copyOf)
    {
        super(copyOf);
        ref = copyOf.ref.ref();
        /** see {@link Memory#Memory(long)} re: null pointers*/
        if (peer == 0 && size != 0)
        {
            ref.ensureReleased();
            throw new IllegalStateException("Cannot create a sharedCopy of a SafeMemory object that has already been closed");
        }
    }

    public SafeMemory sharedCopy()
    {
        return new SafeMemory(this);
    }

    public void free()
    {
        ref.release();
        peer = 0;
    }

    public void close()
    {
        ref.ensureReleased();
        peer = 0;
    }

    public Throwable close(Throwable accumulate)
    {
        return ref.ensureReleased(accumulate);
    }

    public SafeMemory copy(long newSize)
    {
        SafeMemory copy = new SafeMemory(newSize);
        copy.put(0, this, 0, Math.min(size(), newSize));
        return copy;
    }

    private static final class MemoryTidy implements RefCounted.Tidy
    {
        final long peer;
        final long size;
        private MemoryTidy(long peer, long size)
        {
            this.peer = peer;
            this.size = size;
        }

        public void tidy() throws Exception
        {
            /** see {@link Memory#Memory(long)} re: null pointers*/
            if (peer != 0)
                MemoryUtil.free(peer);
        }

        public String name()
        {
            return Memory.toString(peer, size);
        }
    }

    @Inline
    protected void checkBounds(long start, long end)
    {
        assert peer != 0 || size == 0 : ref.printDebugInfo();
        super.checkBounds(start, end);
    }

    public void addTo(Ref.IdentityCollection identities)
    {
        identities.add(ref);
    }
}
