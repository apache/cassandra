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
package org.apache.cassandra.cache;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.apache.cassandra.io.util.Memory;

public class RefCountedMemory extends Memory implements AutoCloseable
{
    private volatile int references = 1;
    private static final AtomicIntegerFieldUpdater<RefCountedMemory> UPDATER = AtomicIntegerFieldUpdater.newUpdater(RefCountedMemory.class, "references");

    public RefCountedMemory(long size)
    {
        super(size);
    }

    /**
     * @return true if we succeed in referencing before the reference count reaches zero.
     * (A FreeableMemory object is created with a reference count of one.)
     */
    public boolean reference()
    {
        while (true)
        {
            int n = UPDATER.get(this);
            if (n <= 0)
                return false;
            if (UPDATER.compareAndSet(this, n, n + 1))
                return true;
        }
    }

    /** decrement reference count.  if count reaches zero, the object is freed. */
    public void unreference()
    {
        if (UPDATER.decrementAndGet(this) == 0)
            super.free();
    }

    public RefCountedMemory copy(long newSize)
    {
        RefCountedMemory copy = new RefCountedMemory(newSize);
        copy.put(0, this, 0, Math.min(size(), newSize));
        return copy;
    }

    public void free()
    {
        throw new AssertionError();
    }

    public void close()
    {
        unreference();
    }
}
