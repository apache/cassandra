package org.apache.cassandra.cache;
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


import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import com.sun.jna.Memory;

public class FreeableMemory extends Memory
{
    AtomicInteger references = new AtomicInteger(1);

    public FreeableMemory(long size)
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
            int n = references.get();
            if (n <= 0)
                return false;
            if (references.compareAndSet(n, n + 1))
                return true;
        }
    }

    /** decrement reference count.  if count reaches zero, the object is freed. */
    public void unreference()
    {
        if (references.decrementAndGet() == 0)
            free();
    }

    private void free()
    {
        assert peer != 0;
        super.finalize(); // calls free and sets peer to zero
    }

    /**
     * avoid re-freeing already-freed memory
     */
    @Override
    protected void finalize()
    {
        assert references.get() <= 0;
        assert peer == 0;
    }
    
    public byte getValidByte(long offset)
    {
        assert peer != 0;
        return super.getByte(offset);
    }
}
