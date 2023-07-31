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

import org.apache.cassandra.utils.memory.BufferPool;

import io.netty.buffer.ByteBuf;

/**
 * Primary {@link ByteBuf} / {@link ByteBuffer} allocator - using the global {@link BufferPool}.
 */
public class GlobalBufferPoolAllocator extends BufferPoolAllocator
{
    public static final GlobalBufferPoolAllocator instance = new GlobalBufferPoolAllocator();

    private GlobalBufferPoolAllocator()
    {
        super();
    }

    public static ByteBuf wrap(ByteBuffer buffer)
    {
        return new Wrapped(instance, buffer, buffer.capacity());
    }
}
