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
package org.apache.cassandra.utils.memory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.cassandra.utils.concurrent.OpOrder;

public abstract class MemtableBufferAllocator extends MemtableAllocator
{
    protected MemtableBufferAllocator(SubAllocator onHeap, SubAllocator offHeap)
    {
        super(onHeap, offHeap);
    }

    public abstract ByteBuffer allocate(int size, OpOrder.Group opGroup);

    protected Cloner allocator(OpOrder.Group opGroup)
    {
        return new MemtableByteBufferCloner(this, opGroup);
    }

    private static class MemtableByteBufferCloner extends ByteBufferCloner
    {
        private final MemtableBufferAllocator allocator;
        private final OpOrder.Group opGroup;
        private final ByteBuffer contextSlab;

        private MemtableByteBufferCloner(MemtableBufferAllocator allocator, OpOrder.Group opGroup)
        {
            this(allocator, opGroup, null);
        }

        private MemtableByteBufferCloner(MemtableBufferAllocator allocator, OpOrder.Group opGroup, ByteBuffer contextSlab)
        {
            this.allocator = allocator;
            this.opGroup = opGroup;
            this.contextSlab = contextSlab;
        }

        @Override
        public ByteBuffer allocate(int size)
        {
            if (contextSlab == null || contextSlab.remaining() < size)
                return allocator.allocate(size, opGroup);

            ByteBuffer result;
            int currentPosition = contextSlab.position();
            if (contextSlab.isDirect())
            {
                // we do not want to keep contextSlab as an attachment for our allocated buffers
                // it would increase a memory pressure by keeping contextSlab instances while a memtable is alive,
                // so we have to imitate that the allocated direct byte buffer is a child of the original parent region
                result = MemoryUtil.getHollowDirectByteBuffer(ByteOrder.BIG_ENDIAN);
                MemoryUtil.duplicateDirectByteBuffer(contextSlab, result);
                MemoryUtil.setAttachment(result, MemoryUtil.getAttachment(contextSlab));
            }
            else
            {
                result = contextSlab.duplicate();
            }
            result.limit(currentPosition + size);
            contextSlab.position(currentPosition + size);
            return result;
        }

        @Override
        public boolean isContextAwareCloningSupported()
        {
            return true;
        }

        @Override
        public Cloner createContextAwareCloner(int estimatedCloneSize)
        {
           return new MemtableByteBufferCloner(allocator, opGroup, allocator.allocate(estimatedCloneSize, opGroup));
        }

        @Override
        public void adjustUnused()
        {
            if (contextSlab != null && contextSlab.remaining() >= 0)
            {
                int remaining = contextSlab.remaining();
                if (contextSlab.isDirect())
                    allocator.offHeap().adjust(-remaining);
                else
                    allocator.onHeap().adjust(-remaining);
                contextSlab.position(contextSlab.position() + remaining);
            }
        }
    }
}
