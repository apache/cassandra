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
package org.apache.cassandra.db.composites;

import java.nio.ByteBuffer;

import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.utils.memory.AbstractAllocator;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.memory.PoolAllocator;

public abstract class Composites
{
    private Composites() {}

    public static final Composite EMPTY = new EmptyComposite();

    static final CBuilder EMPTY_BUILDER = new CBuilder()
    {
        public int remainingCount() { return 0; }

        public CBuilder add(ByteBuffer value) { throw new IllegalStateException(); }
        public CBuilder add(Object value) { throw new IllegalStateException(); }

        public Composite build() { return EMPTY; }
        public Composite buildWith(ByteBuffer value) { throw new IllegalStateException(); }
    };

    private static class EmptyComposite implements Composite
    {
        public boolean isEmpty()
        {
            return true;
        }

        public int size()
        {
            return 0;
        }

        public ByteBuffer get(int i)
        {
            throw new IndexOutOfBoundsException();
        }

        public EOC eoc()
        {
            return EOC.NONE;
        }

        public Composite start()
        {
            return this;
        }

        public Composite end()
        {
            return this;
        }

        public Composite withEOC(EOC newEoc)
        {
            return this;
        }

        public ColumnSlice slice()
        {
            return ColumnSlice.ALL_COLUMNS;
        }

        public ByteBuffer toByteBuffer()
        {
            return ByteBufferUtil.EMPTY_BYTE_BUFFER;
        }

        public boolean isStatic()
        {
            return false;
        }

        public int dataSize()
        {
            return 0;
        }

        public long unsharedHeapSize()
        {
            return 0;
        }

        public boolean isPrefixOf(Composite c)
        {
            return true;
        }

        public Composite copy(AbstractAllocator allocator)
        {
            return this;
        }

        @Override
        public void free(PoolAllocator<?> allocator)
        {
        }

    }
}
