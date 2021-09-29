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
package org.apache.cassandra.io.compress;

import java.nio.ByteBuffer;

public enum BufferType
{
    ON_HEAP
    {
        public ByteBuffer allocate(int size)
        {
            return ByteBuffer.allocate(size);
        }
    },
    OFF_HEAP
    {
        public ByteBuffer allocate(int size)
        {
            return ByteBuffer.allocateDirect(size);
        }
    };

    public abstract ByteBuffer allocate(int size);

    public static BufferType typeOf(ByteBuffer buffer)
    {
        return buffer.isDirect() ? OFF_HEAP : ON_HEAP;
    }

    /**
     * Allocate a buffer of the current type, write the content to it and return it.
     * The buffer is repositioned at the beginning, and it's ready for reading.
     *
     * @param content - the content to write
     *
     * @return a byte buffer with the specified content written in it
     */
    public ByteBuffer withContent(byte[] content)
    {
        ByteBuffer ret = allocate(content.length);
        ret.put(content);
        ret.rewind();
        return ret;
    }
}
