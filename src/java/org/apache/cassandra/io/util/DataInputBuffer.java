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
package org.apache.cassandra.io.util;

import java.nio.ByteBuffer;

/**
 * Input stream around a single ByteBuffer.
 */
public class DataInputBuffer extends RebufferingInputStream
{
    private static ByteBuffer slice(byte[] buffer, int offset, int length)
    {
        ByteBuffer buf = ByteBuffer.wrap(buffer);
        if (offset > 0 || length < buf.capacity())
        {
            buf.position(offset);
            buf.limit(offset + length);
            buf = buf.slice();
        }
        return buf;
    }

    /**
     * @param buffer
     * @param duplicate Whether or not to duplicate the buffer to ensure thread safety
     */
    public DataInputBuffer(ByteBuffer buffer, boolean duplicate)
    {
        super(duplicate ? buffer.duplicate() : buffer);
    }

    public DataInputBuffer(byte[] buffer, int offset, int length)
    {
        super(slice(buffer, offset, length));
    }

    public DataInputBuffer(byte[] buffer)
    {
        super(ByteBuffer.wrap(buffer));
    }

    @Override
    protected void reBuffer()
    {
        //nope, we don't rebuffer, we are done!
    }

    @Override
    public int available()
    {
        return buffer.remaining();
    }

    @Override
    public void close() {}
}
