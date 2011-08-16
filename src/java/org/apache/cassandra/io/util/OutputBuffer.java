/**
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

import java.io.DataInput;
import java.io.IOException;

/**
 * Extends FastByteArrayOutputStream to minimize copies.
 */
public final class OutputBuffer extends FastByteArrayOutputStream
{
    public OutputBuffer()
    {
        this(128);
    }

    public OutputBuffer(int size)
    {
        super(size);
    }

    public byte[] getData()
    {
        return buf;
    }
    
    public int getLength()
    {
        return count;
    }
    
    public void write(DataInput in, int len) throws IOException
    {
        int newcount = count + len;
        if (newcount > buf.length)
        {
            byte newbuf[] = new byte[Math.max(buf.length << 1, newcount)];
            System.arraycopy(buf, 0, newbuf, 0, count);
            buf = newbuf;
        }
        in.readFully(buf, count, len);
        count = newcount;
    }
    
    /**
     * @return The valid contents of the buffer, possibly by copying: only safe for one-time-use buffers.
     */
    public byte[] asByteArray()
    {
        if (count == buf.length)
            // no-copy
            return buf;
        // copy
        return this.toByteArray();
    }
}
