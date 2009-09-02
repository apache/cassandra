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

package org.apache.cassandra.io;

import java.io.*;


/**
 * An implementation of the DataInputStream interface. This instance is completely thread 
 * unsafe.
 */

public final class DataInputBuffer extends DataInputStream
{
    private static class Buffer extends ByteArrayInputStream
    {        
        public Buffer()
        {
            super(new byte[] {});
        }

        public void reset(byte[] input, int start, int length)
        {
            this.buf = input;
            this.count = start + length;
            this.mark = start;
            this.pos = start;
        }
        
        public int getPosition()
        {
            return pos;
        }
        
        public void setPosition(int position)
        {
            pos = position;
        }        

        public int getLength()
        {
            return count;
        }
    }

    private Buffer buffer_;

    /** Constructs a new empty buffer. */
    public DataInputBuffer()
    {
        this(new Buffer());
    }

    private DataInputBuffer(Buffer buffer)
    {
        super(buffer);
        this.buffer_ = buffer;
    }
   
    /** Resets the data that the buffer reads. */
    public void reset(byte[] input, int length)
    {
        buffer_.reset(input, 0, length);
    }

    /** Resets the data that the buffer reads. */
    public void reset(byte[] input, int start, int length)
    {
        buffer_.reset(input, start, length);
    }

    /** Returns the length of the input. */
    public int getLength()
    {
        return buffer_.getLength();
    }

    public int getPosition()
    {
        return buffer_.getPosition();
    }
}
