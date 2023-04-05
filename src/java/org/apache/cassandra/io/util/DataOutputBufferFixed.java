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

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;


/**
 * An implementation of the DataOutputStream interface using a FastByteArrayOutputStream and exposing
 * its buffer so copies can be avoided. This version does not expand if it runs out of capacity and
 * throws BufferOverflowException instead.
 *
 * This class is completely thread unsafe.
 */
public class DataOutputBufferFixed extends DataOutputBuffer
{
    public DataOutputBufferFixed()
    {
        this(128);
    }

    public DataOutputBufferFixed(int size)
    {
        super(size);
    }

    public DataOutputBufferFixed(ByteBuffer buffer)
    {
        super(buffer);
    }

    @Override
    protected void doFlush(int count) throws IOException
    {
        throw new BufferOverflowException();
    }

    /*
     * Not currently reachable (all paths hit doFLush first), but in the spirit of things this should throw
     * if it is called.
     * @see org.apache.cassandra.io.util.DataOutputBuffer#reallocate(long)
     */
    @Override
    protected void expandToFit(long newSize)
    {
        throw new BufferOverflowException();
    }

    public void clear()
    {
        buffer.clear();
    }
}
