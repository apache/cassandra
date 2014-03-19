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
import java.nio.ByteBuffer;

import org.apache.cassandra.utils.ByteBufferUtil;


/**
 * An implementation of the DataOutputStream interface using a FastByteArrayOutputStream and exposing
 * its buffer so copies can be avoided.
 *
 * This class is completely thread unsafe.
 */
public final class DataOutputByteBuffer extends AbstractDataOutput
{

    final ByteBuffer buffer;
    public DataOutputByteBuffer(ByteBuffer buffer)
    {
        this.buffer = buffer;
    }

    @Override
    public void write(int b)
    {
        buffer.put((byte) b);
    }

    @Override
    public void write(byte[] b, int off, int len)
    {
        buffer.put(b, off, len);
    }

    public void write(ByteBuffer buffer) throws IOException
    {
        int len = buffer.remaining();
        ByteBufferUtil.arrayCopy(buffer, buffer.position(), this.buffer, this.buffer.position(), len);
        this.buffer.position(this.buffer.position() + len);
    }
}
