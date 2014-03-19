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

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;

public class DataOutputStreamAndChannel extends DataOutputStreamPlus
{
    private final WritableByteChannel channel;
    public DataOutputStreamAndChannel(OutputStream os, WritableByteChannel channel)
    {
        super(os);
        this.channel = channel;
    }
    public DataOutputStreamAndChannel(WritableByteChannel channel)
    {
        this(Channels.newOutputStream(channel), channel);
    }
    public DataOutputStreamAndChannel(FileOutputStream fos)
    {
        this(fos, fos.getChannel());
    }

    public void write(ByteBuffer buffer) throws IOException
    {
        buffer = buffer.duplicate();
        while (buffer.remaining() > 0)
            channel.write(buffer);
    }

    public WritableByteChannel getChannel()
    {
        return channel;
    }
}
