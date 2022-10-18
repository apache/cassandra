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
import java.nio.channels.FileChannel;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;

public class FileInputStreamPlus extends RebufferingInputStream
{
    final FileChannel channel;

    public FileInputStreamPlus(String file) throws NoSuchFileException
    {
        this(new File(file));
    }

    public FileInputStreamPlus(File file) throws NoSuchFileException
    {
        this(file.newReadChannel());
    }

    public FileInputStreamPlus(Path path) throws NoSuchFileException
    {
        this(PathUtils.newReadChannel(path));
    }

    public FileInputStreamPlus(Path path, int bufferSize) throws NoSuchFileException
    {
        this(PathUtils.newReadChannel(path), bufferSize);
    }

    private FileInputStreamPlus(FileChannel channel)
    {
        this(channel, 1 << 14);
    }

    private FileInputStreamPlus(FileChannel channel, int bufferSize)
    {
        super(ByteBuffer.allocateDirect(bufferSize));
        this.channel = channel;
        this.buffer.limit(0);
    }

    @Override
    protected void reBuffer() throws IOException
    {
        buffer.clear();
        channel.read(buffer);
        buffer.flip();
    }

    public FileChannel getChannel()
    {
        return channel;
    }

    @Override
    public void close() throws IOException
    {
        try
        {
            super.close();
        }
        finally
        {
            try
            {
                FileUtils.clean(buffer);
            }
            finally
            {
                channel.close();
            }
        }
    }
}
