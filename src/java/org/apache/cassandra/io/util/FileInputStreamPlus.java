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

import org.checkerframework.checker.calledmethods.qual.EnsuresCalledMethods;
import org.checkerframework.checker.mustcall.qual.MustCallAlias;
import org.checkerframework.checker.mustcall.qual.Owning;

public class FileInputStreamPlus extends RebufferingInputStream
{
    final @Owning FileChannel channel;
    public final File file;

    public FileInputStreamPlus(String file) throws NoSuchFileException
    {
        this(new File(file));
    }

    public FileInputStreamPlus(Path path) throws NoSuchFileException
    {
        this(new File(path));
    }

    public FileInputStreamPlus(File file) throws NoSuchFileException
    {
        this(file, 1 << 14);
    }

    public FileInputStreamPlus(File file, int bufferSize) throws NoSuchFileException
    {
        super(ByteBuffer.allocateDirect(bufferSize));
        this.channel = file.newReadChannel();
        this.buffer.limit(0);
        this.file = file;
    }

    @Override
    protected void reBuffer() throws IOException
    {
        buffer.clear();
        channel.read(buffer);
        buffer.flip();
    }

    public @MustCallAlias FileChannel getChannel()
    {
        return channel;
    }

    @EnsuresCalledMethods(value = "this.channel", methods = "close")
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
