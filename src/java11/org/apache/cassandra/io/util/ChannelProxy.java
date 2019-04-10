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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.StandardOpenOption;
import com.sun.nio.file.ExtendedOpenOption;

import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.utils.NativeLibrary;
import org.apache.cassandra.utils.concurrent.RefCounted;
import org.apache.cassandra.utils.concurrent.SharedCloseableImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * A proxy of a FileChannel that:
 *
 * - implements reference counting
 * - exports only thread safe FileChannel operations
 * - wraps IO exceptions into runtime exceptions
 *
 * Tested by RandomAccessReaderTest.
 */
public final class ChannelProxy extends SharedCloseableImpl
{
    private final String filePath;
    private final FileChannel channel;
    private boolean useDirectIO;
    private static final Logger logger = LoggerFactory.getLogger(ChannelProxy.class);

    public static FileChannel openChannel(File file, boolean useDirectIO)
    {
        try
        {
            return useDirectIO ?
                   FileChannel.open(file.toPath(), StandardOpenOption.READ, ExtendedOpenOption.DIRECT) :
                   FileChannel.open(file.toPath(), StandardOpenOption.READ);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public static FileChannel openChannel(File file)
    {
        return openChannel(file, false);
    }

    public ChannelProxy(String filePath, FileChannel channel, boolean useDirectIO)
    {
        super(new Cleanup(filePath, channel));

        this.filePath = filePath;
        this.channel = channel;
        this.useDirectIO = useDirectIO;
    }

    public ChannelProxy(String path, FileChannel channel) {
        this(path, channel, false);
    }

    public ChannelProxy(File file, FileChannel channel)
    {
        this(file.getPath(), channel, false);
    }

    public ChannelProxy(File file, boolean useDirectIO) {
        this(file.getPath(), openChannel(file, useDirectIO), useDirectIO);
    }

    public ChannelProxy(File file)
    {
        this(file.getPath(), openChannel(file));
    }

    public ChannelProxy(String path, boolean useDirectIO)
    {
        this (new File(path), useDirectIO);
    }

    public ChannelProxy(String path)
    {
        this (new File(path));
    }

    public ChannelProxy(ChannelProxy copy)
    {
        super(copy);

        this.filePath = copy.filePath;
        this.channel = copy.channel;
        this.useDirectIO = copy.useDirectIO;
    }

    private final static class Cleanup implements RefCounted.Tidy
    {
        final String filePath;
        final FileChannel channel;

        Cleanup(String filePath, FileChannel channel)
        {
            this.filePath = filePath;
            this.channel = channel;
        }

        public String name()
        {
            return filePath;
        }

        public void tidy()
        {
            try
            {
                channel.close();
            }
            catch (IOException e)
            {
                throw new FSReadError(e, filePath);
            }
        }
    }

    public ChannelProxy sharedCopy()
    {
        return new ChannelProxy(this);
    }

    public String filePath()
    {
        return filePath;
    }

    public int read(ByteBuffer buffer, long position)
    {
        try
        {
            // FIXME: consider wrapping in a while loop
            return useDirectIO ? DirectIOUtils.read(channel, buffer, position) : channel.read(buffer, position);
        }
        catch (IOException e)
        {
            throw new FSReadError(e, filePath);
        }
    }

    public long transferTo(long position, long count, WritableByteChannel target)
    {
        try
        {
            return channel.transferTo(position, count, target);
        }
        catch (IOException e)
        {
            throw new FSReadError(e, filePath);
        }
    }

    public MappedByteBuffer map(FileChannel.MapMode mode, long position, long size)
    {
        try
        {
            return channel.map(mode, position, size);
        }
        catch (IOException e)
        {
            throw new FSReadError(e, filePath);
        }
    }

    public long size()
    {
        try
        {
            return channel.size();
        }
        catch (IOException e)
        {
            throw new FSReadError(e, filePath);
        }
    }

    public int getFileDescriptor()
    {
        return NativeLibrary.getfd(channel);
    }

    @Override
    public String toString()
    {
        return filePath();
    }
}
