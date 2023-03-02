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
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.StandardOpenOption;

import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.utils.NativeLibrary;
import org.apache.cassandra.utils.concurrent.RefCounted;
import org.apache.cassandra.utils.concurrent.SharedCloseableImpl;

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
    private final File file;
    private final String filePath;
    private final FileChannel channel;

    public static FileChannel openChannel(File file)
    {
        try
        {
            return FileChannel.open(file.toPath(), StandardOpenOption.READ);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public ChannelProxy(String path)
    {
        this (new File(path));
    }

    public ChannelProxy(File file)
    {
        this(file, openChannel(file));
    }

    public ChannelProxy(File file, FileChannel channel)
    {
        super(new Cleanup(file.path(), channel));

        this.file = file;
        this.filePath = file.path();
        this.channel = channel;
    }

    public ChannelProxy(ChannelProxy copy)
    {
        super(copy);

        this.file = copy.file;
        this.filePath = copy.filePath;
        this.channel = copy.channel;
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

    /**
     * {@link #sharedCopy()} can not be used if thread will be interruped, as the backing channel will be closed.
     *
     * @return a new channel instance
     */
    public final ChannelProxy newChannel()
    {
        return new ChannelProxy(filePath);
    }

    public ChannelProxy sharedCopy()
    {
        return new ChannelProxy(this);
    }

    public String filePath()
    {
        return filePath;
    }

    public File file()
    {
        return file;
    }

    public int read(ByteBuffer buffer, long position)
    {
        try
        {
            // FIXME: consider wrapping in a while loop
            return channel.read(buffer, position);
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
