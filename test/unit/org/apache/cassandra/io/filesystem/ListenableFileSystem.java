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

package org.apache.cassandra.io.filesystem;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.FileSystem;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.spi.FileSystemProvider;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.memory.MemoryUtil;

public class ListenableFileSystem extends ForwardingFileSystem
{
    public interface Listener
    {
    }

    public interface OnOpen extends Listener
    {
        default void preOpen(Path path, Set<? extends OpenOption> options, FileAttribute<?>[] attrs) throws IOException
        {
        }

        void postOpen(Path path, Set<? extends OpenOption> options, FileAttribute<?>[] attrs, FileChannel channel) throws IOException;
    }

    public interface OnRead extends Listener
    {
        default void preRead(Path path, FileChannel channel, long position, ByteBuffer dst) throws IOException
        {
        }

        void postRead(Path path, FileChannel channel, long position, ByteBuffer dst, int read) throws IOException;
    }

    public interface OnTransferTo extends Listener
    {
        default void preTransferTo(Path path, FileChannel channel, long position, long count, WritableByteChannel target) throws IOException
        {
        }

        void postTransferTo(Path path, FileChannel channel, long position, long count, WritableByteChannel target, long transfered) throws IOException;
    }

    public interface OnTransferFrom extends Listener
    {
        default void preTransferFrom(Path path, FileChannel channel, ReadableByteChannel src, long position, long count) throws IOException
        {
        }

        void postTransferFrom(Path path, FileChannel channel, ReadableByteChannel src, long position, long count, long transfered) throws IOException;
    }

    public interface OnWrite extends Listener
    {
        void preWrite(Path path, FileChannel channel, long position, ByteBuffer src) throws IOException;

        void postWrite(Path path, FileChannel channel, long position, ByteBuffer src, int wrote) throws IOException;
    }

    public interface OnChannelMeta extends Listener
    {
        default void prePosition(Path path, FileChannel channel, long position, long newPosition) throws IOException
        {
        }

        default void postPosition(Path path, FileChannel channel, long position, long newPosition) throws IOException
        {
        }

        default void preTruncate(Path path, FileChannel channel, long size, long targetSize) throws IOException
        {
        }

        default void postTruncate(Path path, FileChannel channel, long size, long targetSize, long newSize) throws IOException
        {
        }

        default void preForce(Path path, FileChannel channel, boolean metaData) throws IOException
        {
        }

        default void postForce(Path path, FileChannel channel, boolean metaData) throws IOException
        {
        }
    }

    private final List<OnOpen> onOpen = new CopyOnWriteArrayList<>();
    private final List<OnTransferTo> onTransferTo = new CopyOnWriteArrayList<>();
    private final List<OnRead> onRead = new CopyOnWriteArrayList<>();
    private final List<OnWrite> onWrite = new CopyOnWriteArrayList<>();
    private final List<OnTransferFrom> onTransferFrom = new CopyOnWriteArrayList<>();
    private final List<OnChannelMeta> onChannelMeta = new CopyOnWriteArrayList<>();
    private final ListenableFileSystemProvider provider;

    public ListenableFileSystem(FileSystem delegate)
    {
        super(delegate);
        this.provider = new ListenableFileSystemProvider(super.provider());
    }

    public void listen(Listener listener)
    {
        if (listener instanceof OnOpen)
            onOpen.add((OnOpen) listener);
        if (listener instanceof OnRead)
            onRead.add((OnRead) listener);
        if (listener instanceof OnTransferTo)
            onTransferTo.add((OnTransferTo) listener);
        if (listener instanceof OnWrite)
            onWrite.add((OnWrite) listener);
        if (listener instanceof OnTransferFrom)
            onTransferFrom.add((OnTransferFrom) listener);
        if (listener instanceof OnChannelMeta)
            onChannelMeta.add((OnChannelMeta) listener);
    }

    public void remove(Listener listener)
    {
        onOpen.remove(listener);
        onRead.remove(listener);
        onTransferTo.remove(listener);
        onWrite.remove(listener);
        onTransferFrom.remove(listener);
        onChannelMeta.remove(listener);
    }

    private interface ListenerAction<T>
    {
        void accept(T value) throws IOException;
    }

    private <T> void notifyListeners(List<T> listeners, ListenerAction<T> fn) throws IOException
    {
        for (T listener : listeners)
            fn.accept(listener);
    }

    @Override
    protected Path wrap(Path p)
    {
        return p instanceof ListenablePath ? p : new ListenablePath(p);
    }

    @Override
    protected Path unwrap(Path p)
    {
        return p instanceof ListenablePath ? ((ListenablePath) p).delegate : p;
    }

    @Override
    public ListenableFileSystemProvider provider()
    {
        return provider;
    }


    private class ListenableFileSystemProvider extends ForwardingFileSystemProvider
    {
        ListenableFileSystemProvider(FileSystemProvider delegate)
        {
            super(delegate);
        }

        @Override
        protected Path wrap(Path a)
        {
            return ListenableFileSystem.this.wrap(a);
        }

        @Override
        protected Path unwrap(Path p)
        {
            return ListenableFileSystem.this.unwrap(p);
        }

        @Override
        public OutputStream newOutputStream(Path path, OpenOption... options) throws IOException
        {
            int len = options.length;
            Set<OpenOption> opts = new HashSet<>(len + 3);
            if (len == 0)
            {
                opts.add(StandardOpenOption.CREATE);
                opts.add(StandardOpenOption.TRUNCATE_EXISTING);
            }
            else
            {
                for (OpenOption opt : options)
                {
                    if (opt == StandardOpenOption.READ)
                        throw new IllegalArgumentException("READ not allowed");
                    opts.add(opt);
                }
            }
            opts.add(StandardOpenOption.WRITE);
            return Channels.newOutputStream(newFileChannel(path, opts));
        }

        @Override
        public InputStream newInputStream(Path path, OpenOption... options) throws IOException
        {
            if (options.length > 0)
            {
                for (OpenOption opt : options)
                {
                    // All OpenOption values except for APPEND and WRITE are allowed
                    if (opt == StandardOpenOption.APPEND ||
                        opt == StandardOpenOption.WRITE)
                        throw new UnsupportedOperationException("'" + opt + "' not allowed");
                }
            }
            Set<OpenOption> opts = new HashSet<>();
            opts.addAll(Arrays.asList(options));
            return Channels.newInputStream(newFileChannel(path, opts));
        }

        @Override
        public SeekableByteChannel newByteChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException
        {
            return newFileChannel(path, options, attrs);
        }

        @Override
        public FileChannel newFileChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException
        {
            notifyListeners(onOpen, l -> l.preOpen(path, options, attrs));
            ListenableFileChannel channel = new ListenableFileChannel(path, delegate().newFileChannel(unwrap(path), options, attrs));
            notifyListeners(onOpen, l -> l.postOpen(path, options, attrs, channel));
            return channel;
        }

        @Override
        public AsynchronousFileChannel newAsynchronousFileChannel(Path path, Set<? extends OpenOption> options, ExecutorService executor, FileAttribute<?>... attrs) throws IOException
        {
            throw new UnsupportedOperationException("TODO");
        }

        // block the APIs that try to switch FileSystem based off schema
        @Override
        public FileSystem newFileSystem(URI uri, Map<String, ?> env) throws IOException
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public FileSystem newFileSystem(Path path, Map<String, ?> env) throws IOException
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getScheme()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public FileSystem getFileSystem(URI uri)
        {
            throw new UnsupportedOperationException();
        }
    }

    private class ListenablePath extends ForwardingPath
    {
        public ListenablePath(Path delegate)
        {
            super(delegate);
        }

        @Override
        protected Path wrap(Path a)
        {
            return ListenableFileSystem.this.wrap(a);
        }

        @Override
        protected Path unwrap(Path p)
        {
            return ListenableFileSystem.this.unwrap(p);
        }

        @Override
        public FileSystem getFileSystem()
        {
            return ListenableFileSystem.this;
        }

        @Override
        public File toFile()
        {
            throw new UnsupportedOperationException();
        }
    }

    private class ListenableFileChannel extends ForwardingFileChannel
    {
        private final AtomicReference<Mapped> current = new AtomicReference<>();
        private final Path path;

        ListenableFileChannel(Path path, FileChannel delegate)
        {
            super(delegate);
            this.path = path;
        }

        @Override
        public int read(ByteBuffer dst) throws IOException
        {
            long position = position();
            notifyListeners(onRead, l -> l.preRead(path, this, position, dst));
            int read = super.read(dst);
            notifyListeners(onRead, l -> l.postRead(path, this, position, dst, read));
            return read;
        }

        @Override
        public int read(ByteBuffer dst, long position) throws IOException
        {
            notifyListeners(onRead, l -> l.preRead(path, this, position, dst));
            int read = super.read(dst, position);
            notifyListeners(onRead, l -> l.postRead(path, this, position, dst, read));
            return read;
        }

        @Override
        public int write(ByteBuffer src) throws IOException
        {
            long position = position();
            notifyListeners(onWrite, l -> l.preWrite(path, this, position, src));
            int write = super.write(src);
            notifyListeners(onWrite, l -> l.postWrite(path, this, position, src, write));
            return write;
        }

        @Override
        public int write(ByteBuffer src, long position) throws IOException
        {
            notifyListeners(onWrite, l -> l.preWrite(path, this, position, src));
            int write = super.write(src, position);
            notifyListeners(onWrite, l -> l.postWrite(path, this, position, src, write));
            return write;
        }

        @Override
        public FileChannel position(long newPosition) throws IOException
        {
            long position = position();
            notifyListeners(onChannelMeta, l -> l.prePosition(path, this, position, newPosition));
            super.position(newPosition);
            notifyListeners(onChannelMeta, l -> l.postPosition(path, this, position, newPosition));
            return this;
        }

        @Override
        public FileChannel truncate(long size) throws IOException
        {
            long currentSize = this.size();
            notifyListeners(onChannelMeta, l -> l.preTruncate(path, this, currentSize, size));
            super.truncate(size);
            long latestSize = this.size();
            notifyListeners(onChannelMeta, l -> l.postTruncate(path, this, currentSize, size, latestSize));
            return this;
        }

        @Override
        public void force(boolean metaData) throws IOException
        {
            notifyListeners(onChannelMeta, l -> l.preForce(path, this, metaData));
            super.force(metaData);
            notifyListeners(onChannelMeta, l -> l.postForce(path, this, metaData));
        }

        @Override
        public long transferTo(long position, long count, WritableByteChannel target) throws IOException
        {
            notifyListeners(onTransferTo, l -> l.preTransferTo(path, this, position, count, target));
            long transfered = super.transferTo(position, count, target);
            notifyListeners(onTransferTo, l -> l.postTransferTo(path, this, position, count, target, transfered));
            return transfered;
        }

        @Override
        public long transferFrom(ReadableByteChannel src, long position, long count) throws IOException
        {
            notifyListeners(onTransferFrom, l -> l.preTransferFrom(path, this, src, position, count));
            long transfered = super.transferFrom(src, position, count);
            notifyListeners(onTransferFrom, l -> l.postTransferFrom(path, this, src, position, count, transfered));
            return transfered;
        }

        @Override
        public MappedByteBuffer map(MapMode mode, long position, long size) throws IOException
        {
            // this behavior isn't 100% correct... if you mix access with FileChanel and ByteBuffer you will get different
            // results than with a real mmap solution... This limitation is due to ByteBuffer being private, so not able
            // to create custom BBs to mimc this access...
            if (current.get() != null)
                throw new UnsupportedOperationException("map called twice");

            int isize = Math.toIntExact(size);
            MappedByteBuffer bb = (MappedByteBuffer) ByteBuffer.allocateDirect(isize);

            Mapped mapped = new Mapped(mode, bb, position, isize);
            if (mode == MapMode.READ_ONLY)
            {
                ByteBufferUtil.readFully(this, mapped.bb, position);
                mapped.bb.flip();

                Runnable forcer = () -> {
                };
                MemoryUtil.setAttachment(bb, forcer);
            }
            else if (mode == MapMode.READ_WRITE)
            {
                if (delegate().size() - position > 0)
                {
                    ByteBufferUtil.readFully(this, mapped.bb, position);
                    mapped.bb.flip();
                }
                Runnable forcer = () -> {
                    ByteBuffer local = bb.duplicate();
                    local.position(0);
                    long pos = position;
                    try
                    {
                        while (local.hasRemaining())
                        {
                            int wrote = write(local, pos);
                            if (wrote == -1)
                                throw new EOFException();
                            pos += wrote;
                        }
                    }
                    catch (IOException e)
                    {
                        throw new UncheckedIOException(e);
                    }
                };
                MemoryUtil.setAttachment(bb, forcer);
            }
            else
            {
                throw new UnsupportedOperationException("Unsupported mode: " + mode);
            }
            if (!current.compareAndSet(null, mapped))
                throw new UnsupportedOperationException("map called twice");
            return mapped.bb;
        }
    }

    private static class Mapped
    {
        final FileChannel.MapMode mode;
        final MappedByteBuffer bb;
        final long position;
        final int size;

        private Mapped(FileChannel.MapMode mode, MappedByteBuffer bb, long position, int size)
        {
            this.mode = mode;
            this.bb = bb;
            this.position = position;
            this.size = size;
        }
    }
}
