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
import java.nio.file.FileSystems;
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
    @FunctionalInterface
    public interface PathFilter
    {
        boolean accept(Path entry) throws IOException;
    }

    public interface Listener
    {
    }

    public interface OnPreOpen
    {
        void preOpen(Path path, Set<? extends OpenOption> options, FileAttribute<?>[] attrs) throws IOException;
    }

    public interface OnPostOpen
    {
        void postOpen(Path path, Set<? extends OpenOption> options, FileAttribute<?>[] attrs, FileChannel channel) throws IOException;
    }

    public interface OnOpen extends OnPreOpen, OnPostOpen, Listener
    {
        static OnOpen from(OnPreOpen callback)
        {
            return new OnOpen()
            {
                @Override
                public void postOpen(Path path, Set<? extends OpenOption> options, FileAttribute<?>[] attrs, FileChannel channel) throws IOException
                {

                }

                @Override
                public void preOpen(Path path, Set<? extends OpenOption> options, FileAttribute<?>[] attrs) throws IOException
                {
                    callback.preOpen(path, options, attrs);
                }
            };
        }

        static OnOpen from(OnPostOpen callback)
        {
            return new OnOpen()
            {
                @Override
                public void postOpen(Path path, Set<? extends OpenOption> options, FileAttribute<?>[] attrs, FileChannel channel) throws IOException
                {
                    callback.postOpen(path, options, attrs, channel);
                }

                @Override
                public void preOpen(Path path, Set<? extends OpenOption> options, FileAttribute<?>[] attrs) throws IOException
                {

                }
            };
        }
    }

    public interface OnPreRead
    {
        void preRead(Path path, FileChannel channel, long position, ByteBuffer dst) throws IOException;
    }

    public interface OnPostRead
    {
        void postRead(Path path, FileChannel channel, long position, ByteBuffer dst, int read) throws IOException;
    }

    public interface OnRead extends OnPreRead, OnPostRead, Listener
    {
        static OnRead from(OnPreRead callback)
        {
            return new OnRead()
            {
                @Override
                public void postRead(Path path, FileChannel channel, long position, ByteBuffer dst, int read) throws IOException
                {

                }

                @Override
                public void preRead(Path path, FileChannel channel, long position, ByteBuffer dst) throws IOException
                {
                    callback.preRead(path, channel, position, dst);
                }
            };
        }

        static OnRead from(OnPostRead callback)
        {
            return new OnRead()
            {
                @Override
                public void postRead(Path path, FileChannel channel, long position, ByteBuffer dst, int read) throws IOException
                {
                    callback.postRead(path, channel, position, dst, read);
                }

                @Override
                public void preRead(Path path, FileChannel channel, long position, ByteBuffer dst) throws IOException
                {

                }
            };
        }
    }

    public interface OnPreTransferTo
    {
        void preTransferTo(Path path, FileChannel channel, long position, long count, WritableByteChannel target) throws IOException;
    }

    public interface OnPostTransferTo
    {
        void postTransferTo(Path path, FileChannel channel, long position, long count, WritableByteChannel target, long transfered) throws IOException;
    }

    public interface OnTransferTo extends OnPreTransferTo, OnPostTransferTo, Listener
    {
        static OnTransferTo from(OnPreTransferTo callback)
        {
            return new OnTransferTo()
            {
                @Override
                public void postTransferTo(Path path, FileChannel channel, long position, long count, WritableByteChannel target, long transfered) throws IOException
                {

                }

                @Override
                public void preTransferTo(Path path, FileChannel channel, long position, long count, WritableByteChannel target) throws IOException
                {
                    callback.preTransferTo(path, channel, position, count, target);
                }
            };
        }

        static OnTransferTo from(OnPostTransferTo callback)
        {
            return new OnTransferTo()
            {
                @Override
                public void postTransferTo(Path path, FileChannel channel, long position, long count, WritableByteChannel target, long transfered) throws IOException
                {
                    callback.postTransferTo(path, channel, position, count, target, transfered);
                }

                @Override
                public void preTransferTo(Path path, FileChannel channel, long position, long count, WritableByteChannel target) throws IOException
                {

                }
            };
        }
    }

    public interface OnPreTransferFrom
    {
        void preTransferFrom(Path path, FileChannel channel, ReadableByteChannel src, long position, long count) throws IOException;
    }

    public interface OnPostTransferFrom
    {
        void postTransferFrom(Path path, FileChannel channel, ReadableByteChannel src, long position, long count, long transfered) throws IOException;
    }

    public interface OnTransferFrom extends OnPreTransferFrom, OnPostTransferFrom, Listener
    {
        static OnTransferFrom from(OnPreTransferFrom callback)
        {
            return new OnTransferFrom()
            {
                @Override
                public void postTransferFrom(Path path, FileChannel channel, ReadableByteChannel src, long position, long count, long transfered) throws IOException
                {

                }

                @Override
                public void preTransferFrom(Path path, FileChannel channel, ReadableByteChannel src, long position, long count) throws IOException
                {
                    callback.preTransferFrom(path, channel, src, position, count);
                }
            };
        }

        static OnTransferFrom from(OnPostTransferFrom callback)
        {
            return new OnTransferFrom()
            {
                @Override
                public void postTransferFrom(Path path, FileChannel channel, ReadableByteChannel src, long position, long count, long transfered) throws IOException
                {
                    callback.postTransferFrom(path, channel, src, position, count, transfered);
                }

                @Override
                public void preTransferFrom(Path path, FileChannel channel, ReadableByteChannel src, long position, long count) throws IOException
                {

                }
            };
        }
    }

    public interface OnPreWrite
    {
        void preWrite(Path path, FileChannel channel, long position, ByteBuffer src) throws IOException;
    }

    public interface OnPostWrite
    {
        void postWrite(Path path, FileChannel channel, long position, ByteBuffer src, int wrote) throws IOException;
    }

    public interface OnWrite extends OnPreWrite, OnPostWrite, Listener
    {
        static OnWrite from(OnPreWrite callback)
        {
            return new OnWrite()
            {
                @Override
                public void postWrite(Path path, FileChannel channel, long position, ByteBuffer src, int wrote) throws IOException
                {

                }

                @Override
                public void preWrite(Path path, FileChannel channel, long position, ByteBuffer src) throws IOException
                {
                    callback.preWrite(path, channel, position, src);
                }
            };
        }

        static OnWrite from(OnPostWrite callback)
        {
            return new OnWrite()
            {
                @Override
                public void postWrite(Path path, FileChannel channel, long position, ByteBuffer src, int wrote) throws IOException
                {
                    callback.postWrite(path, channel, position, src, wrote);
                }

                @Override
                public void preWrite(Path path, FileChannel channel, long position, ByteBuffer src) throws IOException
                {

                }
            };
        }
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

    public interface Unsubscribable extends AutoCloseable
    {
        @Override
        void close();
    }

    private final List<OnOpen> onOpen = new CopyOnWriteArrayList<>();
    private final List<OnTransferTo> onTransferTo = new CopyOnWriteArrayList<>();
    private final List<OnRead> onRead = new CopyOnWriteArrayList<>();
    private final List<OnWrite> onWrite = new CopyOnWriteArrayList<>();
    private final List<OnTransferFrom> onTransferFrom = new CopyOnWriteArrayList<>();
    private final List<OnChannelMeta> onChannelMeta = new CopyOnWriteArrayList<>();
    private final List<List<? extends Listener>> lists = Arrays.asList(onOpen, onRead, onTransferTo, onWrite, onTransferFrom, onChannelMeta);
    private final ListenableFileSystemProvider provider;

    public ListenableFileSystem(FileSystem delegate)
    {
        super(delegate);
        this.provider = new ListenableFileSystemProvider(super.provider());
    }

    public Unsubscribable listen(Listener listener)
    {
        boolean match = false;
        if (listener instanceof OnOpen)
        {
            onOpen.add((OnOpen) listener);
            match = true;
        }
        if (listener instanceof OnRead)
        {
            onRead.add((OnRead) listener);
            match = true;
        }
        if (listener instanceof OnTransferTo)
        {
            onTransferTo.add((OnTransferTo) listener);
            match = true;
        }
        if (listener instanceof OnWrite)
        {
            onWrite.add((OnWrite) listener);
            match = true;
        }
        if (listener instanceof OnTransferFrom)
        {
            onTransferFrom.add((OnTransferFrom) listener);
            match = true;
        }
        if (listener instanceof OnChannelMeta)
        {
            onChannelMeta.add((OnChannelMeta) listener);
            match = true;
        }
        if (!match)
            throw new IllegalArgumentException("Unable to find a listenable type for " + listener.getClass());
        return () -> remove(listener);
    }

    public Unsubscribable onPreOpen(OnPreOpen callback)
    {
        return listen(OnOpen.from(callback));
    }

    public Unsubscribable onPreOpen(PathFilter filter, OnPreOpen callback)
    {
        return onPreOpen((path, options, attrs) -> {
            if (filter.accept(path))
                callback.preOpen(path, options, attrs);
        });
    }

    public Unsubscribable onPostOpen(OnPostOpen callback)
    {
        return listen(OnOpen.from(callback));
    }

    public Unsubscribable onPostOpen(PathFilter filter, OnPostOpen callback)
    {
        return onPostOpen((path, options, attrs, channel) -> {
            if (filter.accept(path))
                callback.postOpen(path, options, attrs, channel);
        });
    }

    public Unsubscribable onPreRead(OnPreRead callback)
    {
        return listen(OnRead.from(callback));
    }

    public Unsubscribable onPreRead(PathFilter filter, OnPreRead callback)
    {
        return onPreRead((path, channel, position, dst) -> {
            if (filter.accept(path))
                callback.preRead(path, channel, position, dst);
        });
    }

    public Unsubscribable onPostRead(OnPostRead callback)
    {
        return listen(OnRead.from(callback));
    }

    public Unsubscribable onPostRead(PathFilter filter, OnPostRead callback)
    {
        return onPostRead((path, channel, position, dst, read) -> {
            if (filter.accept(path))
                callback.postRead(path, channel, position, dst, read);
        });
    }

    public Unsubscribable onPreTransferTo(OnPreTransferTo callback)
    {
        return listen(OnTransferTo.from(callback));
    }

    public Unsubscribable onPreTransferTo(PathFilter filter, OnPreTransferTo callback)
    {
        return onPreTransferTo((path, channel, position, count, target) -> {
            if (filter.accept(path))
                callback.preTransferTo(path, channel, position, count, target);
        });
    }

    public Unsubscribable onPostTransferTo(OnPostTransferTo callback)
    {
        return listen(OnTransferTo.from(callback));
    }

    public Unsubscribable onPostTransferTo(PathFilter filter, OnPostTransferTo callback)
    {
        return onPostTransferTo((path, channel, position, count, target, transfered) -> {
            if (filter.accept(path))
                callback.postTransferTo(path, channel, position, count, target, transfered);
        });
    }

    public Unsubscribable onPreTransferFrom(OnPreTransferFrom callback)
    {
        return listen(OnTransferFrom.from(callback));
    }

    public Unsubscribable onPreTransferFrom(PathFilter filter, OnPreTransferFrom callback)
    {
        return onPreTransferFrom((path, channel, src, position, count) -> {
            if (filter.accept(path))
                callback.preTransferFrom(path, channel, src, position, count);
        });
    }

    public Unsubscribable onPostTransferFrom(OnPostTransferFrom callback)
    {
        return listen(OnTransferFrom.from(callback));
    }

    public Unsubscribable onPostTransferFrom(PathFilter filter, OnPostTransferFrom callback)
    {
        return onPostTransferFrom((path, channel, src, position, count, transfered) -> {
            if (filter.accept(path))
                callback.postTransferFrom(path, channel, src, position, count, transfered);
        });
    }

    public Unsubscribable onPreWrite(OnPreWrite callback)
    {
        return listen(OnWrite.from(callback));
    }

    public Unsubscribable onPreWrite(PathFilter filter, OnPreWrite callback)
    {
        return onPreWrite((path, channel, position, src) -> {
            if (filter.accept(path))
                callback.preWrite(path, channel, position, src);
        });
    }

    public Unsubscribable onPostWrite(OnPostWrite callback)
    {
        return listen(OnWrite.from(callback));
    }

    public Unsubscribable onPostWrite(PathFilter filter, OnPostWrite callback)
    {
        return onPostWrite((path, channel, position, src, wrote) -> {
            if (filter.accept(path))
                callback.postWrite(path, channel, position, src, wrote);
        });
    }

    public void remove(Listener listener)
    {
        lists.forEach(l -> l.remove(listener));
    }

    public void clearListeners()
    {
        lists.forEach(List::clear);
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
            for (OpenOption opt : options)
            {
                // All OpenOption values except for APPEND and WRITE are allowed
                if (opt == StandardOpenOption.APPEND ||
                    opt == StandardOpenOption.WRITE)
                    throw new UnsupportedOperationException("'" + opt + "' not allowed");
            }
            Set<OpenOption> opts = new HashSet<>(Arrays.asList(options));
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
            if (delegate().getFileSystem() == FileSystems.getDefault())
                return delegate().toFile();
            throw new UnsupportedOperationException();
        }
    }

    private class ListenableFileChannel extends ForwardingFileChannel
    {
        private final AtomicReference<Mapped> mutable = new AtomicReference<>();
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
            if (mode == MapMode.READ_WRITE && mutable.get() != null)
                throw new UnsupportedOperationException("map called twice with mode READ_WRITE; first was " + mutable.get() + ", now " + new Mapped(mode, null, position, Math.toIntExact(size)));

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
                // with real files the FD gets copied so the close of the channel does not block the BB mutation
                // from flushing...  its possible to support this use case, but kept things simplier for now by
                // failing if the backing channel was closed.
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
                if (!mutable.compareAndSet(null, mapped))
                    throw new UnsupportedOperationException("map called twice");
            }
            else
            {
                throw new UnsupportedOperationException("Unsupported mode: " + mode);
            }
            return mapped.bb;
        }

        @Override
        protected void implCloseChannel() throws IOException
        {
            super.implCloseChannel();
            mutable.set(null);
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

        @Override
        public String toString()
        {
            return "Mapped{" +
                   "mode=" + mode +
                   ", position=" + position +
                   ", size=" + size +
                   '}';
        }
    }
}
