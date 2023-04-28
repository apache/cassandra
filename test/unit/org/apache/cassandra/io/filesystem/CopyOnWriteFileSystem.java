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

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.AccessMode;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryStream;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NotDirectoryException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.spi.FileSystemProvider;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;

public class CopyOnWriteFileSystem extends ForwardingFileSystem
{
    private final FileSystem readonly;
    private final CopyOnWriteFileSystemProvider provider;

    public CopyOnWriteFileSystem(FileSystem delegate, FileSystem readonly)
    {
        super(delegate);
        this.readonly = readonly;
        this.provider = new CopyOnWriteFileSystemProvider(delegate.provider(), readonly.provider());
    }

    public static CopyOnWriteFileSystem create(FileSystem delegate)
    {
        return new CopyOnWriteFileSystem(delegate, FileSystems.getDefault());
    }

    @Override
    protected Path wrap(Path p)
    {
        return p instanceof COWPath ? p : new COWPath(p);
    }

    @Override
    protected Path unwrap(Path p)
    {
        if (p == null)
            return null;
        return p instanceof COWPath ? ((COWPath) p).delegate() : p;
    }

    @Override
    public FileSystemProvider provider()
    {
        return provider;
    }

    private class CopyOnWriteFileSystemProvider extends ForwardingFileSystemProvider
    {
        private final FileSystemProvider readonlyProvider;

        public CopyOnWriteFileSystemProvider(FileSystemProvider delegate, FileSystemProvider readonlyProvider)
        {
            super(delegate);
            this.readonlyProvider = readonlyProvider;
        }

        @Override
        public DirectoryStream<Path> newDirectoryStream(Path dir, DirectoryStream.Filter<? super Path> filter) throws IOException
        {
            Path ro = readOnly(dir.toString());
            Path mut = mutable(dir.toString());
            if (!Files.exists(ro))
                return super.newDirectoryStream(dir, filter);
            if (!Files.exists(mut))
                throw new NotDirectoryException(dir.toString());
            // ro and mut exist... need to union
            DirectoryStream<Path> ros = provider(ro).newDirectoryStream(ro, filter);
            DirectoryStream<Path> muts = provider(mut).newDirectoryStream(mut, filter);
            return new DirectoryStream<Path>()
            {
                @Override
                public Iterator<Path> iterator()
                {
                    Set<Path> union = new HashSet<>();
                    for (Path p : ros)
                        union.add(wrap(p));
                    for (Path p : muts)
                        union.add(wrap(p));
                    return union.iterator();
                }

                @Override
                public void close() throws IOException
                {
                    try
                    {
                        muts.close();
                    }
                    finally
                    {
                        ros.close();
                    }
                }
            };
        }

        @Override
        public SeekableByteChannel newByteChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException
        {
            maybeCopyParent(path);
            if (isWrite(options))
                return super.newByteChannel(path, options, attrs);
            // read
            Path underline = find(path);
            return provider(underline).newByteChannel(underline, options, attrs);
        }

        @Override
        public void createDirectory(Path dir, FileAttribute<?>... attrs) throws IOException
        {
            maybeCopyParent(dir);
            super.createDirectory(dir, attrs);
        }

        @Override
        public void delete(Path path) throws IOException
        {
            //TODO have to track when ro exists and we delete from mut
            super.delete(path);
        }

        @Override
        public boolean deleteIfExists(Path path) throws IOException
        {
            //TODO have to track when ro exists and we delete from mut
            return super.deleteIfExists(path);
        }

        @Override
        public void copy(Path source, Path target, CopyOption... options) throws IOException
        {
            maybeCopyParent(target);
            source = find(source);
            target = find(target);
            //TODO does this work cross fs?
            provider(source).copy(source, target, options);
        }

        @Override
        public void move(Path source, Path target, CopyOption... options) throws IOException
        {
            throw new UnsupportedOperationException("TODO");
        }

        @Override
        public boolean isSameFile(Path path, Path path2) throws IOException
        {
            path = find(path);
            path2 = find(path2);
            if (path.getFileSystem() != path2.getFileSystem())
                return false;
            return provider(path).isSameFile(path, path2);
        }

        @Override
        public boolean isHidden(Path path) throws IOException
        {
            path = find(path);
            return provider(path).isHidden(path);
        }

        @Override
        public FileStore getFileStore(Path path) throws IOException
        {
            path = find(path);
            return provider(path).getFileStore(path);
        }

        @Override
        public void checkAccess(Path path, AccessMode... modes) throws IOException
        {
            path = find(path);
            provider(path).checkAccess(path, modes);
        }

        @Override
        public <V extends FileAttributeView> V getFileAttributeView(Path path, Class<V> type, LinkOption... options)
        {
            path = find(path);
            return provider(path).getFileAttributeView(path, type, options);
        }

        @Override
        public <A extends BasicFileAttributes> A readAttributes(Path path, Class<A> type, LinkOption... options) throws IOException
        {
            path = find(path);
            return provider(path).readAttributes(path, type, options);
        }

        @Override
        public Map<String, Object> readAttributes(Path path, String attributes, LinkOption... options) throws IOException
        {
            path = find(path);
            return provider(path).readAttributes(path, attributes, options);
        }

        @Override
        public InputStream newInputStream(Path path, OpenOption... options) throws IOException
        {
            path = find(path);
            return provider(path).newInputStream(path, options);
        }

        @Override
        public FileChannel newFileChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException
        {
            //TODO support read-write
            maybeCopyParent(path);
            if (isWrite(options))
                return super.newFileChannel(path, options, attrs);
            path = find(path);
            FileSystemProvider p = provider(path);
            return p.newFileChannel(path, options, attrs);
        }

        @Override
        public AsynchronousFileChannel newAsynchronousFileChannel(Path path, Set<? extends OpenOption> options, ExecutorService executor, FileAttribute<?>... attrs) throws IOException
        {
            //TODO support read-write
            maybeCopyParent(path);
            if (isWrite(options))
                return super.newAsynchronousFileChannel(path, options, executor, attrs);
            path = find(path);
            FileSystemProvider p = provider(path);
            return p.newAsynchronousFileChannel(path, options, executor, attrs);
        }

        @Override
        public void createSymbolicLink(Path link, Path target, FileAttribute<?>... attrs) throws IOException
        {
            maybeCopyParent(link);
            target = find(target);
            delegate().createSymbolicLink(unwrap(link), target);
        }

        @Override
        public void createLink(Path link, Path existing) throws IOException
        {
            maybeCopyParent(link);
            existing = find(existing);
            delegate().createLink(unwrap(link), existing);
        }

        @Override
        public Path readSymbolicLink(Path link) throws IOException
        {
            Path underline = find(link);
            return wrap(provider(underline).readSymbolicLink(underline));
        }

        private Path find(Path wrapped)
        {
            if (wrapped == null)
                return null;
            String str = wrapped.toString();
            Path mutable = mutable(str);
            if (Files.exists(mutable))
                return mutable;
            Path ro = readOnly(str);
            return Files.exists(ro) ? ro : mutable;
        }

        private boolean isWrite(Set<? extends OpenOption> options)
        {
            for (OpenOption opt : options)
            {
                if (opt == StandardOpenOption.APPEND ||
                    opt == StandardOpenOption.WRITE)
                    return true;
            }
            return false;
        }

        private Path maybeCopyParent(Path p) throws IOException
        {
            Path real = find(p.getParent());
            if (real == null)
                return p;
            if (isReadOnly(real))
                provider(real).createDirectory(real);
            return p;
        }
    }

    private static FileSystemProvider provider(Path path)
    {
        return path.getFileSystem().provider();
    }

    private Path readOnly(String str, String... more)
    {
        return readonly.getPath(str, more);
    }

    private boolean isReadOnly(Path p)
    {
        return readonly.equals(p.getFileSystem());
    }

    private Path mutable(String str, String... more)
    {
        return CopyOnWriteFileSystem.this.delegate().getPath(str, more);
    }

    private boolean isMutable(Path p)
    {
        return delegate().equals(p.getFileSystem());
    }

    private class COWPath extends ForwardingPath
    {
        COWPath(Path delegate)
        {
            super(delegate);
        }

        @Override
        public FileSystem getFileSystem()
        {
            return CopyOnWriteFileSystem.this;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (!(obj instanceof Path))
                return false;
            Path other = (Path) obj;
            return toAbsolutePath().toString().equals(other.toAbsolutePath().toString());
        }

        @Override
        public int hashCode()
        {
            return toAbsolutePath().toString().hashCode();
        }
    }

}
