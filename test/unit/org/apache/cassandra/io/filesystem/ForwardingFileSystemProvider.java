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
import java.io.OutputStream;
import java.net.URI;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.AccessMode;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryStream;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.LinkOption;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.spi.FileSystemProvider;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import com.google.common.collect.Iterators;

public class ForwardingFileSystemProvider extends FileSystemProvider
{
    protected final FileSystemProvider delegate;

    public ForwardingFileSystemProvider(FileSystemProvider delegate)
    {
        this.delegate = delegate;
    }

    protected FileSystemProvider delegate()
    {
        return delegate;
    }

    protected Path wrap(Path p)
    {
        return p;
    }

    protected Path unwrap(Path p)
    {
        return p;
    }

    @Override
    public String getScheme()
    {
        return delegate().getScheme();
    }

    @Override
    public FileSystem newFileSystem(URI uri, Map<String, ?> env) throws IOException
    {
        return delegate().newFileSystem(uri, env);
    }

    @Override
    public FileSystem getFileSystem(URI uri)
    {
        return delegate().getFileSystem(uri);
    }

    @Override
    public Path getPath(URI uri)
    {
        return wrap(delegate().getPath(uri));
    }

    @Override
    public SeekableByteChannel newByteChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException
    {
        return delegate().newByteChannel(unwrap(path), options, attrs);
    }

    @Override
    public DirectoryStream<Path> newDirectoryStream(Path dir, DirectoryStream.Filter<? super Path> filter) throws IOException
    {
        DirectoryStream<Path> stream = delegate().newDirectoryStream(unwrap(dir), filter);
        return new DirectoryStream<Path>()
        {
            @Override
            public Iterator<Path> iterator()
            {
                return Iterators.transform(stream.iterator(), ForwardingFileSystemProvider.this::wrap);
            }

            @Override
            public void close() throws IOException
            {
                stream.close();
            }
        };
    }

    @Override
    public void createDirectory(Path dir, FileAttribute<?>... attrs) throws IOException
    {
        delegate().createDirectory(unwrap(dir), attrs);
    }

    @Override
    public void delete(Path path) throws IOException
    {
        delegate().delete(unwrap(path));
    }

    @Override
    public void copy(Path source, Path target, CopyOption... options) throws IOException
    {
        delegate().copy(unwrap(source), unwrap(target), options);
    }

    @Override
    public void move(Path source, Path target, CopyOption... options) throws IOException
    {
        delegate().move(unwrap(source), unwrap(target), options);
    }

    @Override
    public boolean isSameFile(Path path, Path path2) throws IOException
    {
        return delegate().isSameFile(unwrap(path), unwrap(path2));
    }

    @Override
    public boolean isHidden(Path path) throws IOException
    {
        return delegate().isHidden(unwrap(path));
    }

    @Override
    public FileStore getFileStore(Path path) throws IOException
    {
        return delegate().getFileStore(unwrap(path));
    }

    @Override
    public void checkAccess(Path path, AccessMode... modes) throws IOException
    {
        delegate().checkAccess(unwrap(path), modes);
    }

    @Override
    public <V extends FileAttributeView> V getFileAttributeView(Path path, Class<V> type, LinkOption... options)
    {
        return delegate().getFileAttributeView(unwrap(path), type, options);
    }

    @Override
    public <A extends BasicFileAttributes> A readAttributes(Path path, Class<A> type, LinkOption... options) throws IOException
    {
        return delegate().readAttributes(unwrap(path), type, options);
    }

    @Override
    public Map<String, Object> readAttributes(Path path, String attributes, LinkOption... options) throws IOException
    {
        return delegate().readAttributes(unwrap(path), attributes, options);
    }

    @Override
    public void setAttribute(Path path, String attribute, Object value, LinkOption... options) throws IOException
    {
        delegate().setAttribute(unwrap(path), attribute, value, options);
    }

    @Override
    public FileSystem newFileSystem(Path path, Map<String, ?> env) throws IOException
    {
        return delegate().newFileSystem(unwrap(path), env);
    }

    @Override
    public InputStream newInputStream(Path path, OpenOption... options) throws IOException
    {
        return delegate().newInputStream(unwrap(path), options);
    }

    @Override
    public OutputStream newOutputStream(Path path, OpenOption... options) throws IOException
    {
        return delegate().newOutputStream(unwrap(path), options);
    }

    @Override
    public FileChannel newFileChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException
    {
        return delegate().newFileChannel(unwrap(path), options, attrs);
    }

    @Override
    public AsynchronousFileChannel newAsynchronousFileChannel(Path path, Set<? extends OpenOption> options, ExecutorService executor, FileAttribute<?>... attrs) throws IOException
    {
        return delegate().newAsynchronousFileChannel(unwrap(path), options, executor, attrs);
    }

    @Override
    public void createSymbolicLink(Path link, Path target, FileAttribute<?>... attrs) throws IOException
    {
        delegate().createSymbolicLink(unwrap(link), target, attrs);
    }

    @Override
    public void createLink(Path link, Path existing) throws IOException
    {
        delegate().createLink(unwrap(link), unwrap(existing));
    }

    @Override
    public boolean deleteIfExists(Path path) throws IOException
    {
        return delegate().deleteIfExists(unwrap(path));
    }

    @Override
    public Path readSymbolicLink(Path link) throws IOException
    {
        return wrap(delegate().readSymbolicLink(unwrap(link)));
    }
}
