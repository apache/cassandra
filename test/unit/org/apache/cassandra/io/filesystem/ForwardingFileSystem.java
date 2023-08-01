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
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.WatchService;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.nio.file.spi.FileSystemProvider;
import java.util.Set;

import com.google.common.collect.Iterables;

public class ForwardingFileSystem extends FileSystem
{
    protected final FileSystem delegate;

    public ForwardingFileSystem(FileSystem delegate)
    {
        this.delegate = delegate;
    }

    protected FileSystem delegate()
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
    public FileSystemProvider provider()
    {
        return delegate().provider();
    }

    @Override
    public void close() throws IOException
    {
        delegate().close();
    }

    @Override
    public boolean isOpen()
    {
        return delegate().isOpen();
    }

    @Override
    public boolean isReadOnly()
    {
        return delegate().isReadOnly();
    }

    @Override
    public String getSeparator()
    {
        return delegate().getSeparator();
    }

    @Override
    public Iterable<Path> getRootDirectories()
    {
        return Iterables.transform(delegate().getRootDirectories(), this::wrap);
    }

    @Override
    public Iterable<FileStore> getFileStores()
    {
        return delegate().getFileStores();
    }

    @Override
    public Set<String> supportedFileAttributeViews()
    {
        return delegate().supportedFileAttributeViews();
    }

    @Override
    public Path getPath(String first, String... more)
    {
        return wrap(delegate().getPath(first, more));
    }

    @Override
    public PathMatcher getPathMatcher(String syntaxAndPattern)
    {
        PathMatcher matcher = delegate().getPathMatcher(syntaxAndPattern);
        return path -> matcher.matches(unwrap(path));
    }

    @Override
    public UserPrincipalLookupService getUserPrincipalLookupService()
    {
        return delegate().getUserPrincipalLookupService();
    }

    @Override
    public WatchService newWatchService() throws IOException
    {
        return delegate().newWatchService();
    }
}
