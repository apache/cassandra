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

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.Iterator;

import com.google.common.collect.Iterators;

public class ForwardingPath implements Path
{
    protected final Path delegate;

    public ForwardingPath(Path delegate)
    {
        this.delegate = delegate;
    }

    protected Path delegate()
    {
        return delegate;
    }

    protected Path wrap(Path a)
    {
        return a;
    }

    protected Path unwrap(Path p)
    {
        return p;
    }

    @Override
    public FileSystem getFileSystem()
    {
        return delegate().getFileSystem();
    }

    @Override
    public boolean isAbsolute()
    {
        return delegate().isAbsolute();
    }

    @Override
    public Path getRoot()
    {
        return wrap(delegate().getRoot());
    }

    @Override
    public Path getFileName()
    {
        return wrap(delegate().getFileName());
    }

    @Override
    public Path getParent()
    {
        Path parent = delegate().getParent();
        if (parent == null)
            return null;
        return wrap(parent);
    }

    @Override
    public int getNameCount()
    {
        return delegate().getNameCount();
    }

    @Override
    public Path getName(int index)
    {
        return wrap(delegate().getName(index));
    }

    @Override
    public Path subpath(int beginIndex, int endIndex)
    {
        return wrap(delegate().subpath(beginIndex, endIndex));
    }

    @Override
    public boolean startsWith(Path other)
    {
        return delegate().startsWith(unwrap(other));
    }

    @Override
    public boolean startsWith(String other)
    {
        return delegate().startsWith(other);
    }

    @Override
    public boolean endsWith(Path other)
    {
        return delegate().endsWith(unwrap(other));
    }

    @Override
    public boolean endsWith(String other)
    {
        return delegate().endsWith(other);
    }

    @Override
    public Path normalize()
    {
        return wrap(delegate().normalize());
    }

    @Override
    public Path resolve(Path other)
    {
        return wrap(delegate().resolve(unwrap(other)));
    }

    @Override
    public Path resolve(String other)
    {
        return wrap(delegate().resolve(other));
    }

    @Override
    public Path resolveSibling(Path other)
    {
        return wrap(delegate().resolveSibling(unwrap(other)));
    }

    @Override
    public Path resolveSibling(String other)
    {
        return wrap(delegate().resolveSibling(other));
    }

    @Override
    public Path relativize(Path other)
    {
        return wrap(delegate().relativize(unwrap(other)));
    }

    @Override
    public URI toUri()
    {
        return delegate().toUri();
    }

    @Override
    public Path toAbsolutePath()
    {
        return wrap(delegate().toAbsolutePath());
    }

    @Override
    public Path toRealPath(LinkOption... options) throws IOException
    {
        return wrap(delegate().toRealPath(options));
    }

    @Override
    public File toFile()
    {
        return delegate().toFile();
    }

    @Override
    public WatchKey register(WatchService watcher, WatchEvent.Kind<?>[] events, WatchEvent.Modifier... modifiers) throws IOException
    {
        return delegate().register(watcher, events, modifiers);
    }

    @Override
    public WatchKey register(WatchService watcher, WatchEvent.Kind<?>... events) throws IOException
    {
        return delegate().register(watcher, events);
    }

    @Override
    public Iterator<Path> iterator()
    {
        return Iterators.transform(delegate().iterator(), this::wrap);
    }

    @Override
    public int compareTo(Path other)
    {
        return delegate().compareTo(unwrap(other));
    }

    @Override
    public boolean equals(Object obj)
    {
        return delegate().equals(obj instanceof Path ? unwrap((Path) obj) : obj);
    }

    @Override
    public int hashCode()
    {
        return delegate().hashCode();
    }

    @Override
    public String toString()
    {
        return delegate().toString();
    }
}
