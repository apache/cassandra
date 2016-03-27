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

package org.apache.cassandra.utils;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.collect.ImmutableSet;

import static com.google.common.collect.Sets.newHashSet;

/**
 * Walks directory recursively, summing up total contents of files within.
 */
public class DirectorySizeCalculator extends SimpleFileVisitor<Path>
{
    protected final AtomicLong size = new AtomicLong(0);
    protected Set<String> visited = newHashSet(); //count each file only once
    protected Set<String> alive = newHashSet();
    protected final File path;

    public DirectorySizeCalculator(File path)
    {
        super();
        this.path = path;
        rebuildFileList();
    }

    public DirectorySizeCalculator(List<File> files)
    {
        super();
        this.path = null;
        ImmutableSet.Builder<String> builder = ImmutableSet.builder();
        for (File file : files)
            builder.add(file.getName());
        alive = builder.build();
    }

    public boolean isAcceptable(Path file)
    {
        return true;
    }

    public void rebuildFileList()
    {
        assert path != null;
        ImmutableSet.Builder<String> builder = ImmutableSet.builder();
        for (File file : path.listFiles())
            builder.add(file.getName());
        size.set(0);
        alive = builder.build();
    }

    @Override
    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException
    {
        if (isAcceptable(file))
        {
            size.addAndGet(attrs.size());
            visited.add(file.toFile().getName());
        }
        return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException
    {
        return FileVisitResult.CONTINUE;
    }

    public long getAllocatedSize()
    {
        return size.get();
    }
}
