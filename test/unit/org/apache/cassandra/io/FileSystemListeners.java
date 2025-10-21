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

package org.apache.cassandra.io;

import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import org.apache.cassandra.io.filesystem.ListenableFileSystem;

/**
 * Listeners to aid in debugging and monitoring of disk issues.  The class is normally unused as its target is to help
 * debug issues, for this reason its likely dead code most of the time... it's here when you need it!
 */
@SuppressWarnings("unused")
public class FileSystemListeners
{
    /**
     * Adds a listener to {@link ListenableFileSystem} that will track open/close/delete operations.  If a path delete
     * is detected before the file is closed, this will throw an exception as this could be due to a possible filesystem leak.
     * A filesystem leak can be very dangerous as the allocated disk space keeps growing and growing, but when you look
     * at the file system you won't see all the space as taken; the files were deleted but since the file handles are still
     * open the kernel will keep the data around.
     */
    public static class FileLeak implements ListenableFileSystem.OnPostOpen, ListenableFileSystem.OnPreClose, ListenableFileSystem.OnPreDelete
    {
        @Nullable
        private final IdentityHashMap<FileChannel, Throwable> whereDidChannelsComeFrom;
        private final Map<Path, Set<FileChannel>> pathToChannels = new HashMap<>();

        public FileLeak(boolean trackOpenSource)
        {
            whereDidChannelsComeFrom = trackOpenSource ? new IdentityHashMap<>() : null;
        }

        public FileLeak()
        {
            this(false);
        }

        @Override
        public synchronized void postOpen(Path path,
                                          Set<? extends OpenOption> options,
                                          FileAttribute<?>[] attrs,
                                          FileChannel channel)
        {
            pathToChannels.computeIfAbsent(path, i -> Collections.newSetFromMap(new IdentityHashMap<>())).add(channel);
            if (whereDidChannelsComeFrom != null)
                whereDidChannelsComeFrom.put(channel, new Throwable("here"));
        }

        @Override
        public synchronized void preClose(Path path, FileChannel channel)
        {
            Set<FileChannel> channels = pathToChannels.get(path);
            if (channels == null || !channels.remove(channel))
                return; // listener was added after the open?
            if (channels.isEmpty())
                pathToChannels.remove(path);
        }

        @Override
        public synchronized void preDelete(Path path)
        {
            var cs = pathToChannels.get(path);
            if (cs == null) return;
            AssertionError e = new AssertionError("File leak (delete before close) detected on path " + path + "; " + cs.size() + " open handels detected");
            if (whereDidChannelsComeFrom != null)
            {
                var sources = cs.stream().map(whereDidChannelsComeFrom::get).collect(Collectors.toList());
                sources.forEach(e::addSuppressed);

            }
            throw e;
        }
    }
}
