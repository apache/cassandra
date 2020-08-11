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

package org.apache.cassandra.db.streaming;

import com.google.common.collect.ImmutableSet;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.util.FileUtils;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Mutable SSTable components and their hardlinks to avoid concurrent sstable component modification
 * during entire-sstable-streaming.
 */
public class ComponentContext implements AutoCloseable
{
    private static final Set<Component> MUTABLE_COMPONENTS = ImmutableSet.of(Component.STATS, Component.SUMMARY);

    private final Map<Component, File> hardLinks;
    private final ComponentManifest manifest;

    private ComponentContext(Map<Component, File> hardLinks, ComponentManifest manifest)
    {
        this.hardLinks = hardLinks;
        this.manifest = manifest;
    }

    public static ComponentContext create(Descriptor descriptor)
    {
        LinkedHashMap<Component, File> hardLinks = new LinkedHashMap<>(1);

        for (Component component : MUTABLE_COMPONENTS)
        {
            File file = new File(descriptor.filenameFor(component));
            if (!file.exists())
                continue;

            File hardlink = new File(descriptor.tmpFilenameForStreaming(component));
            FileUtils.createHardLink(file, hardlink);
            hardLinks.put(component, hardlink);
        }

        return new ComponentContext(hardLinks, ComponentManifest.create(descriptor));
    }

    public ComponentManifest manifest()
    {
        return manifest;
    }

    /**
     * @return file channel to be streamed, either original component or hardlinked component.
     */
    @SuppressWarnings("resource") // file channel will be closed by Caller
    public FileChannel channel(Descriptor descriptor, Component component, long size) throws IOException
    {
        String toTransfer = hardLinks.containsKey(component) ? hardLinks.get(component).getPath() : descriptor.filenameFor(component);
        FileChannel channel = new RandomAccessFile(toTransfer, "r").getChannel();

        assert size == channel.size() : String.format("Entire sstable streaming expects %s file size to be %s but got %s.",
                                                      component, size, channel.size());
        return channel;
    }

    @Override
    public void close()
    {
        hardLinks.values().forEach(File::delete);
        hardLinks.clear();
    }
}
