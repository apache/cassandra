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

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;

/**
 * Where each component of an entire-sstable stream is read from, and which of those files the sender owns. For a
 * whole sstable the components are the sstable's own files, hardlinked for the mutable ones so a concurrent stats
 * update or index summary redistribution cannot change a size after the manifest named it. For a partial stream
 * ({@link #slice}) every component but Data.db is instead a synthesised file describing byte ranges of the parent,
 * and Data.db is those ranges, sent in order from the parent's own file with the gaps skipped. Files the sender
 * created are deleted on close, the parent's own are not.
 * <p>
 * Either way the sender's own files are {@link Descriptor#tmpFileForStreaming} names, so that the crash this cannot
 * clean up after leaves something {@code ColumnFamilyStore.scrubDataDirectories} removes and
 * {@link Descriptor#getTemporaryFiles()} lists, rather than something that looks like a live sstable. The hardlinks
 * are created that way below; a slice's components are renamed to it once written -- see
 * {@code ZeroCopySSTableSlice.toStreamingTemporaries} for why they cannot be written under it directly.
 */
public class ComponentContext implements AutoCloseable
{
    private static final Logger logger = LoggerFactory.getLogger(ComponentContext.class);

    /** One contiguous stretch of a component's source file. */
    public static final class ByteRange
    {
        public final long position;
        public final long length;

        public ByteRange(long position, long length)
        {
            this.position = position;
            this.length = length;
        }

        @Override
        public String toString()
        {
            return "[" + position + ", " + (position + length) + ')';
        }
    }

    /** Files this context created and must remove; anything not named here is read from the descriptor. */
    private final Map<Component, File> sources;

    /** The stretches of the parent's Data.db a partial stream is made of, in order; null for a whole sstable. */
    private final List<ByteRange> dataRanges;

    private final ComponentManifest manifest;

    private ComponentContext(Map<Component, File> sources, List<ByteRange> dataRanges, ComponentManifest manifest)
    {
        this.sources = sources;
        this.dataRanges = dataRanges;
        this.manifest = manifest;
    }

    public static ComponentContext create(SSTable sstable)
    {
        Descriptor descriptor = sstable.descriptor;
        Map<Component, File> hardLinks = new HashMap<>(1);

        for (Component component : descriptor.getFormat().mutableComponents())
        {
            File file = descriptor.fileFor(component);
            if (!file.exists())
                continue;

            File hardlink = descriptor.tmpFileForStreaming(component);
            FileUtils.createHardLink(file, hardlink);
            hardLinks.put(component, hardlink);
        }

        return new ComponentContext(hardLinks, null, ComponentManifest.create(sstable));
    }

    /**
     * @param synthesised files the caller created for every component but Data.db, deleted on close
     * @param dataRanges  the stretches of the parent's Data.db that make up the slice's, in order
     * @param manifest    the sizes of {@code synthesised} plus the total of {@code dataRanges} for Data.db
     */
    public static ComponentContext slice(Map<Component, File> synthesised, List<ByteRange> dataRanges,
                                         ComponentManifest manifest)
    {
        return new ComponentContext(new HashMap<>(synthesised), ImmutableList.copyOf(dataRanges), manifest);
    }

    public ComponentManifest manifest()
    {
        return manifest;
    }

    /**
     * The stretches of {@link #channel} to send, in order: a whole component's whole file, or one per parent byte
     * range for a partial stream's Data.db. Also where the manifest is checked against disk -- for a whole sstable,
     * the assertion that catches a component mutated after it was named in the manifest.
     */
    public List<ByteRange> ranges(Descriptor descriptor, Component component, long size) throws IOException
    {
        long onDisk = fileFor(descriptor, component).length();

        if (dataRanges == null || !component.equals(SSTableFormat.Components.DATA))
        {
            assert size == onDisk : String.format("Entire sstable streaming expects %s file size to be %s but got %s.",
                                                  component, size, onDisk);
            return ImmutableList.of(new ByteRange(0, size));
        }

        long total = 0;
        for (ByteRange range : dataRanges)
        {
            assert range.position >= 0 && range.length > 0 && range.position + range.length <= onDisk
                : String.format("Partial sstable streaming expects %s range %s to be inside a file of %s bytes.",
                                component, range, onDisk);
            total += range.length;
        }
        assert total == size : String.format("Partial sstable streaming expects %s ranges to total %s bytes, not %s.",
                                             component, size, total);
        return dataRanges;
    }

    /**
     * @return channel for the component's own file, its hardlink, or the file synthesised for it; one per range,
     * since writing it hands over ownership.
     */
    public FileChannel channel(Descriptor descriptor, Component component) throws IOException
    {
        @SuppressWarnings("resource") // file channel will be closed by Caller
        FileChannel channel = fileFor(descriptor, component).newReadChannel();
        return channel;
    }

    private File fileFor(Descriptor descriptor, Component component)
    {
        return sources.getOrDefault(component, descriptor.fileFor(component));
    }

    @Override
    public void close()
    {
        Throwable accumulate = null;
        for (File file : sources.values())
            accumulate = FileUtils.deleteWithConfirm(file, accumulate);

        sources.clear();

        if (accumulate != null)
            logger.warn("Failed to remove streaming temporary files", accumulate);
    }
}
