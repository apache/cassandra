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
package org.apache.cassandra.journal;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.NoSuchFileException;
import java.nio.file.StandardOpenOption;
import java.util.*;

import org.agrona.collections.IntHashSet;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.Closeable;
import org.apache.cassandra.utils.concurrent.Ref;

/**
 * An immutable data segment that is no longer written to.
 * <p>
 * Can be compacted with input from {@code PersistedInvalidations} into a new smaller segment,
 * with invalidated entries removed.
 */
final class StaticSegment<K, V> extends Segment<K, V>
{
    final FileChannel channel;

    private final Ref<Segment<K, V>> selfRef;

    private final OnDiskIndex<K> index;

    private StaticSegment(Descriptor descriptor,
                          FileChannel channel,
                          MappedByteBuffer buffer,
                          SyncedOffsets syncedOffsets,
                          OnDiskIndex<K> index,
                          Metadata metadata,
                          KeySupport<K> keySupport)
    {
        super(descriptor, syncedOffsets, metadata, keySupport);
        this.index = index;

        this.channel = channel;
        this.buffer = buffer;

        selfRef = new Ref<>(this, new Tidier<>(descriptor, channel, buffer, index));
    }

    /**
     * Loads all segments matching the supplied desctiptors
     *
     * @param descriptors descriptors of the segments to load
     * @return list of the loaded segments
     */
    static <K, V> List<Segment<K, V>> open(Collection<Descriptor> descriptors, KeySupport<K> keySupport)
    {
        List<Segment<K, V>> segments = new ArrayList<>(descriptors.size());
        for (Descriptor descriptor : descriptors)
            segments.add(open(descriptor, keySupport));
        return segments;
    }

    /**
     * Load the segment corresponding to the provided desrciptor
     *
     * @param descriptor descriptor of the segment to load
     * @return the loaded segment
     */
    @SuppressWarnings({ "resource", "RedundantSuppression" })
    static <K, V> StaticSegment<K, V> open(Descriptor descriptor, KeySupport<K> keySupport)
    {
        if (!Component.DATA.existsFor(descriptor))
            throw new IllegalArgumentException("Data file for segment " + descriptor + " doesn't exist");

        SyncedOffsets syncedOffsets = Component.SYNCED_OFFSETS.existsFor(descriptor)
                                    ? SyncedOffsets.load(descriptor)
                                    : SyncedOffsets.absent();

        Metadata metadata = Component.METADATA.existsFor(descriptor)
                          ? Metadata.load(descriptor)
                          : Metadata.rebuildAndPersist(descriptor, keySupport, syncedOffsets.syncedOffset());

        OnDiskIndex<K> index = Component.INDEX.existsFor(descriptor)
                             ? OnDiskIndex.open(descriptor, keySupport)
                             : OnDiskIndex.rebuildAndPersist(descriptor, keySupport, syncedOffsets.syncedOffset());

        try
        {
            return internalOpen(descriptor, syncedOffsets, index, metadata, keySupport);
        }
        catch (IOException e)
        {
            throw new JournalReadError(descriptor, Component.DATA, e);
        }
    }

    @SuppressWarnings("resource")
    private static <K, V> StaticSegment<K, V> internalOpen(
        Descriptor descriptor, SyncedOffsets syncedOffsets, OnDiskIndex<K> index, Metadata metadata, KeySupport<K> keySupport)
    throws IOException
    {
        File file = descriptor.fileFor(Component.DATA);
        FileChannel channel = FileChannel.open(file.toPath(), StandardOpenOption.READ);
        MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
        return new StaticSegment<>(descriptor, channel, buffer, syncedOffsets, index, metadata, keySupport);
    }

    @Override
    public void close()
    {
        selfRef.release();
    }

    @Override
    public Ref<Segment<K, V>> tryRef()
    {
        return selfRef.tryRef();
    }

    @Override
    public Ref<Segment<K, V>> ref()
    {
        return selfRef.ref();
    }

    private static final class Tidier<K> implements Tidy
    {
        private final Descriptor descriptor;
        private final FileChannel channel;
        private final ByteBuffer buffer;
        private final Index<K> index;

        Tidier(Descriptor descriptor, FileChannel channel, ByteBuffer buffer, Index<K> index)
        {
            this.descriptor = descriptor;
            this.channel = channel;
            this.buffer = buffer;
            this.index = index;
        }

        @Override
        public void tidy()
        {
            FileUtils.clean(buffer);
            FileUtils.closeQuietly(channel);
            index.close();
        }

        @Override
        public String name()
        {
            return descriptor.toString();
        }
    }

    @Override
    OnDiskIndex<K> index()
    {
        return index;
    }

    @Override
    boolean isActive()
    {
        return false;
    }

    @Override
    ActiveSegment<K, V> asActive()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    StaticSegment<K, V> asStatic()
    {
        return this;
    }

    /**
     * Read the entry and specified offset into the entry holder.
     * Expects the record to have been written at this offset, but potentially not flushed and lost.
     */
    @Override
    boolean read(int offset, EntrySerializer.EntryHolder<K> into)
    {
        ByteBuffer duplicate = buffer.duplicate().position(offset);
        try (DataInputBuffer in = new DataInputBuffer(duplicate, false))
        {
            return EntrySerializer.tryRead(into, keySupport, duplicate, in, syncedOffsets.syncedOffset(), descriptor.userVersion);
        }
        catch (IOException e)
        {
            throw new JournalReadError(descriptor, file, e);
        }
    }

    /**
     * Iterate over and invoke the supplied callback on every record.
     */
    void forEachRecord(RecordConsumer<K> consumer)
    {
        try (SequentialReader<K> reader = reader(descriptor, keySupport, syncedOffsets.syncedOffset()))
        {
            while (reader.advance())
            {
                consumer.accept(reader.id(), reader.record(), reader.hosts(), descriptor.userVersion);
            }
        }
    }

    /*
     * Sequential reading (replay and components rebuild)
     */

    static <K> SequentialReader<K> reader(Descriptor descriptor, KeySupport<K> keySupport, int fsyncedLimit)
    {
        return SequentialReader.open(descriptor, keySupport, fsyncedLimit);
    }

    /**
     * A sequential data segment reader to use for journal replay and rebuilding
     * missing auxilirary components (index and metadata).
     * </p>
     * Unexpected EOF and CRC mismatches in synced portions of segments are treated
     * strictly, throwing {@link JournalReadError}. Errors encountered in unsynced portions
     * of segments are treated as segment EOF.
     */
    static final class SequentialReader<K> implements Closeable
    {
        private final Descriptor descriptor;
        private final KeySupport<K> keySupport;
        private final int fsyncedLimit; // exclusive

        private final File file;
        private final FileChannel channel;
        private final MappedByteBuffer buffer;
        private final DataInputBuffer in;

        private int offset = -1;
        private final EntrySerializer.EntryHolder<K> holder = new EntrySerializer.EntryHolder<>();
        private State state = State.RESET;

        static <K> SequentialReader<K> open(Descriptor descriptor, KeySupport<K> keySupport, int fsyncedLimit)
        {
            return new SequentialReader<>(descriptor, keySupport, fsyncedLimit);
        }

        SequentialReader(Descriptor descriptor, KeySupport<K> keySupport, int fsyncedLimit)
        {
            this.descriptor = descriptor;
            this.keySupport = keySupport;
            this.fsyncedLimit = fsyncedLimit;

            file = descriptor.fileFor(Component.DATA);
            try
            {
                channel = file.newReadChannel();
                buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
            }
            catch (NoSuchFileException e)
            {
                throw new IllegalArgumentException("Data file for segment " + descriptor + " doesn't exist");
            }
            catch (IOException e)
            {
                throw new JournalReadError(descriptor, file, e);
            }
            in = new DataInputBuffer(buffer, false);
        }

        @Override
        public void close()
        {
            FileUtils.closeQuietly(channel);
            FileUtils.clean(buffer);
        }

        int offset()
        {
            ensureHasAdvanced();
            return offset;
        }

        K id()
        {
            ensureHasAdvanced();
            return holder.key;
        }

        IntHashSet hosts()
        {
            ensureHasAdvanced();
            return holder.hosts;
        }

        ByteBuffer record()
        {
            ensureHasAdvanced();
            return holder.value;
        }

        private void ensureHasAdvanced()
        {
            if (state != State.ADVANCED)
                throw new IllegalStateException("Must call advance() before accessing entry content");
        }

        boolean advance()
        {
            if (state == State.EOF)
                return false;

            reset();
            return buffer.hasRemaining() ? doAdvance() : eof();
        }

        private boolean doAdvance()
        {
            offset = buffer.position();
            try
            {
                if (!EntrySerializer.tryRead(holder, keySupport, buffer, in, fsyncedLimit, descriptor.userVersion))
                    return eof();
            }
            catch (IOException e)
            {
                throw new JournalReadError(descriptor, file, e);
            }

            state = State.ADVANCED;
            return true;
        }

        private void reset()
        {
            offset = -1;
            holder.clear();
            state = State.RESET;
        }

        private boolean eof()
        {
            state = State.EOF;
            return false;
        }

        enum State { RESET, ADVANCED, EOF }
    }
}