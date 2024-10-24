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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.agrona.collections.IntHashSet;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.Closeable;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.concurrent.Ref;

/**
 * An immutable data segment that is no longer written to.
 * <p>
 * Can be compacted with input from {@code PersistedInvalidations} into a new smaller segment,
 * with invalidated entries removed.
 */
public final class StaticSegment<K, V> extends Segment<K, V>
{
    final FileChannel channel;
    final int fsyncLimit;

    private final Ref<Segment<K, V>> selfRef;

    private final OnDiskIndex<K> index;

    private StaticSegment(Descriptor descriptor,
                          FileChannel channel,
                          MappedByteBuffer buffer,
                          OnDiskIndex<K> index,
                          Metadata metadata,
                          KeySupport<K> keySupport)
    {
        super(descriptor, metadata, keySupport);
        this.index = index;

        this.channel = channel;
        this.fsyncLimit = metadata.fsyncLimit();
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

        Metadata metadata = Component.METADATA.existsFor(descriptor)
                          ? Metadata.load(descriptor)
                          : Metadata.rebuildAndPersist(descriptor, keySupport);

        OnDiskIndex<K> index = Component.INDEX.existsFor(descriptor)
                             ? OnDiskIndex.open(descriptor, keySupport)
                             : OnDiskIndex.rebuildAndPersist(descriptor, keySupport, metadata.fsyncLimit());

        try
        {
            return internalOpen(descriptor, index, metadata, keySupport);
        }
        catch (IOException e)
        {
            throw new JournalReadError(descriptor, Component.DATA, e);
        }
    }

    private static <K, V> StaticSegment<K, V> internalOpen(
        Descriptor descriptor, OnDiskIndex<K> index, Metadata metadata, KeySupport<K> keySupport)
    throws IOException
    {
        File file = descriptor.fileFor(Component.DATA);
        FileChannel channel = FileChannel.open(file.toPath(), StandardOpenOption.READ);
        MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
        return new StaticSegment<>(descriptor, channel, buffer, index, metadata, keySupport);
    }

    public void close(Journal<K, V> journal)
    {
        release(journal);
    }

    /**
     * Waits until this segment is unreferenced, closes it, and deltes all files associated with it.
     */
    void discard(Journal<K, V> journal)
    {
        ((Tidier)selfRef.tidier()).discard = true;
        close(journal);
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

    @Override
    public String toString()
    {
        return "StaticSegment{" + descriptor + '}';
    }

    @Override
    public Ref<Segment<K, V>> selfRef()
    {
        return selfRef;
    }

    private static final class Tidier<K> extends Segment.Tidier implements Tidy
    {
        private final Descriptor descriptor;
        private final FileChannel channel;
        private final ByteBuffer buffer;
        private final Index<K> index;
        boolean discard;

        Tidier(Descriptor descriptor, FileChannel channel, ByteBuffer buffer, Index<K> index)
        {
            this.descriptor = descriptor;
            this.channel = channel;
            this.buffer = buffer;
            this.index = index;
        }

        @Override
        void onUnreferenced()
        {
            FileUtils.clean(buffer);
            FileUtils.closeQuietly(channel);
            index.close();
            if (discard)
            {
                Throwable fail = null;
                for (Component component : Component.VALUES)
                {
                    try { descriptor.fileFor(component).deleteIfExists(); }
                    catch (Throwable t) { fail = Throwables.merge(fail, t); }
                }
                Throwables.maybeFail(fail);
            }
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
    boolean isFlushed(long position)
    {
        return true;
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
    boolean read(int offset, int size, EntrySerializer.EntryHolder<K> into)
    {
        ByteBuffer duplicate = buffer.duplicate().position(offset).limit(offset + size);
        try
        {
            return 0 <= EntrySerializer.tryRead(into, keySupport, duplicate, fsyncLimit, descriptor.userVersion);
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
        try (SequentialReader<K> reader = sequentialReader(descriptor, keySupport, fsyncLimit))
        {
            while (reader.advance())
            {
                consumer.accept(descriptor.timestamp, reader.offset(), reader.key(), reader.record(), reader.hosts(), descriptor.userVersion);
            }
        }
    }

    /*
     * Sequential and in-key order reading (replay and components rebuild)
     */

    static abstract class Reader<K> implements Closeable
    {
        enum State { RESET, ADVANCED, EOF }

        public final Descriptor descriptor;
        protected final KeySupport<K> keySupport;

        protected final File file;
        protected final FileChannel channel;
        protected final MappedByteBuffer buffer;

        protected final EntrySerializer.EntryHolder<K> holder = new EntrySerializer.EntryHolder<>();
        protected int offset = -1;
        protected State state = State.RESET;

        Reader(Descriptor descriptor, KeySupport<K> keySupport)
        {
            this.descriptor = descriptor;
            this.keySupport = keySupport;

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
        }

        @Override
        public void close()
        {
            FileUtils.closeQuietly(channel);
            FileUtils.clean(buffer);
        }

        public abstract boolean advance();

        public int offset()
        {
            ensureHasAdvanced();
            return offset;
        }

        public K key()
        {
            ensureHasAdvanced();
            return holder.key;
        }

        public IntHashSet hosts()
        {
            ensureHasAdvanced();
            return holder.hosts;
        }

        public ByteBuffer record()
        {
            ensureHasAdvanced();
            return holder.value;
        }

        protected void ensureHasAdvanced()
        {
            if (state != State.ADVANCED)
                throw new IllegalStateException("Must call advance() before accessing entry content");
        }

        protected boolean eof()
        {
            state = State.EOF;
            return false;
        }
    }

    static <K> SequentialReader<K> sequentialReader(Descriptor descriptor, KeySupport<K> keySupport, int fsyncedLimit)
    {
        return new SequentialReader<>(descriptor, keySupport, fsyncedLimit);
    }

    /**
     * A sequential data segment reader to use for journal replay and rebuilding
     * missing auxilirary components (index and metadata).
     * </p>
     * Unexpected EOF and CRC mismatches in synced portions of segments are treated
     * strictly, throwing {@link JournalReadError}. Errors encountered in unsynced portions
     * of segments are treated as segment EOF.
     */
    static final class SequentialReader<K> extends Reader<K>
    {
        private final int fsyncedLimit; // exclusive

        SequentialReader(Descriptor descriptor, KeySupport<K> keySupport, int fsyncedLimit)
        {
            super(descriptor, keySupport);
            this.fsyncedLimit = fsyncedLimit;
        }

        @Override
        public boolean advance()
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
                int length = EntrySerializer.tryRead(holder, keySupport, buffer.duplicate(), fsyncedLimit, descriptor.userVersion);
                if (length < 0)
                    return eof();
                buffer.position(offset + length);
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
    }

    public StaticSegment.KeyOrderReader<K> keyOrderReader()
    {
        return new StaticSegment.KeyOrderReader<>(descriptor, keySupport, index.reader());
    }

    public static final class KeyOrderReader<K> extends Reader<K> implements Comparable<KeyOrderReader<K>>
    {
        private final OnDiskIndex<K>.IndexReader indexReader;

        KeyOrderReader(Descriptor descriptor, KeySupport<K> keySupport, OnDiskIndex<K>.IndexReader indexReader)
        {
            super(descriptor, keySupport);
            this.indexReader = indexReader;
        }

        @Override
        public boolean advance()
        {
            if (!indexReader.advance())
                return eof();

            offset = indexReader.offset();

            buffer.limit(offset + indexReader.recordSize())
                  .position(offset);
            try
            {
                EntrySerializer.read(holder, keySupport, buffer, descriptor.userVersion);
            }
            catch (IOException e)
            {
                throw new JournalReadError(descriptor, file, e);
            }

            state = State.ADVANCED;
            return true;
        }

        @Override
        public int compareTo(KeyOrderReader<K> that)
        {
            this.ensureHasAdvanced();
            that.ensureHasAdvanced();

            int cmp = keySupport.compare(this.key(), that.key());
            if (cmp != 0)
                return cmp;
            cmp = Long.compare(that.descriptor.timestamp, this.descriptor.timestamp);
            if (cmp != 0)
                return cmp;
            return Integer.compare(that.offset, this.offset);
        }
    }
}