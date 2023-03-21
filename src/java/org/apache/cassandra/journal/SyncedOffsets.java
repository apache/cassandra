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
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.zip.CRC32;

import com.google.common.primitives.Ints;

import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.utils.Closeable;
import org.apache.cassandra.utils.Crc;
import org.apache.cassandra.utils.SyncUtil;

import static org.apache.cassandra.utils.FBUtilities.updateChecksumInt;

/**
 * Keeps track of fsynced limits of a data file. Enables us to treat invalid
 * records that are known to have been fsynced to disk differently from those
 * that aren't.
 * <p/>
 * On disk representation is a sequence of 2-int tuples of:
 *      (synced offset, CRC32(tuple position in synced offsets file, synced offset))
 */
interface SyncedOffsets extends Closeable
{
    /**
     * @return furthest known synced offset
     */
    int syncedOffset();

    /**
     * Record an offset as synced to disk.
     *
     * @param offset the offset into datafile, up to which contents have been fsynced (exclusive)
     */
    void mark(int offset);

    @Override
    default void close()
    {
    }

    /**
     * @return a mutable MMAP-backed synced offset tracker for a new {@link ActiveSegment}
     */
    static Active active(Descriptor descriptor, boolean syncOnMark)
    {
        return new Active(descriptor, syncOnMark);
    }

    /**
     * Load an existing log of synced offsets from disk into an immutable instance.
     */
    static Static load(Descriptor descriptor)
    {
        return Static.load(descriptor);
    }

    /**
     * @return a placeholder instance in case this component is missing
     */
    static Absent absent()
    {
        return Absent.INSTANCE;
    }

    /**
     * Single-threaded, MMAP-backed list of synced offsets.
     */
    final class Active implements SyncedOffsets
    {
        private static final int INITIAL_CAPACITY = 4 << 10;

        private final Descriptor descriptor;
        private final boolean syncOnMark;

        private final FileChannel channel;
        private MappedByteBuffer buffer;

        private volatile int syncedOffset;

        private Active(Descriptor descriptor, boolean syncOnMark)
        {
            this.descriptor = descriptor;
            this.syncOnMark = syncOnMark;

            File file = descriptor.fileFor(Component.SYNCED_OFFSETS);
            if (file.exists())
                throw new IllegalArgumentException("Synced offsets file " + file + " already exists");

            try
            {
                channel = FileChannel.open(file.toPath(), StandardOpenOption.WRITE, StandardOpenOption.READ, StandardOpenOption.CREATE);
            }
            catch (IOException e)
            {
                throw new JournalWriteError(descriptor, file, e);
            }
            buffer = map(INITIAL_CAPACITY);
        }

        @Override
        public int syncedOffset()
        {
            return syncedOffset;
        }

        @Override
        public void mark(int offset)
        {
            if (offset < syncedOffset)
                throw new IllegalArgumentException("offset " + offset + " is smaller than previous mark " + offset);

            if (!buffer.hasRemaining())
                doubleCapacity();

            CRC32 crc = Crc.crc32();
            updateChecksumInt(crc, buffer.position());
            updateChecksumInt(crc, offset);
            buffer.putInt(offset);
            buffer.putInt((int) crc.getValue());

            syncedOffset = offset;
            if (syncOnMark) sync();
        }

        private void doubleCapacity()
        {
            int position = buffer.position();
            int capacity = buffer.capacity();

            if (!syncOnMark) sync();
            FileUtils.clean(buffer);

            buffer = map(capacity * 2);
            buffer.position(position);
        }

        private MappedByteBuffer map(int capacity)
        {
            try
            {
                return channel.map(FileChannel.MapMode.READ_WRITE, 0, capacity);
            }
            catch (IOException e)
            {
                throw new JournalWriteError(descriptor, Component.SYNCED_OFFSETS, e);
            }
        }

        private void sync()
        {
            try
            {
                SyncUtil.force(buffer);
            }
            catch (Exception e) // MappedByteBuffer.force() does not declare IOException but can actually throw it
            {
                throw new JournalWriteError(descriptor, Component.SYNCED_OFFSETS, e);
            }
        }

        @Override
        public void close()
        {
            if (!syncOnMark) sync();
            FileUtils.clean(buffer);
            FileUtils.closeQuietly(channel);
        }
    }

    final class Static implements SyncedOffsets
    {
        private final int syncedOffset;

        static Static load(Descriptor descriptor)
        {
            File file = descriptor.fileFor(Component.SYNCED_OFFSETS);
            if (!file.exists())
                throw new IllegalArgumentException("Synced offsets file " + file + " doesn't exist");

            int syncedOffset = 0;
            try (RandomAccessReader reader = RandomAccessReader.open(file))
            {
                CRC32 crc = Crc.crc32();
                while (reader.bytesRemaining() >= 8)
                {
                    updateChecksumInt(crc, Ints.checkedCast(reader.getFilePointer()));
                    int offset = reader.readInt();
                    updateChecksumInt(crc, offset);
                    int readCrc = reader.readInt();
                    if (readCrc != (int) crc.getValue())
                        break;
                    syncedOffset = offset;
                    Crc.initialize(crc);
                }
            }
            catch (Throwable t)
            {
                throw new JournalReadError(descriptor, file, t);
            }

            return new Static(syncedOffset);
        }

        Static(int offset)
        {
            this.syncedOffset = offset;
        }

        @Override
        public int syncedOffset()
        {
            return syncedOffset;
        }

        @Override
        public void mark(int offset)
        {
            throw new UnsupportedOperationException();
        }
    }

    final class Absent implements SyncedOffsets
    {
        static final Absent INSTANCE = new Absent();

        @Override
        public int syncedOffset()
        {
            return 0;
        }

        @Override
        public void mark(int offset)
        {
            throw new UnsupportedOperationException();
        }
    }
}
