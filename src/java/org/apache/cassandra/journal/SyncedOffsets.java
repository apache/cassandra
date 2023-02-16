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
import java.io.UncheckedIOException;
import java.nio.file.NoSuchFileException;
import java.util.zip.CRC32;

import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileOutputStreamPlus;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.utils.Closeable;
import org.apache.cassandra.utils.Crc;

import static org.apache.cassandra.utils.FBUtilities.updateChecksumInt;

/**
 * Keeps track of fsynced limits of a data file. Enables us to treat invalid
 * records that are known to have been fsynced to disk differently from those
 * that aren't.
 * <p/>
 * On disk representation is a sequence of 2-int tuples of {synced offset, CRC32(synced offset)}
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
     * @return a disk-backed synced offset tracker for a new {@link ActiveSegment}
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
     * Single-threaded, file-based list of synced offsets.
     */
    final class Active implements SyncedOffsets
    {
        private final Descriptor descriptor;
        private final boolean syncOnMark;

        private final FileOutputStreamPlus output;
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
                output = file.newOutputStream(File.WriteMode.OVERWRITE);
            }
            catch (UncheckedIOException | FSWriteError e)
            {
                // extract original cause and throw as JournalWriteError
                throw new JournalWriteError(descriptor, file, e.getCause());
            }
            catch (NoSuchFileException e)
            {
                throw new AssertionError(); // unreachable
            }
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

            CRC32 crc = Crc.crc32();
            updateChecksumInt(crc, offset);

            try
            {
                output.writeInt(offset);
                output.writeInt((int) crc.getValue());
            }
            catch (IOException e)
            {
                throw new JournalWriteError(descriptor, Component.SYNCED_OFFSETS, e);
            }

            syncedOffset = offset;
            if (syncOnMark) sync();
        }

        private void sync()
        {
            try
            {
                output.sync();
            }
            catch (IOException e)
            {
                throw new JournalWriteError(descriptor, Component.SYNCED_OFFSETS, e);
            }
        }

        @Override
        public void close()
        {
            if (!syncOnMark) sync();

            try
            {
                output.close();
            }
            catch (IOException e)
            {
                throw new JournalWriteError(descriptor, Component.SYNCED_OFFSETS, e);
            }
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
