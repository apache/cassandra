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

import java.io.EOFException;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.zip.CRC32;

import org.apache.cassandra.io.util.*;
import org.apache.cassandra.utils.Crc;

import static org.apache.cassandra.journal.Journal.validateCRC;
import static org.apache.cassandra.utils.FBUtilities.updateChecksumInt;

/**
 * Tracks and serializes the following information:
 * - total count of records in this segment file
 *       used for compaction prioritisation
 */
final class Metadata
{
    private int fsyncLimit;

    private volatile int recordsCount;
    private static final AtomicIntegerFieldUpdater<Metadata> recordsCountUpdater =
        AtomicIntegerFieldUpdater.newUpdater(Metadata.class, "recordsCount");

    static Metadata empty()
    {
        return new Metadata(0, 0);
    }

    private Metadata(int recordsCount, int fsyncLimit)
    {
        this.recordsCount = recordsCount;
        this.fsyncLimit = fsyncLimit;
    }

    void update()
    {
        incrementRecordsCount();
    }

    void fsyncLimit(int fsyncLimit)
    {
        this.fsyncLimit = fsyncLimit;
    }

    int fsyncLimit()
    {
        return fsyncLimit;
    }

    private void incrementRecordsCount()
    {
        recordsCountUpdater.incrementAndGet(this);
    }

    int totalCount()
    {
        return recordsCount;
    }

    void write(DataOutputPlus out) throws IOException
    {
        CRC32 crc = Crc.crc32();
        out.writeInt(recordsCount);
        out.writeInt(fsyncLimit);
        updateChecksumInt(crc, recordsCount);
        updateChecksumInt(crc, fsyncLimit);
        out.writeInt((int) crc.getValue());
    }

    static Metadata read(DataInputPlus in) throws IOException
    {
        CRC32 crc = Crc.crc32();
        int recordsCount = in.readInt();
        int fsyncLimit = in.readInt();
        updateChecksumInt(crc, recordsCount);
        updateChecksumInt(crc, fsyncLimit);
        validateCRC(crc, in.readInt());
        return new Metadata(recordsCount, fsyncLimit);
    }

    void persist(Descriptor descriptor)
    {
        File tmpFile = descriptor.tmpFileFor(Component.METADATA);
        try (FileOutputStreamPlus out = new FileOutputStreamPlus(tmpFile))
        {
            write(out);

            out.flush();
            out.sync();
        }
        catch (IOException e)
        {
            throw new JournalWriteError(descriptor, tmpFile, e);
        }
        tmpFile.move(descriptor.fileFor(Component.METADATA));
    }

    static Metadata load(Descriptor descriptor)
    {
        File file = descriptor.fileFor(Component.METADATA);
        try (FileInputStreamPlus in = new FileInputStreamPlus(file))
        {
            return read(in);
        }
        catch (IOException e)
        {
            throw new JournalReadError(descriptor, file, e);
        }
    }

    static <K> Metadata rebuild(Descriptor descriptor, KeySupport<K> keySupport)
    {
        int recordsCount = 0;
        int fsyncLimit = 0;
        try (StaticSegment.SequentialReader<K> reader = StaticSegment.sequentialReader(descriptor, keySupport, Integer.MAX_VALUE))
        {
            while (reader.advance())
                ++recordsCount;
            fsyncLimit = reader.offset;
        }
        catch (JournalReadError e)
        {
            // we expect EOF when rebuilding
            if (!(e.getCause() instanceof EOFException))
                throw e;
        }

        return new Metadata(recordsCount, fsyncLimit);
    }

    static <K> Metadata rebuildAndPersist(Descriptor descriptor, KeySupport<K> keySupport)
    {
        Metadata metadata = rebuild(descriptor, keySupport);
        metadata.persist(descriptor);
        return metadata;
    }

    @Override
    public String toString()
    {
        return "Metadata{" +
               "fsyncLimit=" + fsyncLimit +
               ", recordsCount=" + recordsCount +
               '}';
    }
}
