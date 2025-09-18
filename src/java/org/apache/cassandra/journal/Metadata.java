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

import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileInputStreamPlus;
import org.apache.cassandra.io.util.FileOutputStreamPlus;
import org.apache.cassandra.utils.Crc;

import static org.apache.cassandra.journal.Journal.validateCRC;
import static org.apache.cassandra.utils.FBUtilities.updateChecksumInt;

/**
 * Tracks and serializes the following information:
 * - total count of records in this segment file
 *       used for compaction prioritisation
 */
public final class Metadata
{
    private int fsyncLimit;
    // Indicates whether a segment needs to be replayed or no.
    private volatile boolean needsReplay;
    private volatile int recordsCount;
    private static final AtomicIntegerFieldUpdater<Metadata> recordsCountUpdater =
        AtomicIntegerFieldUpdater.newUpdater(Metadata.class, "recordsCount");

    static Metadata empty()
    {
        return new Metadata(0, 0,  true);
    }

    private Metadata(int recordsCount, int fsyncLimit, boolean needsReplay)
    {
        this.recordsCount = recordsCount;
        this.fsyncLimit = fsyncLimit;
        this.needsReplay = needsReplay;
    }

    void update()
    {
        incrementRecordsCount();
    }

    void fsyncLimit(int fsyncLimit)
    {
        this.fsyncLimit = fsyncLimit;
    }

    public void clearNeedsReplay()
    {
        this.needsReplay = false;
    }

    int fsyncLimit()
    {
        return fsyncLimit;
    }

    public boolean needsReplay()
    {
        return needsReplay;
    }

    private void incrementRecordsCount()
    {
        recordsCountUpdater.incrementAndGet(this);
    }

    public int totalCount()
    {
        return recordsCount;
    }

    void write(DataOutputPlus out) throws IOException
    {
        CRC32 crc = Crc.crc32();
        out.writeInt(recordsCount);
        out.writeInt(fsyncLimit);
        out.writeBoolean(needsReplay);
        updateChecksumInt(crc, recordsCount);
        updateChecksumInt(crc, fsyncLimit);
        updateChecksumInt(crc, needsReplay ? 1 : 0);
        out.writeInt((int) crc.getValue());
    }

    static Metadata read(DataInputPlus in) throws IOException
    {
        CRC32 crc = Crc.crc32();
        int recordsCount = in.readInt();
        int fsyncLimit = in.readInt();
        boolean needsReplay = in.readBoolean();
        updateChecksumInt(crc, recordsCount);
        updateChecksumInt(crc, fsyncLimit);
        updateChecksumInt(crc, needsReplay ? 1 : 0);
        validateCRC(crc, in.readInt());
        return new Metadata(recordsCount, fsyncLimit, needsReplay);
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

        return new Metadata(recordsCount, fsyncLimit, true);
    }

    static <K> Metadata rebuildAndPersist(Descriptor descriptor, KeySupport<K> keySupport)
    {
        Metadata metadata = rebuild(descriptor, keySupport);
        metadata.persist(descriptor);
        return metadata;
    }

    @Override
    public String toString() {
        return "Metadata{" +
                "fsyncLimit=" + fsyncLimit +
                ", needsReplay=" + needsReplay +
                ", recordsCount=" + recordsCount +
                '}';
    }
}
