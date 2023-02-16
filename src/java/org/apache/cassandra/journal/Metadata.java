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
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.zip.CRC32;

import org.agrona.collections.Int2IntHashMap;
import org.agrona.collections.IntHashSet;
import org.apache.cassandra.io.util.*;
import org.apache.cassandra.utils.Crc;

import static org.apache.cassandra.journal.Journal.validateCRC;
import static org.apache.cassandra.utils.FBUtilities.updateChecksumInt;

/**
 * Tracks and serializes the following information:
 * - all the hosts with entries in the data segment and #of records each is tagged in;
 *       used for compaction prioritisation and to act in response to topology changes
 * - total count of records in this segment file
 *       used for compaction prioritisation
 */
final class Metadata
{
    private final Set<Integer> unmodifiableHosts;
    private final Map<Integer, Integer> recordsPerHost;

    private volatile int recordsCount;
    private static final AtomicIntegerFieldUpdater<Metadata> recordsCountUpdater =
        AtomicIntegerFieldUpdater.newUpdater(Metadata.class, "recordsCount");

    static Metadata create()
    {
        return new Metadata(new ConcurrentHashMap<>(), 0);
    }

    private Metadata(Map<Integer, Integer> recordsPerHost, int recordsCount)
    {
        this.recordsPerHost = recordsPerHost;
        this.recordsCount = recordsCount;
        this.unmodifiableHosts = Collections.unmodifiableSet(recordsPerHost.keySet());
    }

    void update(Set<Integer> hosts)
    {
        updateHosts(hosts);
        incrementRecordsCount();
    }

    private void updateHosts(Set<Integer> hosts)
    {
        for (int host : hosts)
            recordsPerHost.compute(host, (k, v) -> null == v ? 1 : v + 1);
    }

    private void incrementRecordsCount()
    {
        recordsCountUpdater.incrementAndGet(this);
    }

    Set<Integer> hosts()
    {
        return unmodifiableHosts;
    }

    int count(int host)
    {
        return recordsPerHost.getOrDefault(host, 0);
    }

    int totalCount()
    {
        return recordsCount;
    }

    void write(DataOutputPlus out) throws IOException
    {
        CRC32 crc = Crc.crc32();

        /* Write records count per host */

        int size = recordsPerHost.size();
        out.writeInt(size);
        updateChecksumInt(crc, size);

        out.writeInt((int) crc.getValue());

        for (Map.Entry<Integer, Integer> entry : recordsPerHost.entrySet())
        {
            int host = entry.getKey();
            int count = entry.getValue();

            out.writeInt(host);
            out.writeInt(count);

            updateChecksumInt(crc, host);
            updateChecksumInt(crc, count);
        }

        /* Write records count */

        out.writeInt(recordsCount);
        updateChecksumInt(crc, recordsCount);

        out.writeInt((int) crc.getValue());
    }

    static Metadata read(DataInputPlus in) throws IOException
    {
        CRC32 crc = Crc.crc32();

        /* Read records count per host */

        int size = in.readInt();
        updateChecksumInt(crc, size);
        validateCRC(crc, in.readInt());

        Int2IntHashMap recordsPerHost = new Int2IntHashMap(Integer.MIN_VALUE);
        for (int i = 0; i < size; i++)
        {
            int host = in.readInt();
            int count = in.readInt();

            updateChecksumInt(crc, host);
            updateChecksumInt(crc, count);

            recordsPerHost.put(host, count);
        }

        /* Read records count */

        int recordsCount = in.readInt();
        updateChecksumInt(crc, recordsCount);

        validateCRC(crc, in.readInt());
        return new Metadata(recordsPerHost, recordsCount);
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

    static <K> Metadata rebuild(Descriptor descriptor, KeySupport<K> keySupport, int fsyncedLimit)
    {
        Int2IntHashMap recordsPerHost = new Int2IntHashMap(Integer.MIN_VALUE);
        int recordsCount = 0;

        try (StaticSegment.SequentialReader<K> reader = StaticSegment.reader(descriptor, keySupport, fsyncedLimit))
        {
            while (reader.advance())
            {
                // iterator is cached and reused by IntHashSet
                IntHashSet.IntIterator hosts = reader.hosts().iterator();
                while (hosts.hasNext())
                    recordsPerHost.merge(hosts.nextValue(), 1, Integer::sum);

                ++recordsCount;
            }
        }

        return new Metadata(recordsPerHost, recordsCount);
    }

    static <K> Metadata rebuildAndPersist(Descriptor descriptor, KeySupport<K> keySupport, int fsyncedLimit)
    {
        Metadata metadata = rebuild(descriptor, keySupport, fsyncedLimit);
        metadata.persist(descriptor);
        return metadata;
    }
}
