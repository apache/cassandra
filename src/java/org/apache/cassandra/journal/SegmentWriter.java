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

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Set;

import com.google.common.primitives.Ints;

import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileOutputStreamPlus;
import org.apache.cassandra.io.util.TrackedDataOutputPlus;

final class SegmentWriter<K> implements Closeable
{
    private final Descriptor descriptor;
    private final KeySupport<K> keySupport;

    private final InMemoryIndex<K> index;
    private final Metadata metadata;

    private final File file;
    private FileOutputStreamPlus untrackedOut;
    private TrackedDataOutputPlus trackedOut;

    private SegmentWriter(Descriptor descriptor, KeySupport<K> keySupport)
    {
        this.descriptor = descriptor;
        this.keySupport = keySupport;

        index = InMemoryIndex.create(keySupport);
        metadata = Metadata.create();

        file = descriptor.fileFor(Component.DATA);
        try
        {
            untrackedOut = new FileOutputStreamPlus(file);
        }
        catch (IOException e)
        {
            throw new JournalWriteError(descriptor, file, e);
        }
        trackedOut = TrackedDataOutputPlus.wrap(untrackedOut);
    }

    static <K> SegmentWriter<K> create(Descriptor descriptor, KeySupport<K> keySupport)
    {
        return new SegmentWriter<>(descriptor, keySupport);
    }

    int write(K key, ByteBuffer record, Set<Integer> hosts)
    {
        int position = position();
        try
        {
            index.update(key, position);
            metadata.update(hosts);

            EntrySerializer.write(key, record, hosts, keySupport, trackedOut, descriptor.userVersion);
        }
        catch (IOException e)
        {
            throw new JournalWriteError(descriptor, file, e);
        }
        return position;
    }

    int position()
    {
        return Ints.checkedCast(trackedOut.position());
    }

    @Override
    public void close()
    {
        try
        {
            untrackedOut.flush();
            untrackedOut.sync();
            untrackedOut.close();
        }
        catch (IOException e)
        {
            throw new JournalWriteError(descriptor, file, e);
        }

        try (SyncedOffsets syncedOffsets = SyncedOffsets.active(descriptor, true))
        {
            syncedOffsets.mark(position());
        }

        index.persist(descriptor);
        metadata.persist(descriptor);

        untrackedOut = null;
        trackedOut = null;
    }
}
