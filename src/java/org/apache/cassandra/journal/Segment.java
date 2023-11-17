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

import java.nio.ByteBuffer;

import accord.utils.Invariants;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.utils.*;
import org.apache.cassandra.utils.concurrent.RefCounted;

abstract class Segment<K, V> implements Closeable, RefCounted<Segment<K, V>>
{
    final File file;
    final Descriptor descriptor;
    final SyncedOffsets syncedOffsets;
    final Metadata metadata;
    final KeySupport<K> keySupport;

    ByteBuffer buffer;

    Segment(Descriptor descriptor, SyncedOffsets syncedOffsets, Metadata metadata, KeySupport<K> keySupport)
    {
        this.file = descriptor.fileFor(Component.DATA);
        this.descriptor = descriptor;
        this.syncedOffsets = syncedOffsets;
        this.metadata = metadata;
        this.keySupport = keySupport;
    }

    abstract Index<K> index();

    abstract boolean isActive();
    boolean isStatic() { return !isActive(); }

    abstract ActiveSegment<K, V> asActive();
    abstract StaticSegment<K, V> asStatic();

    /*
     * Reading entries (by id, by offset, iterate)
     */

    boolean readFirst(K id, RecordConsumer<K> consumer)
    {
        int offset = index().lookUpFirst(id);
        if (offset == -1)
            return false;

        EntrySerializer.EntryHolder<K> into = new EntrySerializer.EntryHolder<>();
        if (read(offset, into))
        {
            Invariants.checkState(id.equals(into.key), "Index for %s read incorrect key: expected %s but read %s", descriptor, id, into.key);
            consumer.accept(id, into.value, into.hosts, descriptor.userVersion);
            return true;
        }
        return false;
    }

    boolean readFirst(K id, EntrySerializer.EntryHolder<K> into)
    {
        int offset = index().lookUpFirst(id);
        if (offset == -1 || !read(offset, into))
            return false;
        Invariants.checkState(id.equals(into.key), "Index for %s read incorrect key: expected %s but read %s", descriptor, id, into.key);
        return true;
    }

    abstract boolean read(int offset, EntrySerializer.EntryHolder<K> into);
}
