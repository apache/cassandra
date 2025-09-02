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

package org.apache.cassandra.replication;

import java.util.Iterator;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.collect.Iterators;

import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.memtable.SkipListMemtable;
import org.apache.cassandra.db.memtable.TrieMemtable;
import org.jctools.maps.NonBlockingHashMapLong;
import org.jctools.queues.MpscUnboundedArrayQueue;

/**
 * This is different from {@link Log2OffsetsMap} because it's focused on supporting fast, frequent updates from multiple
 * threads at {@link Memtable#put}, and infrequent reads at {@link Memtable#getFlushSet}.
 * <p>
 * Concurrent lock-free memtable implementations like {@link SkipListMemtable} should use {@link Concurrent}, which
 * performs better for contending updates. Locking memtable implementations like {@link TrieMemtable} should use
 * {@link Exclusive} which assumes updates are already protected by a lock at a higher level.
 */
abstract class NonBlockingCoordinatorLogOffsets<E extends NonBlockingCoordinatorLogOffsets.Entry> extends NonBlockingHashMapLong<E> implements MutableCoordinatorLogOffsets
{
    interface EntryFactory<E>
    {
        E create(CoordinatorLogId logId);
    }

    private final EntryFactory<E> factory;

    private NonBlockingCoordinatorLogOffsets(EntryFactory<E> factory)
    {
        this.factory = factory;
    }

    abstract static class Entry
    {
        protected final Offsets.Mutable base;

        private Entry(CoordinatorLogId logId)
        {
            this.base = new Offsets.Mutable(logId);
        }

        abstract void add(ShortMutationId id);

        abstract Offsets.Mutable offsets();
    }

    public void add(ShortMutationId mutationId)
    {
        computeIfAbsent(mutationId.logId(), logId -> factory.create(new CoordinatorLogId(logId))).add(mutationId);
    }

    @Override
    public Mutations<Offsets.Mutable> mutations()
    {
        return new Mutations<>()
        {
            @Override
            public Offsets.Mutable offsets(long logId)
            {
                E logOffsets = get(logId);
                if (logOffsets == null)
                    return new Offsets.Mutable(new CoordinatorLogId(logId));
                return logOffsets.offsets();
            }

            @Override
            public int size()
            {
                return NonBlockingCoordinatorLogOffsets.super.size();
            }

            @Override
            public Iterator<Long> iterator()
            {
                return Iterators.unmodifiableIterator(keys().asIterator());
            }
        };
    }

    public static class Exclusive extends NonBlockingCoordinatorLogOffsets<Exclusive.Entry>
    {
        private static final Exclusive.EntryFactory FACTORY = new Exclusive.EntryFactory();

        static class Entry extends NonBlockingCoordinatorLogOffsets.Entry
        {
            private Entry(CoordinatorLogId logId)
            {
                super(logId);
            }

            @Override
            void add(ShortMutationId id)
            {
                base.add(id.offset());
            }

            @Override
            Offsets.Mutable offsets()
            {
                return base;
            }
        }

        static class EntryFactory implements NonBlockingCoordinatorLogOffsets.EntryFactory<Exclusive.Entry>
        {
            @Override
            public Exclusive.Entry create(CoordinatorLogId logId)
            {
                return new Exclusive.Entry(logId);
            }
        }

        public Exclusive()
        {
            super(FACTORY);
        }
    }

    public static class Concurrent extends NonBlockingCoordinatorLogOffsets<Concurrent.Entry>
    {
        static class Entry extends NonBlockingCoordinatorLogOffsets.Entry
        {
            ReentrantLock lock = new ReentrantLock();
            MpscUnboundedArrayQueue<Integer> contended;

            private Entry(CoordinatorLogId logId, int contentions)
            {
                super(logId);
                this.contended = new MpscUnboundedArrayQueue<>(Math.max(2, contentions));
            }

            @Override
            void add(ShortMutationId id)
            {
                int offset = id.offset();
                boolean locked = lock.tryLock();
                try
                {
                    if (locked)
                    {
                        flush();
                        base.add(offset);
                    }
                    else
                        contended.add(offset);
                }
                finally
                {
                    if (locked)
                        lock.unlock();
                }
            }

            @Override
            Offsets.Mutable offsets()
            {
                flush();
                return base;
            }

            private void flush()
            {
                boolean locked = lock.tryLock();
                try
                {
                    if (locked && !contended.isEmpty())
                        contended.drain(base::add);
                }
                finally
                {
                    if (locked)
                        lock.unlock();
                }
            }
        }

        static class EntryFactory implements NonBlockingCoordinatorLogOffsets.EntryFactory<Concurrent.Entry>
        {
            final int contentions;

            public EntryFactory(int contentions)
            {
                this.contentions = contentions;
            }

            @Override
            public Entry create(CoordinatorLogId logId)
            {
                return new Concurrent.Entry(logId, contentions);
            }
        }

        public Concurrent(int contentions)
        {
            super(new EntryFactory(contentions));
        }
    }
}
