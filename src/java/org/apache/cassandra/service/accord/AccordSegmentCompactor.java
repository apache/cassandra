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
package org.apache.cassandra.service.accord;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.PriorityQueue;

import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.utils.Invariants;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.PartitionUpdate.SimpleBuilder;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableTxnWriter;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.journal.KeySupport;
import org.apache.cassandra.journal.SegmentCompactor;
import org.apache.cassandra.journal.StaticSegment;
import org.apache.cassandra.journal.StaticSegment.KeyOrderReader;
import org.apache.cassandra.service.accord.AccordJournalValueSerializers.FlyweightSerializer;

/**
 * Segment compactor: takes static segments and compacts them into a single SSTable.
 */
public class AccordSegmentCompactor<V> implements SegmentCompactor<JournalKey, V>
{
    private static final Logger logger = LoggerFactory.getLogger(AccordSegmentCompactor.class);
    private final int userVersion;
    private final KeySupport<JournalKey> keySupport;

    public AccordSegmentCompactor(KeySupport<JournalKey> keySupport, int userVersion)
    {
        this.userVersion = userVersion;
        this.keySupport = keySupport;
    }

    @Override
    public Collection<StaticSegment<JournalKey, V>> compact(Collection<StaticSegment<JournalKey, V>> segments)
    {
        Invariants.checkState(segments.size() >= 2, () -> String.format("Can only compact 2 or more segments, but got %d", segments.size()));
        logger.info("Compacting {} static segments: {}", segments.size(), segments);

        PriorityQueue<KeyOrderReader<JournalKey>> readers = new PriorityQueue<>();
        for (StaticSegment<JournalKey, V> segment : segments)
        {
            KeyOrderReader<JournalKey> reader = segment.keyOrderReader();
            if (reader.advance())
                readers.add(reader);
        }

        // nothing to compact (all segments empty, should never happen, but it is theoretically possible?) - exit early
        // TODO: investigate how this comes to be, check if there is a cleanup issue
        if (readers.isEmpty())
            return segments;

        ColumnFamilyStore cfs = Keyspace.open(AccordKeyspace.metadata().name).getColumnFamilyStore(AccordKeyspace.JOURNAL);
        Descriptor descriptor = cfs.newSSTableDescriptor(cfs.getDirectories().getDirectoryForNewSSTables());
        SerializationHeader header = new SerializationHeader(true, cfs.metadata(), cfs.metadata().regularAndStaticColumns(), EncodingStats.NO_STATS);

        try (SSTableTxnWriter writer = SSTableTxnWriter.create(cfs, descriptor, 0, 0, null, false, header))
        {
            JournalKey key = null;
            Object builder = null;
            FlyweightSerializer<Object, Object> serializer = null;
            long lastDescriptor = -1;
            int lastOffset = -1;
            try
            {
                KeyOrderReader<JournalKey> reader;
                while ((reader = readers.poll()) != null)
                {
                    if (key == null || !reader.key().equals(key))
                    {
                        maybeWritePartition(cfs, writer, key, builder, serializer, lastDescriptor, lastOffset);

                        key = reader.key();
                        serializer = (FlyweightSerializer<Object, Object>) key.type.serializer;
                        builder = serializer.mergerFor(key);
                    }

                    boolean advanced;
                    do
                    {
                        try (DataInputBuffer in = new DataInputBuffer(reader.record(), false))
                        {
                            serializer.deserialize(key, builder, in, reader.descriptor.userVersion);
                            lastDescriptor = reader.descriptor.timestamp;
                            lastOffset = reader.offset();
                        }
                    }
                    while ((advanced = reader.advance()) && reader.key().equals(key));

                    if (advanced) readers.offer(reader); // there is more to this reader, but not with this key
                }

                maybeWritePartition(cfs, writer, key, builder, serializer, lastDescriptor, lastOffset);
            }
            catch (Throwable t)
            {
                Throwable accumulate = writer.abort(t);
                Throwables.throwIfUnchecked(accumulate);
                throw new RuntimeException(accumulate);
            }

            cfs.addSSTables(writer.finish(true));
            return Collections.emptyList();
        }
    }

    private void maybeWritePartition(ColumnFamilyStore cfs, SSTableTxnWriter writer, JournalKey key, Object builder, FlyweightSerializer<Object, Object> serializer, long descriptor, int offset) throws IOException
    {
        if (builder != null)
        {
            SimpleBuilder partitionBuilder = PartitionUpdate.simpleBuilder(AccordKeyspace.Journal, AccordJournalTable.makePartitionKey(cfs, key, keySupport, userVersion));
            try (DataOutputBuffer out = DataOutputBuffer.scratchBuffer.get())
            {
                serializer.serialize(key, serializer.toWriter(builder), out, userVersion);
                partitionBuilder.row(descriptor, offset)
                                .add("record", out.asNewBuffer())
                                .add("user_version", userVersion);
            }
            writer.append(partitionBuilder.build().unfilteredIterator());
        }
    }
}

