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
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableTxnWriter;
import org.apache.cassandra.journal.KeySupport;
import org.apache.cassandra.journal.SegmentCompactor;
import org.apache.cassandra.journal.StaticSegment;
import org.apache.cassandra.journal.StaticSegment.KeyOrderReader;

/**
 * Segment compactor: takes static segments and compacts them into a single SSTable.
 */
public class AccordSegmentCompactor<K, V> implements SegmentCompactor<K, V>
{
    private static final Logger logger = LoggerFactory.getLogger(AccordSegmentCompactor.class);

    @Override
    public Collection<StaticSegment<K, V>> compact(Collection<StaticSegment<K, V>> segments, KeySupport<K> keySupport)
    {
        Invariants.checkState(segments.size() >= 2, () -> String.format("Can only compact 2 or more segments, but got %d", segments.size()));
        logger.info("Compacting {} static segments: {}", segments.size(), segments);

        PriorityQueue<KeyOrderReader<K>> readers = new PriorityQueue<>();
        for (StaticSegment<K, V> segment : segments)
        {
            KeyOrderReader<K> reader = segment.keyOrderReader();
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
            K key = null;
            PartitionUpdate.SimpleBuilder partitionBuilder = null;

            try
            {
                KeyOrderReader<K> reader;
                while ((reader = readers.poll()) != null)
                {
                    if (!reader.key().equals(key)) // first ever - or new - key
                    {
                        if (partitionBuilder != null) // append previous partition if any
                            writer.append(partitionBuilder.build().unfilteredIterator());

                        key = reader.key();
                        partitionBuilder = PartitionUpdate.simpleBuilder(
                            AccordKeyspace.Journal, AccordJournalTable.makePartitionKey(cfs, key, keySupport, reader.descriptor.userVersion)
                        );
                    }

                    boolean advanced;
                    do
                    {
                        partitionBuilder.row(reader.descriptor.timestamp, reader.offset())
                                        .add("record", reader.record())
                                        .add("user_version", reader.descriptor.userVersion);
                    }
                    while ((advanced = reader.advance()) && reader.key().equals(key));

                    if (advanced) readers.offer(reader); // there is more to this reader, but not with this key
                }

                //noinspection DataFlowIssue
                writer.append(partitionBuilder.build().unfilteredIterator()); // append the last partition
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
}

