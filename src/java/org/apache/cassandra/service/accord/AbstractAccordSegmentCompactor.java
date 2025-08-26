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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.utils.Invariants;
import org.apache.cassandra.db.BufferClustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.exceptions.UnknownTableException;
import org.apache.cassandra.io.sstable.SSTableTxnWriter;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.journal.SegmentCompactor;
import org.apache.cassandra.journal.StaticSegment;
import org.apache.cassandra.journal.StaticSegment.KeyOrderReader;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.accord.AccordJournalValueSerializers.FlyweightImage;
import org.apache.cassandra.service.accord.AccordJournalValueSerializers.FlyweightSerializer;
import org.apache.cassandra.utils.BulkIterator;
import org.apache.cassandra.utils.NoSpamLogger;

import static java.util.concurrent.TimeUnit.MINUTES;
import org.apache.cassandra.service.accord.serializers.Version;
import org.apache.cassandra.utils.btree.BTree;
import org.apache.cassandra.utils.btree.UpdateFunction;

/**
 * Segment compactor: takes static segments and compacts them into a single SSTable.
 */
public abstract class AbstractAccordSegmentCompactor<V> implements SegmentCompactor<JournalKey, V>
{
    protected static final Logger logger = LoggerFactory.getLogger(AbstractAccordSegmentCompactor.class);
    private static final NoSpamLogger.NoSpamLogStatement unknownTable = NoSpamLogger.getStatement(logger, "Unknown (probably dropped) TableId {} reading {}; skipping record", 1L, MINUTES);

    static final Object[] rowTemplate = BTree.build(BulkIterator.of(new Object[2]), 2, UpdateFunction.noOp);

    protected final Version userVersion;
    protected final ColumnData userVersionCell;
    protected final ColumnFamilyStore cfs;
    protected final long timestamp = ClientState.getTimestamp();

    public AbstractAccordSegmentCompactor(Version userVersion, ColumnFamilyStore cfs)
    {
        this.userVersion = userVersion;
        this.userVersionCell = BufferCell.live(AccordKeyspace.JournalColumns.user_version, timestamp, Int32Type.instance.decompose(userVersion.version));
        this.cfs = cfs;
    }

    void switchPartitions() {}

    boolean considerWritingKey()
    {
        return false;
    }

    abstract void initializeWriter(int estimatedKeyCount);
    abstract SSTableTxnWriter writer();
    abstract void finishAndAddWriter();
    abstract Throwable cleanupWriter(Throwable t);

    // Only valid in the scope of a single `compact` call
    private JournalKey prevKey;
    private DecoratedKey prevDecoratedKey;

    @Override
    public Collection<StaticSegment<JournalKey, V>> compact(Collection<StaticSegment<JournalKey, V>> segments)
    {
        logger.info("Compacting {} static segments: {}", segments.size(), segments);

        // TODO (expected): this will be a large over-estimate. should make segments an sstable format and include cardinality estimation
        int estimatedKeyCount = 0;
        PriorityQueue<KeyOrderReader<JournalKey>> readers = new PriorityQueue<>();
        for (StaticSegment<JournalKey, V> segment : segments)
        {
            estimatedKeyCount += segment.entryCount();
            KeyOrderReader<JournalKey> reader = segment.keyOrderReader();
            if (reader.advance())
                readers.add(reader);
            else
                reader.close();
        }

        // nothing to compact (all segments empty, should never happen, but it is theoretically possible?) - exit early
        // TODO (required): investigate how this comes to be, check if there is a cleanup issue
        if (readers.isEmpty())
            return Collections.emptyList();

        initializeWriter(estimatedKeyCount);

        JournalKey key = null;
        FlyweightImage builder = null;
        FlyweightSerializer<Object, FlyweightImage> serializer = null;
        long firstDescriptor = -1, lastDescriptor = -1;
        int firstOffset = -1, lastOffset = -1;
        try
        {
            KeyOrderReader<JournalKey> reader;
            while ((reader = readers.poll()) != null)
            {
                if (key == null || !reader.key().equals(key))
                {
                    maybeWritePartition(key, builder, serializer, firstDescriptor, firstOffset);
                    switchPartitions();
                    key = reader.key();
                    serializer = (FlyweightSerializer<Object, FlyweightImage>) key.type.serializer;
                    builder = serializer.mergerFor();
                    builder.reset(key);
                    firstDescriptor = lastDescriptor = -1;
                    firstOffset = lastOffset = -1;
                }

                Version realVersion = Version.fromVersion(reader.descriptor.userVersion);

                boolean advanced;
                do
                {
                    if (builder == null)
                    {
                        builder = serializer.mergerFor();
                        builder.reset(key);
                    }

                    try (DataInputBuffer in = new DataInputBuffer(reader.record(), false))
                    {
                        if (lastDescriptor != -1)
                        {
                            Invariants.require(reader.descriptor.timestamp <= lastDescriptor,
                                               "Descriptors were accessed out of order: %d was accessed after %d", reader.descriptor.timestamp, lastDescriptor);
                            Invariants.require(reader.descriptor.timestamp != lastDescriptor ||
                                               reader.offset() < lastOffset,
                                               "Offsets were accessed out of order: %d was accessed after %s", reader.offset(), lastOffset);
                        }
                        serializer.deserialize(key, builder, in, realVersion);
                        lastDescriptor = reader.descriptor.timestamp;
                        lastOffset = reader.offset();
                        if (firstDescriptor == -1)
                        {
                            firstDescriptor = lastDescriptor;
                            firstOffset = lastOffset;
                        }
                    }

                    if (considerWritingKey())
                    {
                        maybeWritePartition(key, builder, serializer, firstDescriptor, firstOffset);
                        builder = null;
                        firstDescriptor = lastDescriptor = -1;
                        firstOffset = lastOffset = -1;
                    }
                }
                while ((advanced = reader.advance()) && reader.key().equals(key));

                if (advanced) readers.offer(reader); // there is more to this reader, but not with this key
                else reader.close();
            }

            maybeWritePartition(key, builder, serializer, firstDescriptor, firstOffset);
            switchPartitions();
        }
        catch (UnknownTableException e)
        {
            unknownTable.info(e.id, key);
        }
        catch (Throwable t)
        {
            t = cleanupWriter(t);
            throw new RuntimeException(String.format("Caught exception while serializing. Last seen key: %s", key), t);
        }
        finally
        {
            prevKey = null;
            prevDecoratedKey = null;
        }

        finishAndAddWriter();
        return Collections.emptyList();
    }

    private void maybeWritePartition(JournalKey key, FlyweightImage builder, FlyweightSerializer<Object, FlyweightImage> serializer, long descriptor, int offset) throws IOException
    {
        if (builder != null)
        {
            DecoratedKey decoratedKey = AccordKeyspace.JournalColumns.decorate(key);
            Invariants.requireArgument(prevKey == null || normalize(decoratedKey.compareTo(prevDecoratedKey)) == normalize(JournalKey.SUPPORT.compare(key, prevKey)),
                                       "Partition key and JournalKey didn't have matching order, which may imply a serialization issue.\n%s (%s)\n%s (%s)",
                                       key, decoratedKey, prevKey, prevDecoratedKey);
            prevKey = key;
            prevDecoratedKey = decoratedKey;

            Object[] rowData = rowTemplate.clone();
            try (DataOutputBuffer out = DataOutputBuffer.scratchBuffer.get())
            {
                serializer.reserialize(key, builder, out, userVersion);
                rowData[0] = BufferCell.live(AccordKeyspace.JournalColumns.record, timestamp, out.asNewBuffer());
            }
            rowData[1] = userVersionCell;
            Row row = BTreeRow.create(BufferClustering.make(LongType.instance.decompose(descriptor), Int32Type.instance.decompose(offset)), LivenessInfo.EMPTY, Row.Deletion.LIVE, rowData);
            PartitionUpdate update = PartitionUpdate.singleRowUpdate(AccordKeyspace.Journal, decoratedKey, row);
            writer().append(update.unfilteredIterator());
        }
    }

    private static int normalize(int cmp)
    {
        if (cmp == 0)
            return 0;
        if (cmp < 0)
            return -1;
        return 1;
    }
}

