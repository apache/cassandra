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

package org.apache.cassandra.db.streaming;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.lifecycle.SSTableIntervalTree;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.locator.RangesAtEndpoint;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.streaming.IncomingStream;
import org.apache.cassandra.streaming.OutgoingStream;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.streaming.StreamReceiver;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.TableStreamManager;
import org.apache.cassandra.streaming.messages.StreamMessageHeader;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.concurrent.Ref;
import org.apache.cassandra.utils.concurrent.Refs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * Implements the streaming interface for the native cassandra storage engine.
 *
 * Handles the streaming a one or more section of one of more sstables to and from a specific
 * remote node. The sending side performs a block-level transfer of the source stream, while the receiver
 * must deserilaize that data stream into an partitions and rows, and then write that out as an sstable.
 */
public class CassandraStreamManager implements TableStreamManager
{
    private static final Logger logger = LoggerFactory.getLogger(CassandraStreamManager.class);

    private final ColumnFamilyStore cfs;

    public CassandraStreamManager(ColumnFamilyStore cfs)
    {
        this.cfs = cfs;
    }

    @Override
    public IncomingStream prepareIncomingStream(StreamSession session, StreamMessageHeader header)
    {
        return new CassandraIncomingFile(cfs, session, header);
    }

    @Override
    public StreamReceiver createStreamReceiver(StreamSession session, int totalStreams)
    {
        return new CassandraStreamReceiver(cfs, session, totalStreams);
    }

    @Override
    public Collection<OutgoingStream> createOutgoingStreams(StreamSession session, RangesAtEndpoint replicas, TimeUUID pendingRepair, PreviewKind previewKind)
    {
        Refs<SSTableReader> refs = new Refs<>();
        try
        {
            final List<Range<PartitionPosition>> keyRanges = new ArrayList<>(replicas.size());
            for (Replica replica : replicas)
                keyRanges.add(Range.makeRowRange(replica.range()));
            refs.addAll(cfs.selectAndReference(view -> {
                Set<SSTableReader> sstables = Sets.newHashSet();
                SSTableIntervalTree intervalTree = SSTableIntervalTree.build(view.select(SSTableSet.CANONICAL));
                Predicate<SSTableReader> predicate;
                if (previewKind.isPreview())
                {
                    predicate = previewKind.predicate();
                }
                else if (pendingRepair == ActiveRepairService.NO_PENDING_REPAIR)
                {
                    predicate = Predicates.alwaysTrue();
                }
                else
                {
                    predicate = s -> {
                        StatsMetadata sstableMetadata = s.getSSTableMetadata();
                        return sstableMetadata.pendingRepair != ActiveRepairService.NO_PENDING_REPAIR && sstableMetadata.pendingRepair.equals(pendingRepair);
                    };
                }

                for (Range<PartitionPosition> keyRange : keyRanges)
                {
                    // keyRange excludes its start, while sstableInBounds is inclusive (of both start and end).
                    // This is fine however, because keyRange has been created from a token range through Range.makeRowRange (see above).
                    // And that later method uses the Token.maxKeyBound() method to creates the range, which return a "fake" key that
                    // sort after all keys having the token. That "fake" key cannot however be equal to any real key, so that even
                    // including keyRange.left will still exclude any key having the token of the original token range, and so we're
                    // still actually selecting what we wanted.
                    for (SSTableReader sstable : Iterables.filter(View.sstablesInBounds(keyRange.left, keyRange.right, intervalTree), predicate))
                    {
                        sstables.add(sstable);
                    }
                }

                if (logger.isDebugEnabled())
                    logger.debug("ViewFilter for {}/{} sstables", sstables.size(), Iterables.size(view.select(SSTableSet.CANONICAL)));
                return sstables;
            }).refs);

            // This call is normally preceded by a memtable flush in StreamSession.addTransferRanges.
            // Persistent memtables will not flush, make an sstable with their data.
            cfs.writeAndAddMemtableRanges(session.getPendingRepair(), () -> Range.normalize(keyRanges), refs);

            List<Range<Token>> normalizedFullRanges = Range.normalize(replicas.onlyFull().ranges());
            List<Range<Token>> normalizedAllRanges = Range.normalize(replicas.ranges());
            //Create outgoing file streams for ranges possibly skipping repaired ranges in sstables
            List<OutgoingStream> streams = new ArrayList<>(refs.size());
            for (SSTableReader sstable : refs)
            {
                List<Range<Token>> ranges = sstable.isRepaired() ? normalizedFullRanges : normalizedAllRanges;
                List<SSTableReader.PartitionPositionBounds> sections = sstable.getPositionsForRanges(ranges);

                Ref<SSTableReader> ref = refs.get(sstable);
                if (sections.isEmpty())
                {
                    ref.release();
                    continue;
                }
                streams.add(new CassandraOutgoingFile(session.getStreamOperation(), ref, sections, ranges,
                                                      sstable.estimatedKeysForRanges(ranges)));
            }

            return streams;
        }
        catch (Throwable t)
        {
            refs.release();
            throw t;
        }
    }
}
