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

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.streaming.OutgoingStream;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.StreamingDataOutputPlus;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.concurrent.Ref;

/**
 * used to transfer the part(or whole) of a SSTable data file
 */
public class CassandraOutgoingFile implements OutgoingStream
{
    private final Ref<SSTableReader> ref;
    private final long estimatedKeys;
    private final List<SSTableReader.PartitionPositionBounds> sections;
    private final String filename;
    private final boolean shouldStreamEntireSSTable;
    private final StreamOperation operation;
    private final CassandraStreamHeader header;

    public CassandraOutgoingFile(StreamOperation operation, Ref<SSTableReader> ref,
                                 List<SSTableReader.PartitionPositionBounds> sections, List<Range<Token>> normalizedRanges,
                                 long estimatedKeys)
    {
        Preconditions.checkNotNull(ref.get());
        Range.assertNormalized(normalizedRanges);
        this.operation = operation;
        this.ref = ref;
        this.estimatedKeys = estimatedKeys;
        this.sections = sections;

        SSTableReader sstable = ref.get();

        this.filename = sstable.getFilename();
        this.shouldStreamEntireSSTable = computeShouldStreamEntireSSTables();
        ComponentManifest manifest = ComponentManifest.create(sstable);
        this.header = makeHeader(sstable, operation, sections, estimatedKeys, shouldStreamEntireSSTable, manifest);
    }

    private static CassandraStreamHeader makeHeader(SSTableReader sstable,
                                                    StreamOperation operation,
                                                    List<SSTableReader.PartitionPositionBounds> sections,
                                                    long estimatedKeys,
                                                    boolean shouldStreamEntireSSTable,
                                                    ComponentManifest manifest)
    {
        CompressionInfo compressionInfo = sstable.compression
                ? CompressionInfo.newLazyInstance(sstable.getCompressionMetadata(), sections)
                : null;

        return CassandraStreamHeader.builder()
                                    .withSSTableVersion(sstable.descriptor.version)
                                    .withSSTableLevel(operation.keepSSTableLevel() ? sstable.getSSTableLevel() : 0)
                                    .withEstimatedKeys(estimatedKeys)
                                    .withSections(sections)
                                    .withCompressionInfo(compressionInfo)
                                    .withSerializationHeader(sstable.header.toComponent())
                                    .isEntireSSTable(shouldStreamEntireSSTable)
                                    .withComponentManifest(manifest)
                                    .withFirstKey(sstable.getFirst())
                                    .withTableId(sstable.metadata().id)
                                    .build();
    }

    @VisibleForTesting
    public static CassandraOutgoingFile fromStream(OutgoingStream stream)
    {
        Preconditions.checkArgument(stream instanceof CassandraOutgoingFile);
        return (CassandraOutgoingFile) stream;
    }

    @VisibleForTesting
    public Ref<SSTableReader> getRef()
    {
        return ref;
    }

    @Override
    public String getName()
    {
        return filename;
    }

    @Override
    public long getEstimatedSize()
    {
        return header.size();
    }

    @Override
    public TableId getTableId()
    {
        return ref.get().metadata().id;
    }

    @Override
    public int getNumFiles()
    {
        return shouldStreamEntireSSTable ? header.componentManifest.components().size() : 1;
    }

    @Override
    public long getRepairedAt()
    {
        return ref.get().getRepairedAt();
    }

    @Override
    public TimeUUID getPendingRepair()
    {
        return ref.get().getPendingRepair();
    }

    @Override
    public void write(StreamSession session, StreamingDataOutputPlus out, int version) throws IOException
    {
        SSTableReader sstable = ref.get();

        if (shouldStreamEntireSSTable)
        {
            // Acquire lock to avoid concurrent sstable component mutation because of stats update or index summary
            // redistribution, otherwise file sizes recorded in component manifest will be different from actual
            // file sizes.
            // Recreate the latest manifest and hard links for mutatable components in case they are modified.
            try (ComponentContext context = sstable.runWithLock(ignored -> ComponentContext.create(sstable)))
            {
                CassandraStreamHeader current = makeHeader(sstable, operation, sections, estimatedKeys, true, context.manifest());
                CassandraStreamHeader.serializer.serialize(current, out, version);
                out.flush();

                CassandraEntireSSTableStreamWriter writer = new CassandraEntireSSTableStreamWriter(sstable, session, context);
                writer.write(out);
            }
        }
        else
        {
            // legacy streaming is not affected by stats metadata mutation and index sumary redistribution
            CassandraStreamHeader.serializer.serialize(header, out, version);
            out.flush();

            CassandraStreamWriter writer = header.isCompressed() ?
                                           new CassandraCompressedStreamWriter(sstable, header, session) :
                                           new CassandraStreamWriter(sstable, header, session);
            writer.write(out);
        }
    }

    @VisibleForTesting
    public boolean computeShouldStreamEntireSSTables()
    {
        // don't stream if full sstable transfers are disabled or legacy counter shards are present
        if (!DatabaseDescriptor.streamEntireSSTables() || ref.get().getSSTableMetadata().hasLegacyCounterShards)
            return false;

        return contained(sections, ref.get());
    }

    @VisibleForTesting
    public boolean contained(List<SSTableReader.PartitionPositionBounds> sections, SSTableReader sstable)
    {
        if (sections == null || sections.isEmpty())
            return false;

        // if transfer sections contain entire sstable
        long transferLength = sections.stream().mapToLong(p -> p.upperPosition - p.lowerPosition).sum();
        return transferLength == sstable.uncompressedLength();
    }

    @Override
    public void finish()
    {
        ref.release();
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CassandraOutgoingFile that = (CassandraOutgoingFile) o;
        return estimatedKeys == that.estimatedKeys &&
               Objects.equals(ref, that.ref) &&
               Objects.equals(sections, that.sections);
    }

    public int hashCode()
    {
        return Objects.hash(ref, estimatedKeys, sections);
    }

    @Override
    public String toString()
    {
        return "CassandraOutgoingFile{" + filename + '}';
    }
}
