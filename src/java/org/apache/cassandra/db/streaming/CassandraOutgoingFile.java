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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.KeyIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.streaming.OutgoingStream;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.utils.concurrent.Ref;

import static org.apache.cassandra.db.compaction.Verifier.RangeOwnHelper;

/**
 * used to transfer the part(or whole) of a SSTable data file
 */
public class CassandraOutgoingFile implements OutgoingStream
{
    private static final boolean isFullSSTableTransfersEnabled = DatabaseDescriptor.isFullSSTableTransfersEnabled();
    public static final List<Component> STREAM_COMPONENTS = ImmutableList.of(Component.DATA, Component.PRIMARY_INDEX, Component.STATS,
                                                                             Component.COMPRESSION_INFO, Component.FILTER, Component.SUMMARY,
                                                                             Component.DIGEST, Component.CRC);

    private final Ref<SSTableReader> ref;
    private final long estimatedKeys;
    private final List<SSTableReader.PartitionPositionBounds> sections;
    private final String filename;
    private final CassandraStreamHeader header;
    private final boolean keepSSTableLevel;
    private final List<ComponentInfo> components;
    private final boolean isFullyContained;

    private final List<Range<Token>> ranges;

    public CassandraOutgoingFile(StreamOperation operation, Ref<SSTableReader> ref,
                                 List<SSTableReader.PartitionPositionBounds> sections, Collection<Range<Token>> ranges,
                                 long estimatedKeys)
    {
        Preconditions.checkNotNull(ref.get());
        this.ref = ref;
        this.estimatedKeys = estimatedKeys;
        this.sections = sections;
        this.ranges = ImmutableList.copyOf(ranges);
        this.filename = ref.get().getFilename();
        this.components = getComponents(ref.get());
        this.isFullyContained = fullyContainedIn(this.ranges, ref.get());

        SSTableReader sstable = ref.get();
        keepSSTableLevel = operation == StreamOperation.BOOTSTRAP || operation == StreamOperation.REBUILD;
        this.header = new CassandraStreamHeader(sstable.descriptor.version,
                                                sstable.descriptor.formatType,
                                                estimatedKeys,
                                                sections,
                                                sstable.compression ? sstable.getCompressionMetadata() : null,
                                                keepSSTableLevel ? sstable.getSSTableLevel() : 0,
                                                sstable.header.toComponent(), components, shouldStreamFullSSTable(),
                                                sstable.first,
                                                sstable.metadata().id);
    }

    private static List<ComponentInfo> getComponents(SSTableReader sstable)
    {
        List<ComponentInfo> result = new ArrayList<>(STREAM_COMPONENTS.size());
        for (Component component : STREAM_COMPONENTS)
        {
            File file = new File(sstable.descriptor.filenameFor(component));
            if (file.exists())
                result.add(new ComponentInfo(component.type, file.length()));
        }

        return result;
    }

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
    public long getSize()
    {
        return header.size();
    }

    @Override
    public TableId getTableId()
    {
        return ref.get().metadata().id;
    }

    @Override
    public long getRepairedAt()
    {
        return ref.get().getRepairedAt();
    }

    @Override
    public UUID getPendingRepair()
    {
        return ref.get().getPendingRepair();
    }

    @Override
    public void write(StreamSession session, DataOutputStreamPlus out, int version) throws IOException
    {
        SSTableReader sstable = ref.get();
        CassandraStreamHeader.serializer.serialize(header, out, version);
        out.flush();

        IStreamWriter writer;
        if (shouldStreamFullSSTable())
        {
            writer = new CassandraBlockStreamWriter(sstable, session, components);
        }
        else
        {
            writer = (header.compressionInfo == null) ?
                     new CassandraStreamWriter(sstable, header.sections, session) :
                     new CompressedCassandraStreamWriter(sstable, header.sections,
                                                         header.compressionInfo, session);
        }
        writer.write(out);
    }

    @VisibleForTesting
    public boolean shouldStreamFullSSTable()
    {
        return isFullSSTableTransfersEnabled && isFullyContained;
    }

    @VisibleForTesting
    public boolean fullyContainedIn(List<Range<Token>> normalizedRanges, SSTableReader sstable)
    {
        if (normalizedRanges == null)
            return false;

        RangeOwnHelper rangeOwnHelper = new RangeOwnHelper(normalizedRanges);
        try (KeyIterator iter = new KeyIterator(sstable.descriptor, sstable.metadata()))
        {
            while (iter.hasNext())
            {
                DecoratedKey key = iter.next();
                try
                {
                    rangeOwnHelper.check(key);
                } catch(RuntimeException e)
                {
                    return false;
                }
            }
        }
        return true;
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
        return "CassandraOutgoingFile{" + ref.get().getFilename() + '}';
    }
}
