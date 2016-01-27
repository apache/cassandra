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
package org.apache.cassandra.db;

import java.io.*;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

public abstract class ReadResponse
{
    // Serializer for single partition read response
    public static final IVersionedSerializer<ReadResponse> serializer = new Serializer();
    // Serializer for partition range read response (this actually delegate to 'serializer' in 3.0 and to
    // 'legacyRangeSliceReplySerializer' in older version.
    public static final IVersionedSerializer<ReadResponse> rangeSliceSerializer = new RangeSliceSerializer();
    // Serializer for the pre-3.0 rang slice responses.
    public static final IVersionedSerializer<ReadResponse> legacyRangeSliceReplySerializer = new LegacyRangeSliceReplySerializer();

    // This is used only when serializing data responses and we can't it easily in other cases. So this can be null, which is slighly
    // hacky, but as this hack doesn't escape this class, and it's easy enough to validate that it's not null when we need, it's "good enough".
    private final ReadCommand command;

    protected ReadResponse(ReadCommand command)
    {
        this.command = command;
    }

    public static ReadResponse createDataResponse(UnfilteredPartitionIterator data, ReadCommand command)
    {
        return new LocalDataResponse(data, command);
    }

    @VisibleForTesting
    public static ReadResponse createRemoteDataResponse(UnfilteredPartitionIterator data, ReadCommand command)
    {
        return new RemoteDataResponse(LocalDataResponse.build(data, command.columnFilter()));
    }

    public static ReadResponse createDigestResponse(UnfilteredPartitionIterator data, ReadCommand command)
    {
        return new DigestResponse(makeDigest(data, command));
    }

    public abstract UnfilteredPartitionIterator makeIterator(ReadCommand command);
    public abstract ByteBuffer digest(ReadCommand command);

    public abstract boolean isDigestResponse();

    protected static ByteBuffer makeDigest(UnfilteredPartitionIterator iterator, ReadCommand command)
    {
        MessageDigest digest = FBUtilities.threadLocalMD5Digest();
        UnfilteredPartitionIterators.digest(command, iterator, digest, command.digestVersion());
        return ByteBuffer.wrap(digest.digest());
    }

    private static class DigestResponse extends ReadResponse
    {
        private final ByteBuffer digest;

        private DigestResponse(ByteBuffer digest)
        {
            super(null);
            assert digest.hasRemaining();
            this.digest = digest;
        }

        public UnfilteredPartitionIterator makeIterator(ReadCommand command)
        {
            throw new UnsupportedOperationException();
        }

        public ByteBuffer digest(ReadCommand command)
        {
            // We assume that the digest is in the proper version, which bug excluded should be true since this is called with
            // ReadCommand.digestVersion() as argument and that's also what we use to produce the digest in the first place.
            // Validating it's the proper digest in this method would require sending back the digest version along with the
            // digest which would waste bandwith for little gain.
            return digest;
        }

        public boolean isDigestResponse()
        {
            return true;
        }
    }

    // built on the owning node responding to a query
    private static class LocalDataResponse extends DataResponse
    {
        private LocalDataResponse(UnfilteredPartitionIterator iter, ReadCommand command)
        {
            super(command, build(iter, command.columnFilter()), SerializationHelper.Flag.LOCAL);
        }

        private static ByteBuffer build(UnfilteredPartitionIterator iter, ColumnFilter selection)
        {
            try (DataOutputBuffer buffer = new DataOutputBuffer())
            {
                UnfilteredPartitionIterators.serializerForIntraNode().serialize(iter, selection, buffer, MessagingService.current_version);
                return buffer.buffer();
            }
            catch (IOException e)
            {
                // We're serializing in memory so this shouldn't happen
                throw new RuntimeException(e);
            }
        }
    }

    // built on the coordinator node receiving a response
    private static class RemoteDataResponse extends DataResponse
    {
        protected RemoteDataResponse(ByteBuffer data)
        {
            super(null, data, SerializationHelper.Flag.FROM_REMOTE);
        }
    }

    static abstract class DataResponse extends ReadResponse
    {
        // TODO: can the digest be calculated over the raw bytes now?
        // The response, serialized in the current messaging version
        private final ByteBuffer data;
        private final SerializationHelper.Flag flag;

        protected DataResponse(ReadCommand command, ByteBuffer data, SerializationHelper.Flag flag)
        {
            super(command);
            this.data = data;
            this.flag = flag;
        }

        public UnfilteredPartitionIterator makeIterator(ReadCommand command)
        {
            try (DataInputBuffer in = new DataInputBuffer(data, true))
            {
                // Note that the command parameter shadows the 'command' field and this is intended because
                // the later can be null (for RemoteDataResponse as those are created in the serializers and
                // those don't have easy access to the command). This is also why we need the command as parameter here.
                return UnfilteredPartitionIterators.serializerForIntraNode().deserialize(in,
                                                                                         MessagingService.current_version,
                                                                                         command.metadata(),
                                                                                         command.columnFilter(),
                                                                                         flag);
            }
            catch (IOException e)
            {
                // We're deserializing in memory so this shouldn't happen
                throw new RuntimeException(e);
            }
        }

        public ByteBuffer digest(ReadCommand command)
        {
            try (UnfilteredPartitionIterator iterator = makeIterator(command))
            {
                return makeDigest(iterator, command);
            }
        }

        public boolean isDigestResponse()
        {
            return false;
        }
    }

    /**
     * A remote response from a pre-3.0 node.  This needs a separate class in order to cleanly handle trimming and
     * reversal of results when the read command calls for it.  Pre-3.0 nodes always return results in the normal
     * sorted order, even if the query asks for reversed results.  Additionally,  pre-3.0 nodes do not have a notion of
     * exclusive slices on non-composite tables, so extra rows may need to be trimmed.
     */
    @VisibleForTesting
    static class LegacyRemoteDataResponse extends ReadResponse
    {
        private final List<ImmutableBTreePartition> partitions;

        @VisibleForTesting
        LegacyRemoteDataResponse(List<ImmutableBTreePartition> partitions)
        {
            super(null); // we never serialize LegacyRemoteDataResponses, so we don't care about the command
            this.partitions = partitions;
        }

        public UnfilteredPartitionIterator makeIterator(final ReadCommand command)
        {
            // Due to a bug in the serialization of AbstractBounds, anything that isn't a Range is understood by pre-3.0 nodes
            // as a Bound, which means IncludingExcludingBounds and ExcludingBounds responses may include keys they shouldn't.
            // So filter partitions that shouldn't be included here.
            boolean skipFirst = false;
            boolean skipLast = false;
            if (!partitions.isEmpty() && command instanceof PartitionRangeReadCommand)
            {
                AbstractBounds<PartitionPosition> keyRange = ((PartitionRangeReadCommand)command).dataRange().keyRange();
                boolean isExcludingBounds = keyRange instanceof ExcludingBounds;
                skipFirst = isExcludingBounds && !keyRange.contains(partitions.get(0).partitionKey());
                skipLast = (isExcludingBounds || keyRange instanceof IncludingExcludingBounds) && !keyRange.contains(partitions.get(partitions.size() - 1).partitionKey());
            }

            final List<ImmutableBTreePartition> toReturn;
            if (skipFirst || skipLast)
            {
                toReturn = partitions.size() == 1
                         ? Collections.emptyList()
                         : partitions.subList(skipFirst ? 1 : 0, skipLast ? partitions.size() - 1 : partitions.size());
            }
            else
            {
                toReturn = partitions;
            }

            return new AbstractUnfilteredPartitionIterator()
            {
                private int idx;

                public boolean isForThrift()
                {
                    return true;
                }

                public CFMetaData metadata()
                {
                    return command.metadata();
                }

                public boolean hasNext()
                {
                    return idx < toReturn.size();
                }

                public UnfilteredRowIterator next()
                {
                    ImmutableBTreePartition partition = toReturn.get(idx++);

                    ClusteringIndexFilter filter = command.clusteringIndexFilter(partition.partitionKey());

                    // Pre-3.0, we didn't have a way to express exclusivity for non-composite comparators, so all slices were
                    // inclusive on both ends. If we have exclusive slice ends, we need to filter the results here.
                    if (!command.metadata().isCompound())
                        return partition.unfilteredIterator(command.columnFilter(), filter.getSlices(command.metadata()), filter.isReversed());

                    return partition.unfilteredIterator(command.columnFilter(), Slices.ALL, filter.isReversed());
                }
            };
        }

        public ByteBuffer digest(ReadCommand command)
        {
            try (UnfilteredPartitionIterator iterator = makeIterator(command))
            {
                return makeDigest(iterator, command);
            }
        }

        public boolean isDigestResponse()
        {
            return false;
        }
    }

    private static class Serializer implements IVersionedSerializer<ReadResponse>
    {
        public void serialize(ReadResponse response, DataOutputPlus out, int version) throws IOException
        {
            boolean isDigest = response instanceof DigestResponse;
            ByteBuffer digest = isDigest ? ((DigestResponse)response).digest : ByteBufferUtil.EMPTY_BYTE_BUFFER;
            if (version < MessagingService.VERSION_30)
            {
                out.writeInt(digest.remaining());
                out.write(digest);
                out.writeBoolean(isDigest);
                if (!isDigest)
                {
                    assert response.command != null; // we only serialize LocalDataResponse, which always has the command set
                    try (UnfilteredPartitionIterator iter = response.makeIterator(response.command))
                    {
                        assert iter.hasNext();
                        try (UnfilteredRowIterator partition = iter.next())
                        {
                            ByteBufferUtil.writeWithShortLength(partition.partitionKey().getKey(), out);
                            LegacyLayout.serializeAsLegacyPartition(response.command, partition, out, version);
                        }
                        assert !iter.hasNext();
                    }
                }
                return;
            }

            ByteBufferUtil.writeWithVIntLength(digest, out);
            if (!isDigest)
            {
                ByteBuffer data = ((DataResponse)response).data;
                ByteBufferUtil.writeWithVIntLength(data, out);
            }
        }

        public ReadResponse deserialize(DataInputPlus in, int version) throws IOException
        {
            if (version < MessagingService.VERSION_30)
            {
                byte[] digest = null;
                int digestSize = in.readInt();
                if (digestSize > 0)
                {
                    digest = new byte[digestSize];
                    in.readFully(digest, 0, digestSize);
                }
                boolean isDigest = in.readBoolean();
                assert isDigest == digestSize > 0;
                if (isDigest)
                {
                    assert digest != null;
                    return new DigestResponse(ByteBuffer.wrap(digest));
                }

                // ReadResponses from older versions are always single-partition (ranges are handled by RangeSliceReply)
                ByteBuffer key = ByteBufferUtil.readWithShortLength(in);
                try (UnfilteredRowIterator rowIterator = LegacyLayout.deserializeLegacyPartition(in, version, SerializationHelper.Flag.FROM_REMOTE, key))
                {
                    if (rowIterator == null)
                        return new LegacyRemoteDataResponse(Collections.emptyList());

                    return new LegacyRemoteDataResponse(Collections.singletonList(ImmutableBTreePartition.create(rowIterator)));
                }
            }

            ByteBuffer digest = ByteBufferUtil.readWithVIntLength(in);
            if (digest.hasRemaining())
                return new DigestResponse(digest);

            assert version == MessagingService.VERSION_30;
            ByteBuffer data = ByteBufferUtil.readWithVIntLength(in);
            return new RemoteDataResponse(data);
        }

        public long serializedSize(ReadResponse response, int version)
        {
            boolean isDigest = response instanceof DigestResponse;
            ByteBuffer digest = isDigest ? ((DigestResponse)response).digest : ByteBufferUtil.EMPTY_BYTE_BUFFER;

            if (version < MessagingService.VERSION_30)
            {
                long size = TypeSizes.sizeof(digest.remaining())
                        + digest.remaining()
                        + TypeSizes.sizeof(isDigest);
                if (!isDigest)
                {
                    assert response.command != null; // we only serialize LocalDataResponse, which always has the command set
                    try (UnfilteredPartitionIterator iter = response.makeIterator(response.command))
                    {
                        assert iter.hasNext();
                        try (UnfilteredRowIterator partition = iter.next())
                        {
                            size += ByteBufferUtil.serializedSizeWithShortLength(partition.partitionKey().getKey());
                            size += LegacyLayout.serializedSizeAsLegacyPartition(response.command, partition, version);
                        }
                        assert !iter.hasNext();
                    }
                }
                return size;
            }

            long size = ByteBufferUtil.serializedSizeWithVIntLength(digest);
            if (!isDigest)
            {
                // Note that we can only get there if version == 3.0, which is the current_version. When we'll change the
                // version, we'll have to deserialize/re-serialize the data to be in the proper version.
                assert version == MessagingService.VERSION_30;
                ByteBuffer data = ((DataResponse)response).data;
                size += ByteBufferUtil.serializedSizeWithVIntLength(data);
            }
            return size;
        }
    }

    private static class RangeSliceSerializer implements IVersionedSerializer<ReadResponse>
    {
        public void serialize(ReadResponse response, DataOutputPlus out, int version) throws IOException
        {
            if (version < MessagingService.VERSION_30)
                legacyRangeSliceReplySerializer.serialize(response, out, version);
            else
                serializer.serialize(response, out, version);
        }

        public ReadResponse deserialize(DataInputPlus in, int version) throws IOException
        {
            return version < MessagingService.VERSION_30
                 ? legacyRangeSliceReplySerializer.deserialize(in, version)
                 : serializer.deserialize(in, version);
        }

        public long serializedSize(ReadResponse response, int version)
        {
            return version < MessagingService.VERSION_30
                 ? legacyRangeSliceReplySerializer.serializedSize(response, version)
                 : serializer.serializedSize(response, version);
        }
    }

    private static class LegacyRangeSliceReplySerializer implements IVersionedSerializer<ReadResponse>
    {
        public void serialize(ReadResponse response, DataOutputPlus out, int version) throws IOException
        {
            assert version < MessagingService.VERSION_30;

            // determine the number of partitions upfront for serialization
            int numPartitions = 0;
            assert response.command != null; // we only serialize LocalDataResponse, which always has the command set
            try (UnfilteredPartitionIterator iterator = response.makeIterator(response.command))
            {
                while (iterator.hasNext())
                {
                    try (UnfilteredRowIterator atomIterator = iterator.next())
                    {
                        numPartitions++;

                        // we have to fully exhaust the subiterator
                        while (atomIterator.hasNext())
                            atomIterator.next();
                    }
                }
            }

            out.writeInt(numPartitions);

            try (UnfilteredPartitionIterator iterator = response.makeIterator(response.command))
            {
                while (iterator.hasNext())
                {
                    try (UnfilteredRowIterator partition = iterator.next())
                    {
                        ByteBufferUtil.writeWithShortLength(partition.partitionKey().getKey(), out);
                        LegacyLayout.serializeAsLegacyPartition(response.command, partition, out, version);
                    }
                }
            }
        }

        public ReadResponse deserialize(DataInputPlus in, int version) throws IOException
        {
            int partitionCount = in.readInt();
            ArrayList<ImmutableBTreePartition> partitions = new ArrayList<>(partitionCount);
            for (int i = 0; i < partitionCount; i++)
            {
                ByteBuffer key = ByteBufferUtil.readWithShortLength(in);
                try (UnfilteredRowIterator partition = LegacyLayout.deserializeLegacyPartition(in, version, SerializationHelper.Flag.FROM_REMOTE, key))
                {
                    partitions.add(ImmutableBTreePartition.create(partition));
                }
            }
            return new LegacyRemoteDataResponse(partitions);
        }

        public long serializedSize(ReadResponse response, int version)
        {
            assert version < MessagingService.VERSION_30;
            long size = TypeSizes.sizeof(0);  // number of partitions

            assert response.command != null; // we only serialize LocalDataResponse, which always has the command set
            try (UnfilteredPartitionIterator iterator = response.makeIterator(response.command))
            {
                while (iterator.hasNext())
                {
                    try (UnfilteredRowIterator partition = iterator.next())
                    {
                        size += ByteBufferUtil.serializedSizeWithShortLength(partition.partitionKey().getKey());
                        size += LegacyLayout.serializedSizeAsLegacyPartition(response.command, partition, version);
                    }
                }
            }
            return size;
        }
    }
}
