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

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

public abstract class ReadResponse
{
    public static final IVersionedSerializer<ReadResponse> serializer = new Serializer();
    public static final IVersionedSerializer<ReadResponse> legacyRangeSliceReplySerializer = new LegacyRangeSliceReplySerializer();

    // This is used only when serializing data responses and we can't it easily in other cases. So this can be null, which is slighly
    // hacky, but as this hack doesn't escape this class, and it's easy enough to validate that it's not null when we need, it's "good enough".
    private final CFMetaData metadata;

    protected ReadResponse(CFMetaData metadata)
    {
        this.metadata = metadata;
    }

    public static ReadResponse createDataResponse(UnfilteredPartitionIterator data)
    {
        return new DataResponse(data);
    }

    public static ReadResponse createDigestResponse(UnfilteredPartitionIterator data)
    {
        return new DigestResponse(makeDigest(data));
    }

    public abstract UnfilteredPartitionIterator makeIterator(CFMetaData metadata, ReadCommand command);

    public abstract ByteBuffer digest(CFMetaData metadata, ReadCommand command);

    public abstract boolean isDigestQuery();

    protected static ByteBuffer makeDigest(UnfilteredPartitionIterator iterator)
    {
        MessageDigest digest = FBUtilities.threadLocalMD5Digest();
        UnfilteredPartitionIterators.digest(iterator, digest);
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

        public UnfilteredPartitionIterator makeIterator(CFMetaData metadata, ReadCommand command)
        {
            throw new UnsupportedOperationException();
        }

        public ByteBuffer digest(CFMetaData metadata, ReadCommand command)
        {
            return digest;
        }

        public boolean isDigestQuery()
        {
            return true;
        }
    }

    private static class DataResponse extends ReadResponse
    {
        // The response, serialized in the current messaging version
        private final ByteBuffer data;
        private final SerializationHelper.Flag flag;

        private DataResponse(ByteBuffer data)
        {
            super(null); // This is never call on the serialization side, where we actually care of the metadata.
            this.data = data;
            this.flag = SerializationHelper.Flag.FROM_REMOTE;
        }

        private DataResponse(UnfilteredPartitionIterator iter)
        {
            super(iter.metadata());
            try (DataOutputBuffer buffer = new DataOutputBuffer())
            {
                UnfilteredPartitionIterators.serializerForIntraNode().serialize(iter, buffer, MessagingService.current_version);
                this.data = buffer.buffer();
                this.flag = SerializationHelper.Flag.LOCAL;
            }
            catch (IOException e)
            {
                // We're serializing in memory so this shouldn't happen
                throw new RuntimeException(e);
            }
        }

        public UnfilteredPartitionIterator makeIterator(CFMetaData metadata, ReadCommand command)
        {
            try
            {
                DataInputPlus in = new DataInputBuffer(data, true);
                return UnfilteredPartitionIterators.serializerForIntraNode().deserialize(in, MessagingService.current_version, metadata, flag);
            }
            catch (IOException e)
            {
                // We're deserializing in memory so this shouldn't happen
                throw new RuntimeException(e);
            }
        }

        public ByteBuffer digest(CFMetaData metadata, ReadCommand command)
        {
            try (UnfilteredPartitionIterator iterator = makeIterator(metadata, command))
            {
                return makeDigest(iterator);
            }
        }

        public boolean isDigestQuery()
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
    private static class LegacyRemoteDataResponse extends ReadResponse
    {
        private final List<ArrayBackedPartition> partitions;

        private LegacyRemoteDataResponse(List<ArrayBackedPartition> partitions)
        {
            super(null); // we never serialize LegacyRemoteDataResponses, so we don't care about the metadata
            this.partitions = partitions;
        }

        public UnfilteredPartitionIterator makeIterator(CFMetaData metadata, final ReadCommand command)
        {
            return new AbstractUnfilteredPartitionIterator()
            {
                private int idx;

                public boolean isForThrift()
                {
                    return true;
                }

                public CFMetaData metadata()
                {
                    return metadata;
                }

                public boolean hasNext()
                {
                    return idx < partitions.size();
                }

                public UnfilteredRowIterator next()
                {
                    ArrayBackedPartition partition = partitions.get(idx++);

                    ClusteringIndexFilter filter = command.clusteringIndexFilter(partition.partitionKey());

                    // Pre-3.0, we didn't have a way to express exclusivity for non-composite comparators, so all slices were
                    // inclusive on both ends. If we have exclusive slice ends, we need to filter the results here.
                    if (!command.metadata().isCompound())
                        return filter.filter(partition.sliceableUnfilteredIterator(command.columnFilter(), filter.isReversed()));

                    return partition.unfilteredIterator(command.columnFilter(), Slices.ALL, filter.isReversed());
                }
            };
        }

        public ByteBuffer digest(CFMetaData metadata, ReadCommand command)
        {
            try (UnfilteredPartitionIterator iterator = makeIterator(metadata, command))
            {
                return makeDigest(iterator);
            }
        }

        public boolean isDigestQuery()
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
                    assert !(response instanceof LegacyRemoteDataResponse); // we only use those on the receiving side
                    try (UnfilteredPartitionIterator iter = response.makeIterator(response.metadata, null))
                    {
                        assert iter.hasNext();
                        try (UnfilteredRowIterator partition = iter.next())
                        {
                            ByteBufferUtil.writeWithShortLength(partition.partitionKey().getKey(), out);
                            LegacyLayout.serializeAsLegacyPartition(partition, out, version);
                        }
                        assert !iter.hasNext();
                    }
                }
                return;
            }

            ByteBufferUtil.writeWithVIntLength(digest, out);
            if (!isDigest)
            {
                // Note that we can only get there if version == 3.0, which is the current_version. When we'll change the
                // version, we'll have to deserialize/re-serialize the data to be in the proper version.
                assert version == MessagingService.VERSION_30;
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
                UnfilteredRowIterator rowIterator = LegacyLayout.deserializeLegacyPartition(in, version, SerializationHelper.Flag.FROM_REMOTE, key);
                if (rowIterator == null)
                    return new LegacyRemoteDataResponse(Collections.emptyList());

                try
                {
                    return new LegacyRemoteDataResponse(Collections.singletonList(ArrayBackedPartition.create(rowIterator)));
                }
                finally
                {
                    rowIterator.close();
                }
            }

            ByteBuffer digest = ByteBufferUtil.readWithVIntLength(in);
            if (digest.hasRemaining())
                return new DigestResponse(digest);

            assert version == MessagingService.VERSION_30;
            ByteBuffer data = ByteBufferUtil.readWithVIntLength(in);
            return new DataResponse(data);
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
                    assert !(response instanceof LegacyRemoteDataResponse); // we only use those on the receiving side
                    try (UnfilteredPartitionIterator iter = response.makeIterator(response.metadata, null))
                    {
                        assert iter.hasNext();
                        try (UnfilteredRowIterator partition = iter.next())
                        {
                            size += ByteBufferUtil.serializedSizeWithShortLength(partition.partitionKey().getKey());
                            size += LegacyLayout.serializedSizeAsLegacyPartition(partition, version);
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

    private static class LegacyRangeSliceReplySerializer implements IVersionedSerializer<ReadResponse>
    {
        public void serialize(ReadResponse response, DataOutputPlus out, int version) throws IOException
        {
            assert version < MessagingService.VERSION_30;

            // determine the number of partitions upfront for serialization
            int numPartitions = 0;
            assert !(response instanceof LegacyRemoteDataResponse); // we only use those on the receiving side
            try (UnfilteredPartitionIterator iterator = response.makeIterator(response.metadata, null))
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

            try (UnfilteredPartitionIterator iterator = response.makeIterator(response.metadata, null))
            {
                while (iterator.hasNext())
                {
                    try (UnfilteredRowIterator partition = iterator.next())
                    {
                        ByteBufferUtil.writeWithShortLength(partition.partitionKey().getKey(), out);
                        LegacyLayout.serializeAsLegacyPartition(partition, out, version);
                    }
                }
            }
        }

        public ReadResponse deserialize(DataInputPlus in, int version) throws IOException
        {
            // Contrarily to serialize, we have to read the number of serialized partitions here.
            int partitionCount = in.readInt();
            ArrayList<ArrayBackedPartition> partitions = new ArrayList<>(partitionCount);
            for (int i = 0; i < partitionCount; i++)
            {
                ByteBuffer key = ByteBufferUtil.readWithShortLength(in);
                try (UnfilteredRowIterator partition = LegacyLayout.deserializeLegacyPartition(in, version, SerializationHelper.Flag.FROM_REMOTE, key))
                {
                    partitions.add(ArrayBackedPartition.create(partition));
                }
            }
            return new LegacyRemoteDataResponse(partitions);
        }

        public long serializedSize(ReadResponse response, int version)
        {
            assert version < MessagingService.VERSION_30;
            long size = TypeSizes.sizeof(0);  // number of partitions

            assert !(response instanceof LegacyRemoteDataResponse); // we only use those on the receiving side
            try (UnfilteredPartitionIterator iterator = response.makeIterator(response.metadata, null))
            {
                while (iterator.hasNext())
                {
                    try (UnfilteredRowIterator partition = iterator.next())
                    {
                        size += ByteBufferUtil.serializedSizeWithShortLength(partition.partitionKey().getKey());
                        size += LegacyLayout.serializedSizeAsLegacyPartition(partition, version);
                    }
                }
            }
            return size;
        }
    }
}
