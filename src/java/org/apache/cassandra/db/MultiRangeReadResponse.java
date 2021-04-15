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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.NoSuchElementException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.AbstractUnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.db.rows.DeserializationHelper;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * It's used to store response of multi-range read request from a given endpoint,
 * {@link ReadResponse} of subrange can be extracted via {@link #subrangeResponse(MultiRangeReadCommand, AbstractBounds)};
 */
public abstract class MultiRangeReadResponse extends ReadResponse
{
    protected static final Logger logger = LoggerFactory.getLogger(MultiRangeReadResponse.class);

    public static final IVersionedSerializer<ReadResponse> serializer = new Serializer();

    private MultiRangeReadResponse()
    {
    }

    /**
     * @param data results of multiple ranges
     * @param command current multi-range read command
     * @return multi-range read response
     */
    static ReadResponse createDataResponse(UnfilteredPartitionIterator data, MultiRangeReadCommand command)
    {
        return new LocalDataResponse(data, command);
    }

    /**
     * @param command current multi-range read command
     * @param range target subrange
     * @return response corresponding to the given range
     */
    public abstract ReadResponse subrangeResponse(MultiRangeReadCommand command, AbstractBounds<PartitionPosition> range);

    @Override
    public ByteBuffer digest(ReadCommand command)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isDigestResponse()
    {
        return false;
    }

    @Override
    public ByteBuffer repairedDataDigest()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isRepairedDigestConclusive()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean mayIncludeRepairedDigest()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toDebugString(ReadCommand command, DecoratedKey key)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * A local response that is not meant to be serialized or used for caching remote endpoint's multi-range response.
     */
    private static class LocalResponse extends MultiRangeReadResponse
    {
        private final RangeBoundPartitionIterator iterator;

        LocalResponse(UnfilteredPartitionIterator response)
        {
            this.iterator = new RangeBoundPartitionIterator(response);
        }

        @Override
        public UnfilteredPartitionIterator makeIterator(ReadCommand command)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public ReadResponse subrangeResponse(MultiRangeReadCommand command, AbstractBounds<PartitionPosition> range)
        {
            // deliver already cached content without deserialization.
            return new LocalSubrangeResponse(iterator, range);
        }

        class RangeBoundPartitionIterator
        {
            private final UnfilteredPartitionIterator iterator;
            private UnfilteredRowIterator next = null;

            RangeBoundPartitionIterator(UnfilteredPartitionIterator iterator)
            {
                this.iterator = iterator;
            }

            public boolean hasNext(AbstractBounds<PartitionPosition> range)
            {
                if (next != null)
                    return range.contains(next.partitionKey());

                if (iterator.hasNext())
                {
                    next = iterator.next();
                    if (range.contains(next.partitionKey()))
                        return true;
                }
                return false;
            }

            public UnfilteredRowIterator next()
            {
                if (next != null)
                {
                    UnfilteredRowIterator result = next;
                    next = null;
                    return result;
                }
                throw new NoSuchElementException();
            }
        }
    }

    private static class LocalSubrangeResponse extends ReadResponse
    {
        private final LocalResponse.RangeBoundPartitionIterator iterator;
        private final AbstractBounds<PartitionPosition> range;

        LocalSubrangeResponse(LocalResponse.RangeBoundPartitionIterator iterator, AbstractBounds<PartitionPosition> range)
        {
            this.iterator = iterator;
            this.range = range;
        }

        @Override
        public UnfilteredPartitionIterator makeIterator(ReadCommand command)
        {
            return new AbstractUnfilteredPartitionIterator()
            {
                @Override
                public TableMetadata metadata()
                {
                    return command.metadata();
                }

                @Override
                public boolean hasNext()
                {
                    return iterator.hasNext(range);
                }

                @Override
                public UnfilteredRowIterator next()
                {
                    return iterator.next();
                }
            };
        }

        @Override
        public ByteBuffer digest(ReadCommand command)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public ByteBuffer repairedDataDigest()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isRepairedDigestConclusive()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean mayIncludeRepairedDigest()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isDigestResponse()
        {
            return false;
        }
    }

    /**
     * A local response that needs to be serialized, i.e. sent to another node. The iterator
     * is serialized by the build method and can be closed as soon as this response has been created.
     */
    private static class LocalDataResponse extends DataResponse
    {
        private LocalDataResponse(UnfilteredPartitionIterator iterator, MultiRangeReadCommand command)
        {
            super(build(iterator, command.columnFilter()), MessagingService.current_version, DeserializationHelper.Flag.FROM_REMOTE);
        }

        private static ByteBuffer build(UnfilteredPartitionIterator iterator, ColumnFilter selection)
        {
            try (DataOutputBuffer buffer = new DataOutputBuffer())
            {
                UnfilteredPartitionIterators.serializerForIntraNode().serialize(iterator, selection, buffer, MessagingService.current_version);
                return buffer.buffer();
            }
            catch (IOException e)
            {
                // We're serializing in memory so this shouldn't happen
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * A response received from a remove node. We keep the response serialized in the byte buffer.
     */
    private static class RemoteDataResponse extends DataResponse
    {
        RemoteDataResponse(ByteBuffer data,
                           int dataSerializationVersion)
        {
            super(data, dataSerializationVersion, DeserializationHelper.Flag.FROM_REMOTE);
        }
    }

    /**
     * The command base class for local or remote responses that stay serialized in a byte buffer,
     * the data.
     */
    static abstract class DataResponse extends MultiRangeReadResponse
    {
        // The response, serialized in the current messaging version
        private final ByteBuffer data;
        private final int dataSerializationVersion;
        private final DeserializationHelper.Flag flag;

        private MultiRangeReadResponse.LocalResponse cached;

        DataResponse(ByteBuffer data,
                     int dataSerializationVersion,
                     DeserializationHelper.Flag flag)
        {
            this.data = data;
            this.dataSerializationVersion = dataSerializationVersion;
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
                                                                                         dataSerializationVersion,
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

        public ByteBuffer repairedDataDigest()
        {
            return ByteBufferUtil.EMPTY_BYTE_BUFFER;
        }

        @Override
        public boolean isRepairedDigestConclusive()
        {
            return true;
        }

        @Override
        public boolean mayIncludeRepairedDigest()
        {
            return dataSerializationVersion >= MessagingService.VERSION_40;
        }

        @Override
        public ReadResponse subrangeResponse(MultiRangeReadCommand command, AbstractBounds<PartitionPosition> range)
        {
            if (cached == null)
            {
                try (DataInputBuffer in = new DataInputBuffer(data, true))
                {
                    @SuppressWarnings("resource") // The close operation is a noop for a deserialized UPI
                    UnfilteredPartitionIterator iterator = UnfilteredPartitionIterators.serializerForIntraNode()
                                                                                       .deserialize(in,
                                                                                                    dataSerializationVersion,
                                                                                                    command.metadata(),
                                                                                                    command.columnFilter(),
                                                                                                    flag);
                    cached = new LocalResponse(iterator);
                }
                catch (IOException e)
                {
                    throw new RuntimeException(e);
                }
            }

            return cached.subrangeResponse(command, range);
        }
    }

    /**
     * A copy of {@code ReadResponse.Serializer} that doesn't support a digest response
     */
    private static class Serializer implements IVersionedSerializer<ReadResponse>
    {
        public void serialize(ReadResponse response, DataOutputPlus out, int version) throws IOException
        {
            ByteBuffer digest = ByteBufferUtil.EMPTY_BYTE_BUFFER;
            ByteBufferUtil.writeWithVIntLength(digest, out);
            if (version >= MessagingService.VERSION_40)
            {
                ByteBufferUtil.writeWithVIntLength(response.repairedDataDigest(), out);
                out.writeBoolean(response.isRepairedDigestConclusive());
            }
            ByteBuffer data = ((DataResponse)response).data;
            ByteBufferUtil.writeWithVIntLength(data, out);
        }

        public ReadResponse deserialize(DataInputPlus in, int version) throws IOException
        {
            ByteBuffer digest = ByteBufferUtil.readWithVIntLength(in);
            assert !digest.hasRemaining();

            if (version >= MessagingService.VERSION_40)
            {
                ByteBufferUtil.readWithVIntLength(in);
                in.readBoolean();
            }
            ByteBuffer data = ByteBufferUtil.readWithVIntLength(in);
            return new RemoteDataResponse(data, version);
        }

        public long serializedSize(ReadResponse response, int version)
        {
            ByteBuffer digest = ByteBufferUtil.EMPTY_BYTE_BUFFER;
            long size = ByteBufferUtil.serializedSizeWithVIntLength(digest);

            if (version >= MessagingService.VERSION_40)
            {
                size += ByteBufferUtil.serializedSizeWithVIntLength(response.repairedDataDigest());
                size += 1;
            }
            assert version >= MessagingService.VERSION_30;
            ByteBuffer data = ((DataResponse)response).data;
            size += ByteBufferUtil.serializedSizeWithVIntLength(data);
            return size;
        }
    }
}
