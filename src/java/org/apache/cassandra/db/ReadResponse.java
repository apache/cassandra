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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.hash.Hasher;

import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.HashingUtils;

public abstract class ReadResponse
{
    // Serializer for single partition read response
    public static final IVersionedSerializer<ReadResponse> serializer = new Serializer();

    protected ReadResponse()
    {
    }

    public static ReadResponse createDataResponse(UnfilteredPartitionIterator data, ReadCommand command)
    {
        return new LocalDataResponse(data, command);
    }

    @VisibleForTesting
    public static ReadResponse createRemoteDataResponse(UnfilteredPartitionIterator data, ReadCommand command)
    {
        return new RemoteDataResponse(LocalDataResponse.build(data, command.columnFilter()), MessagingService.current_version);
    }

    public static ReadResponse createDigestResponse(UnfilteredPartitionIterator data, ReadCommand command)
    {
        return new DigestResponse(makeDigest(data, command));
    }

    public abstract UnfilteredPartitionIterator makeIterator(ReadCommand command);
    public abstract ByteBuffer digest(ReadCommand command);

    public abstract boolean isDigestResponse();

    /**
     * Creates a string of the requested partition in this read response suitable for debugging.
     */
    public String toDebugString(ReadCommand command, DecoratedKey key)
    {
        if (isDigestResponse())
            return "Digest:0x" + ByteBufferUtil.bytesToHex(digest(command));

        try (UnfilteredPartitionIterator iter = makeIterator(command))
        {
            while (iter.hasNext())
            {
                try (UnfilteredRowIterator partition = iter.next())
                {
                    if (partition.partitionKey().equals(key))
                        return ImmutableBTreePartition.create(partition).toString();
                }
            }
        }
        return "<key " + key + " not found>";
    }

    protected static ByteBuffer makeDigest(UnfilteredPartitionIterator iterator, ReadCommand command)
    {
        Hasher hasher = HashingUtils.CURRENT_HASH_FUNCTION.newHasher();
        UnfilteredPartitionIterators.digest(iterator, hasher, command.digestVersion());
        return ByteBuffer.wrap(hasher.hash().asBytes());
    }

    private static class DigestResponse extends ReadResponse
    {
        private final ByteBuffer digest;

        private DigestResponse(ByteBuffer digest)
        {
            super();
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
            super(build(iter, command.columnFilter()), MessagingService.current_version, SerializationHelper.Flag.LOCAL);
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
        protected RemoteDataResponse(ByteBuffer data, int version)
        {
            super(data, version, SerializationHelper.Flag.FROM_REMOTE);
        }
    }

    static abstract class DataResponse extends ReadResponse
    {
        // TODO: can the digest be calculated over the raw bytes now?
        // The response, serialized in the current messaging version
        private final ByteBuffer data;
        private final int dataSerializationVersion;
        private final SerializationHelper.Flag flag;

        protected DataResponse(ByteBuffer data, int dataSerializationVersion, SerializationHelper.Flag flag)
        {
            super();
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

            ByteBufferUtil.writeWithVIntLength(digest, out);
            if (!isDigest)
            {
                ByteBuffer data = ((DataResponse)response).data;
                ByteBufferUtil.writeWithVIntLength(data, out);
            }
        }

        public ReadResponse deserialize(DataInputPlus in, int version) throws IOException
        {
            ByteBuffer digest = ByteBufferUtil.readWithVIntLength(in);
            if (digest.hasRemaining())
                return new DigestResponse(digest);

            ByteBuffer data = ByteBufferUtil.readWithVIntLength(in);
            return new RemoteDataResponse(data, version);
        }

        public long serializedSize(ReadResponse response, int version)
        {
            boolean isDigest = response instanceof DigestResponse;
            ByteBuffer digest = isDigest ? ((DigestResponse)response).digest : ByteBufferUtil.EMPTY_BYTE_BUFFER;

            long size = ByteBufferUtil.serializedSizeWithVIntLength(digest);
            if (!isDigest)
            {
                // In theory, we should deserialize/re-serialize if the version asked is different from the current
                // version as the content could have a different serialization format. So far though, we haven't made
                // change to partition iterators serialization since 3.0 so we skip this.
                assert version >= MessagingService.VERSION_30;
                ByteBuffer data = ((DataResponse)response).data;
                size += ByteBufferUtil.serializedSizeWithVIntLength(data);
            }
            return size;
        }
    }
}
