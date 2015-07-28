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
import java.util.List;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.partitions.*;
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

    public abstract UnfilteredPartitionIterator makeIterator(CFMetaData metadata);
    public abstract ByteBuffer digest(CFMetaData metadata);
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

        public UnfilteredPartitionIterator makeIterator(CFMetaData metadata)
        {
            throw new UnsupportedOperationException();
        }

        public ByteBuffer digest(CFMetaData metadata)
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

        public UnfilteredPartitionIterator makeIterator(CFMetaData metadata)
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

        public ByteBuffer digest(CFMetaData metadata)
        {
            try (UnfilteredPartitionIterator iterator = makeIterator(metadata))
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
            if (version < MessagingService.VERSION_30)
            {
                // TODO
                throw new UnsupportedOperationException();
            }

            boolean isDigest = response.isDigestQuery();
            ByteBufferUtil.writeWithShortLength(isDigest ? response.digest(response.metadata) : ByteBufferUtil.EMPTY_BYTE_BUFFER, out);
            if (!isDigest)
            {
                // Note that we can only get there if version == 3.0, which is the current_version. When we'll change the
                // version, we'll have to deserialize/re-serialize the data to be in the proper version.
                assert version == MessagingService.VERSION_30;
                ByteBuffer data = ((DataResponse)response).data;
                ByteBufferUtil.writeWithLength(data, out);
            }
        }

        public ReadResponse deserialize(DataInputPlus in, int version) throws IOException
        {
            if (version < MessagingService.VERSION_30)
            {
                // TODO
                throw new UnsupportedOperationException();
            }

            ByteBuffer digest = ByteBufferUtil.readWithShortLength(in);
            if (digest.hasRemaining())
                return new DigestResponse(digest);

            assert version == MessagingService.VERSION_30;
            ByteBuffer data = ByteBufferUtil.readWithLength(in);
            return new DataResponse(data);
        }

        public long serializedSize(ReadResponse response, int version)
        {
            if (version < MessagingService.VERSION_30)
            {
                // TODO
                throw new UnsupportedOperationException();
            }

            boolean isDigest = response.isDigestQuery();
            long size = ByteBufferUtil.serializedSizeWithShortLength(isDigest ? response.digest(response.metadata) : ByteBufferUtil.EMPTY_BYTE_BUFFER);

            if (!isDigest)
            {
                // Note that we can only get there if version == 3.0, which is the current_version. When we'll change the
                // version, we'll have to deserialize/re-serialize the data to be in the proper version.
                assert version == MessagingService.VERSION_30;
                ByteBuffer data = ((DataResponse)response).data;
                size += ByteBufferUtil.serializedSizeWithLength(data);
            }
            return size;
        }
    }

    private static class LegacyRangeSliceReplySerializer implements IVersionedSerializer<ReadResponse>
    {
        public void serialize(ReadResponse response, DataOutputPlus out, int version) throws IOException
        {
            // TODO
            throw new UnsupportedOperationException();
            //        out.writeInt(rsr.rows.size());
            //        for (Row row : rsr.rows)
            //            Row.serializer.serialize(row, out, version);
        }

        public ReadResponse deserialize(DataInputPlus in, int version) throws IOException
        {
            // TODO
            throw new UnsupportedOperationException();
            //        int rowCount = in.readInt();
            //        List<Row> rows = new ArrayList<Row>(rowCount);
            //        for (int i = 0; i < rowCount; i++)
            //            rows.add(Row.serializer.deserialize(in, version));
            //        return new RangeSliceReply(rows);
        }

        public long serializedSize(ReadResponse response, int version)
        {
            // TODO
            throw new UnsupportedOperationException();
            //        int size = TypeSizes.sizeof(rsr.rows.size());
            //        for (Row row : rsr.rows)
            //            size += Row.serializer.serializedSize(row, version);
            //        return size;
        }
    }
}
