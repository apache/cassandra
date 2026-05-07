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
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.AbstractUnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.ImmutableBTreePartition;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.db.rows.AbstractUnfilteredRowIterator;
import org.apache.cassandra.db.rows.DeserializationHelper;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIteratorSerializer;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ExpMovingAverage;
import org.apache.cassandra.utils.MovingAverage;

import static org.apache.cassandra.db.RepairedDataInfo.NO_OP_REPAIRED_DATA_INFO;

public abstract class ReadResponse
{
    // Serializer for single partition read response
    public static final IVersionedSerializer<ReadResponse> serializer = new Serializer();

    static final int IN_MEMORY_MAX_ROWS = CassandraRelevantProperties.DATA_RESPONSE_IN_MEMORY_MAX_ROWS.getInt();

    protected ReadResponse()
    {
    }

    public static ReadResponse createDataResponse(UnfilteredPartitionIterator data, ReadCommand command, RepairedDataInfo rdi)
    {
        return new LocalDataResponse(data, command, rdi);
    }

    public static ReadResponse createDataResponse(UnfilteredPartitionIterator data, ReadCommand command)
    {
        return new LocalDataResponse(data, command, NO_OP_REPAIRED_DATA_INFO);
    }

    static ReadResponse createInMemoryDataResponse(UnfilteredPartitionIterator data, ReadCommand command, RepairedDataInfo rdi)
    {
        return createInMemoryDataResponse(data, command, rdi, IN_MEMORY_MAX_ROWS);
    }

    @VisibleForTesting
    static ReadResponse createInMemoryDataResponse(UnfilteredPartitionIterator data, ReadCommand command, RepairedDataInfo rdi, int maxRows)
    {
        return new InMemoryDataResponse(data, command, rdi, maxRows);
    }

    @VisibleForTesting
    public static ReadResponse createRemoteDataResponse(UnfilteredPartitionIterator data,
                                                        ByteBuffer repairedDataDigest,
                                                        boolean isRepairedDigestConclusive,
                                                        ReadCommand command,
                                                        int version)
    {
        return new RemoteDataResponse(LocalDataResponse.build(data, command.columnFilter()),
                                      repairedDataDigest,
                                      isRepairedDigestConclusive,
                                      version);
    }


    public static ReadResponse createDigestResponse(UnfilteredPartitionIterator data, ReadCommand command)
    {
        return new DigestResponse(makeDigest(data, command));
    }

    public abstract UnfilteredPartitionIterator makeIterator(ReadCommand command);
    public abstract ByteBuffer digest(ReadCommand command);
    public abstract ByteBuffer repairedDataDigest();
    public abstract boolean isRepairedDigestConclusive();
    public abstract boolean mayIncludeRepairedDigest();

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
                        return toDebugString(partition, command.metadata());
                }
            }
        }
        return String.format("<key %s not found (repaired_digest=%s repaired_digest_conclusive=%s)>",
                             key, ByteBufferUtil.bytesToHex(repairedDataDigest()), isRepairedDigestConclusive());
    }

    private String toDebugString(UnfilteredRowIterator partition, TableMetadata metadata)
    {
        StringBuilder sb = new StringBuilder();

        sb.append(String.format("[%s] key=%s partition_deletion=%s columns=%s repaired_digest=%s repaired_digest_conclusive==%s",
                                metadata,
                                metadata.partitionKeyType.getString(partition.partitionKey().getKey()),
                                partition.partitionLevelDeletion(),
                                partition.columns(),
                                ByteBufferUtil.bytesToHex(repairedDataDigest()),
                                isRepairedDigestConclusive()
                                ));

        if (partition.staticRow() != Rows.EMPTY_STATIC_ROW)
            sb.append("\n    ").append(partition.staticRow().toString(metadata, true));

        while (partition.hasNext())
            sb.append("\n    ").append(partition.next().toString(metadata, true));

        return sb.toString();
    }

    /**
     * For range reads Accord generates multiple responses per node because each command store executes
     * the reads independently. The responses are already sorted in token order so the iterators or digests can be
     * merged and still produce a consistent result across different nodes.
     *
     * This can *only* be called from the node producing the results not the coordinator because isEmptyDigest is
     * not serialized
     */
    public static ReadResponse merge(List<ReadResponse> responses, ReadCommand command)
    {
        if (responses.get(0).isDigestResponse())
        {
            Digest digest = Digest.forReadResponse();
            for (ReadResponse response : responses)
                digest.update(((DigestResponse)response).digest);
            return new DigestResponse(ByteBuffer.wrap(digest.digest()));
        }
        else
        {
            List<UnfilteredPartitionIterator> iterators = new ArrayList<>(responses.size());
            for (ReadResponse response : responses)
                iterators.add(response.makeIterator(command));

            // Range responses will not respect the limit because each command store returns a separate response
            // so we effectively deserialize and then reserialize in order to apply the limits
            // Wasteful, but better than sending it to the coordinator to do it
            UnfilteredPartitionIterator filtered = command.limits().filter(UnfilteredPartitionIterators.concat(iterators), 0, command.selectsFullPartition());
            return new LocalDataResponse(filtered, command, NO_OP_REPAIRED_DATA_INFO);
        }
    }

    protected static ByteBuffer makeDigest(UnfilteredPartitionIterator iterator, ReadCommand command)
    {
        Digest digest = Digest.forReadResponse();
        UnfilteredPartitionIterators.digest(iterator, digest, command.digestVersion());
        return ByteBuffer.wrap(digest.digest());
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

        public boolean mayIncludeRepairedDigest()
        {
            return false;
        }

        public ByteBuffer repairedDataDigest()
        {
            throw new UnsupportedOperationException();
        }

        public boolean isRepairedDigestConclusive()
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
        // Exponential moving average of response sizes, used to set initial size of output buffer.
        private static final MovingAverage estimatedResponseBytes = ExpMovingAverage.decayBy1000();

        // Exponential moving average of overflow(!) sizes, used to set initial size of output buffer.
        private static final MovingAverage estimatedOverflowBytes = ExpMovingAverage.decayBy1000();
        private static final int bufferInitialSizeMin = CassandraRelevantProperties.DATA_RESPONSE_BUFFER_INITIAL_SIZE_MIN.getInt();
        private static final int bufferInitialSizeMax = CassandraRelevantProperties.DATA_RESPONSE_BUFFER_INITIAL_SIZE_MAX.getInt();

        private LocalDataResponse(UnfilteredPartitionIterator iter, ReadCommand command, RepairedDataInfo rdi)
        {
            super(build(iter, command.columnFilter()),
                  rdi.getDigest(), rdi.isConclusive(),
                  MessagingService.current_version,
                  DeserializationHelper.Flag.LOCAL);
        }

        private static ByteBuffer buildOverflow(UnfilteredRowIterator rowIter, ColumnFilter selection)
        {
            int initialBufferSize = bufferInitialSizeMin;

            if (bufferInitialSizeMax > bufferInitialSizeMin)
            {
                double estimatedOverflowSize = estimatedOverflowBytes.get();
                double bufferSizeEstimate = Double.isNaN(estimatedOverflowSize) ? bufferInitialSizeMin : estimatedOverflowSize * 1.1;
                initialBufferSize = Math.min((int) bufferSizeEstimate, bufferInitialSizeMax);
            }

            try (DataOutputBuffer buffer = new DataOutputBuffer(initialBufferSize))
            {
                // NOTE: we use UnfilteredRowIteratorSerializer here, not UnfilteredPartitionIterators
                UnfilteredRowIteratorSerializer.serializer.serialize(rowIter, selection, buffer, MessagingService.current_version);
                estimatedOverflowBytes.update(buffer.position());
                return buffer.buffer(false);
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }

        private static ByteBuffer build(UnfilteredPartitionIterator iter, ColumnFilter selection)
        {
            int initialBufferSize = bufferInitialSizeMin;

            if (bufferInitialSizeMax > bufferInitialSizeMin)
            {
                // Size output buffer to 10% above the moving average to absorb minor variance and limit rebuffering.
                double estimatedResponseSize = estimatedResponseBytes.get();
                double bufferSizeEstimate = Double.isNaN(estimatedResponseSize) ? bufferInitialSizeMin : estimatedResponseSize * 1.1;
                initialBufferSize = Math.min((int) bufferSizeEstimate, bufferInitialSizeMax);
            }

            try (DataOutputBuffer buffer = new DataOutputBuffer(initialBufferSize))
            {
                UnfilteredPartitionIterators.serializerForIntraNode().serialize(iter, selection, buffer, MessagingService.current_version);
                estimatedResponseBytes.update(buffer.position());
                return buffer.buffer(false);
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
        protected RemoteDataResponse(ByteBuffer data,
                                     ByteBuffer repairedDataDigest,
                                     boolean isRepairedDigestConclusive,
                                     int version)
        {
            super(data, repairedDataDigest, isRepairedDigestConclusive, version, DeserializationHelper.Flag.FROM_REMOTE);
        }
    }

    static abstract class DataResponse extends ReadResponse
    {
        // TODO: can the digest be calculated over the raw bytes now?
        // The response, serialized in the current messaging version
        private final ByteBuffer data;
        protected ByteBuffer repairedDataDigest;
        protected boolean isRepairedDigestConclusive;
        private final int dataSerializationVersion;
        private final DeserializationHelper.Flag flag;

        protected DataResponse(ByteBuffer data,
                               ByteBuffer repairedDataDigest,
                               boolean isRepairedDigestConclusive,
                               int dataSerializationVersion,
                               DeserializationHelper.Flag flag)
        {
            super();
            this.data = data;
            this.repairedDataDigest = repairedDataDigest;
            this.isRepairedDigestConclusive = isRepairedDigestConclusive;
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

        public boolean mayIncludeRepairedDigest()
        {
            return dataSerializationVersion >= MessagingService.VERSION_40;
        }

        public ByteBuffer repairedDataDigest()
        {
            return repairedDataDigest;
        }

        public boolean isRepairedDigestConclusive()
        {
            return isRepairedDigestConclusive;
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

        protected ByteBuffer getSerializedData()
        {
            return data;
        }
    }

    // built on the coordinator for local single-partition reads; never sent over the network
    private static class InMemoryDataResponse extends DataResponse
    {
        // rows up to the max-rows limit stored as an in-memory partition; null if the response was empty
        private final ImmutableBTreePartition partition;
        // rows beyond the max-rows limit serialized to a buffer; null if all rows fit in memory
        private final ByteBuffer overflow;

        private InMemoryDataResponse(UnfilteredPartitionIterator iter, ReadCommand command, RepairedDataInfo rdi, int maxRows)
        {
            // pass fake values for rdi.getDigest(), rdi.isConclusive(), they will be calculated and set later
            super(null, null, false, MessagingService.current_version, DeserializationHelper.Flag.LOCAL);

            if (!iter.hasNext())
            {
                this.partition = null;
                this.overflow = null;
            }
            else
            {
                try (UnfilteredRowIterator rowIter = iter.next())
                {
                    LimitedUnfilteredRowIterator limitedIter = new LimitedUnfilteredRowIterator(rowIter, maxRows);
                    this.partition = ImmutableBTreePartition.create(limitedIter);
                    // Uses buildOverflow (row-iterator level) to match the UnfilteredRowIteratorSerializer
                    // deserializer used in makeIterator.
                    this.overflow = limitedIter.hasOverflow()
                                    ? LocalDataResponse.buildOverflow(rowIter, command.columnFilter())
                                    : null;
                }
            }

            // Capture digest after consuming the iterator so that any updates made by
            // RepairedDataInfo transformations are reflected in the digest.
            this.repairedDataDigest = rdi.getDigest();
            this.isRepairedDigestConclusive = rdi.isConclusive();
        }

        // Decorating iterator that stops after maxRows Unfiltered objects and records whether overflow occurred.
        // Does NOT close the wrapped iterator on close() so the caller can continue reading overflow rows.
        private static class LimitedUnfilteredRowIterator extends AbstractUnfilteredRowIterator
        {
            private final UnfilteredRowIterator wrapped;
            private final int maxRows;
            private int rowCount = 0;
            private boolean hasOverflow = false;

            private LimitedUnfilteredRowIterator(UnfilteredRowIterator wrapped, int maxRows)
            {
                super(wrapped.metadata(),
                      wrapped.partitionKey(),
                      wrapped.partitionLevelDeletion(),
                      wrapped.columns(),
                      wrapped.staticRow(),
                      wrapped.isReverseOrder(),
                      wrapped.stats());
                this.wrapped = wrapped;
                this.maxRows = maxRows;
            }

            boolean hasOverflow()
            {
                return hasOverflow;
            }

            @Override
            protected Unfiltered computeNext()
            {
                if (rowCount >= maxRows)
                {
                    hasOverflow = wrapped.hasNext();
                    return endOfData();
                }
                if (!wrapped.hasNext())
                    return endOfData();
                rowCount++;
                return wrapped.next();
            }
        }

        @Override
        public UnfilteredPartitionIterator makeIterator(ReadCommand command)
        {
            if (partition == null)
                return EmptyIterators.unfilteredPartition(command.metadata());

            UnfilteredRowIterator inMemoryIter = partition.unfilteredIterator(command.columnFilter(), Slices.ALL, command.isReversed());

            if (overflow == null)
                return new SingletonUnfilteredPartitionIterator(command.metadata(), inMemoryIter);

            // Deserialize the overflow and concatenate with the in-memory part.
            // Buffer closing is not needed and actually is incorrect
            // because the result iterator uses the buffer context
            DataInputBuffer in = new DataInputBuffer(overflow, true);
            try
            {
                UnfilteredRowIterator overflowIter = UnfilteredRowIteratorSerializer.serializer.deserialize(
                        in, MessagingService.current_version, command.metadata(), command.columnFilter(), DeserializationHelper.Flag.LOCAL);
                UnfilteredRowIterator combined = UnfilteredRowIterators.concat(inMemoryIter, overflowIter);
                return new SingletonUnfilteredPartitionIterator(command.metadata(), combined);
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }

        @Override
        protected ByteBuffer getSerializedData()
        {
            throw new UnsupportedOperationException("InMemoryDataResponse cannot be serialized over the network");
        }
    }

    private static class SingletonUnfilteredPartitionIterator extends AbstractUnfilteredPartitionIterator
    {
        private final TableMetadata metadata;
        private final UnfilteredRowIterator partition;
        private boolean returned = false;

        private SingletonUnfilteredPartitionIterator(TableMetadata metadata, UnfilteredRowIterator partition)
        {
            this.metadata = metadata;
            this.partition = partition;
        }

        public TableMetadata metadata() { return metadata; }

        public boolean hasNext()
        {
            return !returned;
        }

        public UnfilteredRowIterator next()
        {
            if (returned) throw new NoSuchElementException();
            returned = true;
            return partition;
        }

        @Override
        public void close()
        {
            partition.close();
        }
    }

    private static class Serializer implements IVersionedSerializer<ReadResponse>
    {
        public void serialize(ReadResponse response, DataOutputPlus out, int version) throws IOException
        {
            assert version >= MessagingService.VERSION_40;
            boolean isDigest = response instanceof DigestResponse;
            ByteBuffer digest = isDigest ? ((DigestResponse)response).digest : ByteBufferUtil.EMPTY_BYTE_BUFFER;
            ByteBufferUtil.writeWithVIntLength(digest, out);
            if (!isDigest)
            {
                // From 4.0, a coordinator may request additional info about the repaired data that
                // makes up the response, namely a digest generated from the repaired data and a
                // flag indicating our level of confidence in that digest. The digest may be considered
                // inconclusive if it may have been affected by some unrepaired data during read.
                // e.g. some sstables read during this read were involved in pending but not yet
                // committed repair sessions or an unrepaired partition tombstone meant that not all
                // repaired sstables were read (but they might be on other replicas).
                // If the coordinator did not request this info, the response contains an empty digest
                // and a true for the isConclusive flag.
                ByteBufferUtil.writeWithVIntLength(response.repairedDataDigest(), out);
                out.writeBoolean(response.isRepairedDigestConclusive());

                ByteBuffer data = ((DataResponse)response).getSerializedData();
                ByteBufferUtil.writeWithVIntLength(data, out);
            }
        }

        public ReadResponse deserialize(DataInputPlus in, int version) throws IOException
        {
            assert version >= MessagingService.VERSION_40;
            ByteBuffer digest = ByteBufferUtil.readWithVIntLength(in);
            if (digest.hasRemaining())
                return new DigestResponse(digest);

            // A data response may also contain a digest of the portion of its payload
            // that comes from the replica's repaired set, along with a flag indicating
            // whether or not the digest may be influenced by unrepaired/pending
            // repaired data
            digest = ByteBufferUtil.readWithVIntLength(in);
            boolean repairedDigestConclusive = in.readBoolean();

            ByteBuffer data = ByteBufferUtil.readWithVIntLength(in);
            return new RemoteDataResponse(data, digest, repairedDigestConclusive, version);
        }

        public long serializedSize(ReadResponse response, int version)
        {
            assert version >= MessagingService.VERSION_40;
            boolean isDigest = response instanceof DigestResponse;
            ByteBuffer digest = isDigest ? ((DigestResponse)response).digest : ByteBufferUtil.EMPTY_BYTE_BUFFER;
            long size = ByteBufferUtil.serializedSizeWithVIntLength(digest);

            if (!isDigest)
            {
                // From 4.0, a coordinator may request an additional info about the repaired data
                // that makes up the response.
                size += ByteBufferUtil.serializedSizeWithVIntLength(response.repairedDataDigest());
                size += 1;

                // In theory, we should deserialize/re-serialize if the version asked is different from the current
                // version as the content could have a different serialization format. So far though, we haven't made
                // change to partition iterators serialization since 3.0 so we skip this.
                ByteBuffer data = ((DataResponse)response).getSerializedData();
                size += ByteBufferUtil.serializedSizeWithVIntLength(data);
            }
            return size;
        }
    }
}
