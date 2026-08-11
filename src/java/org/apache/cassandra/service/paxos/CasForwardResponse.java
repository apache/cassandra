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

package org.apache.cassandra.service.paxos;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import javax.annotation.Nonnull;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.partitions.FilteredPartition;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.PartitionIterators;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIteratorSerializer;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.exceptions.CassandraException;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.CollectionSerializers;
import org.apache.cassandra.utils.StringSerializer;

import static com.google.common.base.Preconditions.checkState;
import static org.apache.cassandra.db.SerializationHeader.StableHeaderSerializer.STABLE;
import static org.apache.cassandra.db.rows.DeserializationHelper.Flag.FROM_REMOTE;

/**
 * Response containing the result of a forwarded CAS operation.
 * Can contain either a successful result or an exception that occurred during execution.
 */
public class CasForwardResponse
{
    private final FilteredPartition result;

    /**
     * Direction {@link #result} was read in. {@link FilteredPartition} always stores rows in clustering
     * order, so a reversed slice needs its direction carried alongside.
     */
    private final boolean reversed;

    public final CassandraException exception;

    @Nonnull
    public final List<String> warnings;

    public CasForwardResponse(RowIterator result, List<String> warnings)
    {
        this(Materialized.of(result), null, warnings);
    }

    public CasForwardResponse(PartitionIterator result, List<String> warnings)
    {
        this(Materialized.of(result), null, warnings);
    }

    public CasForwardResponse(CassandraException exception, List<String> warnings)
    {
        this(Materialized.NONE, exception, warnings);
    }

    private CasForwardResponse(Materialized result, CassandraException exception, List<String> warnings)
    {
        this(result.partition, result.reversed, exception, warnings);
    }

    private CasForwardResponse(FilteredPartition result, boolean reversed, CassandraException exception, List<String> warnings)
    {
        this.result = result;
        this.reversed = reversed;
        this.exception = exception;
        this.warnings = warnings == null ? Collections.emptyList() : warnings;
    }

    /** A materialized result and the direction it was read in, taken before the iterator is consumed. */
    private static class Materialized
    {
        private static final Materialized NONE = new Materialized(null, false);

        private final FilteredPartition partition;
        private final boolean reversed;

        private Materialized(FilteredPartition partition, boolean reversed)
        {
            this.partition = partition;
            this.reversed = reversed;
        }

        private static Materialized of(RowIterator rows)
        {
            if (rows == null)
                return NONE;

            try (RowIterator toClose = rows)
            {
                boolean reversed = toClose.isReverseOrder();
                return new Materialized(new FilteredPartition(toClose), reversed);
            }
        }

        private static Materialized of(PartitionIterator partitions)
        {
            if (partitions == null)
                return NONE;

            try (PartitionIterator toClose = partitions)
            {
                if (!toClose.hasNext())
                    return NONE;

                Materialized materialized = of(toClose.next());
                // Serial reads are single partition, enforced in StorageProxy.readWithConsensusInternal.
                // Asked only after the partition above is drained, per the note in PartitionIterators.
                checkState(!toClose.hasNext(), "Forwarded read response cannot carry more than one partition");
                return materialized;
            }
        }
    }

    public boolean isSuccess()
    {
        return exception == null;
    }

    public boolean hasResult()
    {
        return result != null;
    }

    public RowIterator rowIterator()
    {
        return result == null ? null : result.rowIterator(reversed);
    }

    public PartitionIterator partitionIterator()
    {
        RowIterator rows = rowIterator();
        return rows == null ? null : PartitionIterators.singletonIterator(rows);
    }

    public static final Serializer serializer = new Serializer();

    public static class Serializer implements IVersionedSerializer<CasForwardResponse>
    {
        private static final int HAS_RESULT    = 0x01;
        private static final int HAS_EXCEPTION = 0x02;
        private static final int HAS_WARNINGS  = 0x04;
        /** Rows are always written in clustering order, so this records the direction asked for. */
        private static final int IS_REVERSED   = 0x08;

        @Override
        public void serialize(CasForwardResponse response, DataOutputPlus out, int version) throws IOException
        {
            int flags = (response.hasResult() ? HAS_RESULT : 0)
                      | (response.exception != null ? HAS_EXCEPTION : 0)
                      | (!response.warnings.isEmpty() ? HAS_WARNINGS : 0)
                      | (response.reversed ? IS_REVERSED : 0)
                      ;
            out.write(flags);

            if (response.hasResult())
            {
                FilteredPartition partition = response.result;
                partition.metadata().id.serializeCompact(out);
                try (UnfilteredRowIterator iterator = partition.unfilteredIterator())
                {
                    UnfilteredRowIteratorSerializer.serializer.serialize(iterator, out, version, partition.rowCount(), STABLE, null);
                }
            }

            if (response.exception != null)
                CassandraException.serializer.serialize(response.exception, out, version);

            if (!response.warnings.isEmpty())
                CollectionSerializers.serializeList(response.warnings, out, version, StringSerializer.instance);
        }

        @Override
        public CasForwardResponse deserialize(DataInputPlus in, int version) throws IOException
        {
            int flags = in.readUnsignedByte();
            boolean hasResult    = (flags & HAS_RESULT)    != 0;
            boolean hasException = (flags & HAS_EXCEPTION) != 0;
            boolean hasWarnings  = (flags & HAS_WARNINGS)  != 0;
            boolean reversed     = (flags & IS_REVERSED)   != 0;

            FilteredPartition result = null;
            if (hasResult)
            {
                TableMetadata metadata = Schema.instance.getExistingTableMetadata(TableId.deserializeCompact(in));
                UnfilteredRowIteratorSerializer.Header header = UnfilteredRowIteratorSerializer.serializer.deserializeHeader(metadata, in, version, FROM_REMOTE, STABLE, null);
                try (UnfilteredRowIterator partition = UnfilteredRowIteratorSerializer.serializer.deserialize(in, version, metadata, FROM_REMOTE, header))
                {
                    // Materialise inside the block: the deserialized iterator reads lazily from `in`,
                    // so it has to be drained here — both so the result outlives the iterator, and so
                    // the stream is positioned past the partition for the fields that follow.
                    result = new FilteredPartition(UnfilteredRowIterators.filter(partition, 0));
                }
            }

            CassandraException exception = null;
            if (hasException)
                exception = CassandraException.serializer.deserialize(in, version);

            List<String> warnings = Collections.emptyList();
            if (hasWarnings)
                warnings = CollectionSerializers.deserializeList(in, version, StringSerializer.instance);

            return new CasForwardResponse(result, reversed, exception, warnings);
        }

        @Override
        public long serializedSize(CasForwardResponse response, int version)
        {
            long size = TypeSizes.BYTE_SIZE; // flags byte

            if (response.hasResult())
            {
                FilteredPartition partition = response.result;
                size += partition.metadata().id.serializedCompactSize();
                try (UnfilteredRowIterator iterator = partition.unfilteredIterator())
                {
                    size += UnfilteredRowIteratorSerializer.serializer.serializedSize(iterator, version, partition.rowCount(), STABLE, null);
                }
            }

            if (response.exception != null)
                size += CassandraException.serializer.serializedSize(response.exception, version);

            if (!response.warnings.isEmpty())
                size += CollectionSerializers.serializedListSize(response.warnings, version, StringSerializer.instance);

            return size;
        }
    }
}
