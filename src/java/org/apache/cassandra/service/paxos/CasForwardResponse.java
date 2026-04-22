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

import static org.apache.cassandra.db.SerializationHeader.StableHeaderSerializer.STABLE;
import static org.apache.cassandra.db.rows.DeserializationHelper.Flag.FROM_REMOTE;

/**
 * Response containing the result of a forwarded CAS operation.
 * Can contain either a successful result or an exception that occurred during execution.
 */
public class CasForwardResponse
{
    public final RowIterator result;
    public final CassandraException exception;

    @Nonnull
    public final List<String> warnings;

    public CasForwardResponse(RowIterator result, List<String> warnings)
    {
        this(result, null, warnings);
    }

    public CasForwardResponse(PartitionIterator result, List<String> warnings)
    {
        // Extract the single partition from the iterator (consensus reads are single partition)
        this(result != null && result.hasNext() ? result.next() : null, null, warnings);
    }

    public CasForwardResponse(CassandraException exception, List<String> warnings)
    {
        this(null, exception, warnings);
    }

    private CasForwardResponse(RowIterator result, CassandraException exception, List<String> warnings)
    {
        this.result = result;
        this.exception = exception;
        this.warnings = warnings == null ? Collections.emptyList() : warnings;
    }

    public boolean isSuccess()
    {
        return exception == null;
    }

    /**
     * Get the result as a PartitionIterator.
     */
    public PartitionIterator partitionIterator()
    {
        return result == null ? null : PartitionIterators.singletonIterator(result);
    }

    public static final Serializer serializer = new Serializer();

    public static class Serializer implements IVersionedSerializer<CasForwardResponse>
    {
        private static final int HAS_RESULT    = 0x01;
        private static final int HAS_EXCEPTION = 0x02;
        private static final int HAS_WARNINGS  = 0x04;

        @Override
        public void serialize(CasForwardResponse response, DataOutputPlus out, int version) throws IOException
        {
            int flags = (response.result != null ? HAS_RESULT : 0)
                      | (response.exception != null ? HAS_EXCEPTION : 0)
                      | (!response.warnings.isEmpty() ? HAS_WARNINGS : 0)
                      ;
            out.write(flags);

            if (response.result != null)
            {
                FilteredPartition partition = new FilteredPartition(response.result);
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

            RowIterator result = null;
            if (hasResult)
            {
                TableMetadata metadata = Schema.instance.getExistingTableMetadata(TableId.deserializeCompact(in));
                UnfilteredRowIteratorSerializer.Header header = UnfilteredRowIteratorSerializer.serializer.deserializeHeader(metadata, in, version, FROM_REMOTE, STABLE, null);
                try (UnfilteredRowIterator partition = UnfilteredRowIteratorSerializer.serializer.deserialize(in, version, metadata, FROM_REMOTE, header))
                {
                    result = UnfilteredRowIterators.filter(partition, 0);
                }
            }

            CassandraException exception = null;
            if (hasException)
                exception = CassandraException.serializer.deserialize(in, version);

            List<String> warnings = Collections.emptyList();
            if (hasWarnings)
                warnings = CollectionSerializers.deserializeList(in, version, StringSerializer.instance);

            return new CasForwardResponse(result, exception, warnings);
        }

        @Override
        public long serializedSize(CasForwardResponse response, int version)
        {
            long size = TypeSizes.BYTE_SIZE; // flags byte

            if (response.result != null)
            {
                FilteredPartition partition = new FilteredPartition(response.result);
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
