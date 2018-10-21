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
package org.apache.cassandra.audit;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;

import io.netty.buffer.ByteBuf;
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.wire.ValueOut;
import net.openhft.chronicle.wire.WireOut;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.CBUtil;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.binlog.BinLog;
import org.apache.cassandra.utils.concurrent.WeightedQueue;
import org.github.jamm.MemoryLayoutSpecification;

/**
 * A logger that logs entire query contents after the query finishes (or times out).
 */
public class FullQueryLogger extends BinLogAuditLogger implements IAuditLogger
{
    public static final long CURRENT_VERSION = 0; // encode a dummy version, to prevent pain in decoding in the future

    public static final String VERSION = "version";
    public static final String TYPE = "type";

    public static final String PROTOCOL_VERSION = "protocol-version";
    public static final String QUERY_OPTIONS = "query-options";
    public static final String QUERY_START_TIME = "query-start-time";

    public static final String GENERATED_TIMESTAMP = "generated-timestamp";
    public static final String GENERATED_NOW_IN_SECONDS = "generated-now-in-seconds";
    public static final String KEYSPACE = "keyspace";

    public static final String BATCH = "batch";
    public static final String SINGLE_QUERY = "single-query";

    public static final String QUERY = "query";
    public static final String BATCH_TYPE = "batch-type";
    public static final String QUERIES = "queries";
    public static final String VALUES = "values";

    private static final int EMPTY_BYTEBUFFER_SIZE = Ints.checkedCast(ObjectSizes.sizeOnHeapExcludingData(ByteBuffer.allocate(0)));

    private static final int EMPTY_LIST_SIZE = Ints.checkedCast(ObjectSizes.measureDeep(new ArrayList(0)));
    private static final int EMPTY_BYTEBUF_SIZE;

    private static final int OBJECT_HEADER_SIZE = MemoryLayoutSpecification.SPEC.getObjectHeaderSize();
    private static final int OBJECT_REFERENCE_SIZE = MemoryLayoutSpecification.SPEC.getReferenceSize();

    static
    {
        ByteBuf buf = CBUtil.allocator.buffer(0, 0);
        try
        {
            EMPTY_BYTEBUF_SIZE = Ints.checkedCast(ObjectSizes.measure(buf));
        }
        finally
        {
            buf.release();
        }
    }

    @Override
    public void log(AuditLogEntry entry)
    {
        logQuery(entry.getOperation(), entry.getOptions(), entry.getState(), entry.getTimestamp());
    }

    /**
     * Log an invocation of a batch of queries
     * @param type The type of the batch
     * @param queries CQL text of the queries
     * @param values Values to bind to as parameters for the queries
     * @param queryOptions Options associated with the query invocation
     * @param queryState Timestamp state associated with the query invocation
     * @param batchTimeMillis Approximate time in milliseconds since the epoch since the batch was invoked
     */
    void logBatch(BatchStatement.Type type,
                  List<String> queries,
                  List<List<ByteBuffer>> values,
                  QueryOptions queryOptions,
                  QueryState queryState,
                  long batchTimeMillis)
    {
        Preconditions.checkNotNull(type, "type was null");
        Preconditions.checkNotNull(queries, "queries was null");
        Preconditions.checkNotNull(values, "value was null");
        Preconditions.checkNotNull(queryOptions, "queryOptions was null");
        Preconditions.checkNotNull(queryState, "queryState was null");
        Preconditions.checkArgument(batchTimeMillis > 0, "batchTimeMillis must be > 0");

        //Don't construct the wrapper if the log is disabled
        BinLog binLog = this.binLog;
        if (binLog == null)
        {
            return;
        }

        Batch wrappedBatch = new Batch(type, queries, values, queryOptions, queryState, batchTimeMillis);
        logRecord(wrappedBatch, binLog);
    }

    /**
     * Log a single CQL query
     * @param query CQL query text
     * @param queryOptions Options associated with the query invocation
     * @param queryState Timestamp state associated with the query invocation
     * @param queryTimeMillis Approximate time in milliseconds since the epoch since the batch was invoked
     */
    void logQuery(String query, QueryOptions queryOptions, QueryState queryState, long queryTimeMillis)
    {
        Preconditions.checkNotNull(query, "query was null");
        Preconditions.checkNotNull(queryOptions, "queryOptions was null");
        Preconditions.checkNotNull(queryState, "queryState was null");
        Preconditions.checkArgument(queryTimeMillis > 0, "queryTimeMillis must be > 0");

        //Don't construct the wrapper if the log is disabled
        BinLog binLog = this.binLog;
        if (binLog == null)
        {
            return;
        }

        Query wrappedQuery = new Query(query, queryOptions, queryState, queryTimeMillis);
        logRecord(wrappedQuery, binLog);
    }

    public static class Query extends AbstractLogEntry
    {
        private final String query;

        public Query(String query, QueryOptions queryOptions, QueryState queryState, long queryStartTime)
        {
            super(queryOptions, queryState, queryStartTime);
            this.query = query;
        }

        @Override
        protected String type()
        {
            return SINGLE_QUERY;
        }

        @Override
        public void writeMarshallable(WireOut wire)
        {
            super.writeMarshallable(wire);
            wire.write(QUERY).text(query);
        }

        @Override
        public int weight()
        {
            return Ints.checkedCast(ObjectSizes.sizeOf(query)) + super.weight();
        }
    }

    public static class Batch extends AbstractLogEntry
    {
        private final int weight;
        private final BatchStatement.Type batchType;
        private final List<String> queries;
        private final List<List<ByteBuffer>> values;

        public Batch(BatchStatement.Type batchType,
                     List<String> queries,
                     List<List<ByteBuffer>> values,
                     QueryOptions queryOptions,
                     QueryState queryState,
                     long batchTimeMillis)
        {
            super(queryOptions, queryState, batchTimeMillis);

            this.queries = queries;
            this.values = values;
            this.batchType = batchType;

            int weight = super.weight();

            // weight, queries, values, batch type
            weight += 4 +                    // cached weight
                      2 * EMPTY_LIST_SIZE +  // queries + values lists
                      OBJECT_REFERENCE_SIZE; // batchType reference, worst case

            for (String query : queries)
                weight += ObjectSizes.sizeOf(query);

            for (List<ByteBuffer> subValues : values)
            {
                weight += EMPTY_LIST_SIZE;
                for (ByteBuffer value : subValues)
                    weight += EMPTY_BYTEBUFFER_SIZE + value.capacity();
            }

            this.weight = weight;
        }

        @Override
        protected String type()
        {
            return BATCH;
        }

        @Override
        public void writeMarshallable(WireOut wire)
        {
            super.writeMarshallable(wire);
            wire.write(BATCH_TYPE).text(batchType.name());
            ValueOut valueOut = wire.write(QUERIES);
            valueOut.int32(queries.size());
            for (String query : queries)
            {
                valueOut.text(query);
            }
            valueOut = wire.write(VALUES);
            valueOut.int32(values.size());
            for (List<ByteBuffer> subValues : values)
            {
                valueOut.int32(subValues.size());
                for (ByteBuffer value : subValues)
                {
                    valueOut.bytes(BytesStore.wrap(value));
                }
            }
        }

        @Override
        public int weight()
        {
            return weight;
        }
    }

    private static abstract class AbstractLogEntry extends BinLog.ReleaseableWriteMarshallable implements WeightedQueue.Weighable
    {
        private final long queryStartTime;
        private final int protocolVersion;
        private final ByteBuf queryOptionsBuffer;

        private final long generatedTimestamp;
        private final int generatedNowInSeconds;
        @Nullable
        private final String keyspace;

        AbstractLogEntry(QueryOptions queryOptions, QueryState queryState, long queryStartTime)
        {
            this.queryStartTime = queryStartTime;

            this.protocolVersion = queryOptions.getProtocolVersion().asInt();
            int optionsSize = QueryOptions.codec.encodedSize(queryOptions, queryOptions.getProtocolVersion());
            queryOptionsBuffer = CBUtil.allocator.buffer(optionsSize, optionsSize);

            this.generatedTimestamp = queryState.generatedTimestamp();
            this.generatedNowInSeconds = queryState.generatedNowInSeconds();
            this.keyspace = queryState.getClientState().getRawKeyspace();

            /*
             * Struggled with what tradeoff to make in terms of query options which is potentially large and complicated
             * There is tension between low garbage production (or allocator overhead), small working set size, and CPU overhead reserializing the
             * query options into binary format.
             *
             * I went with the lowest risk most predictable option which is allocator overhead and CPU overhead
             * rather then keep the original query message around so I could just serialize that as a memcpy. It's more
             * instructions when turned on, but it doesn't change memory footprint quite as much and it's more pay for what you use
             * in terms of query volume. The CPU overhead is spread out across producers so we should at least get
             * some scaling.
             *
             */
            try
            {
                QueryOptions.codec.encode(queryOptions, queryOptionsBuffer, queryOptions.getProtocolVersion());
            }
            catch (Throwable e)
            {
                queryOptionsBuffer.release();
                throw e;
            }
        }

        @Override
        public void writeMarshallable(WireOut wire)
        {
            wire.write(VERSION).int16(CURRENT_VERSION);
            wire.write(TYPE).text(type());

            wire.write(QUERY_START_TIME).int64(queryStartTime);
            wire.write(PROTOCOL_VERSION).int32(protocolVersion);
            wire.write(QUERY_OPTIONS).bytes(BytesStore.wrap(queryOptionsBuffer.nioBuffer()));

            wire.write(GENERATED_TIMESTAMP).int64(generatedTimestamp);
            wire.write(GENERATED_NOW_IN_SECONDS).int32(generatedNowInSeconds);

            wire.write(KEYSPACE).text(keyspace);
        }

        @Override
        public void release()
        {
            queryOptionsBuffer.release();
        }

        @Override
        public int weight()
        {
            return OBJECT_HEADER_SIZE
                 + 8                                                  // queryStartTime
                 + 4                                                  // protocolVersion
                 + EMPTY_BYTEBUF_SIZE + queryOptionsBuffer.capacity() // queryOptionsBuffer
                 + 8                                                  // generatedTimestamp
                 + 4                                                  // generatedNowInSeconds
                 + (keyspace != null
                    ? Ints.checkedCast(ObjectSizes.sizeOf(keyspace))  // keyspace
                    : OBJECT_REFERENCE_SIZE);                         // null
        }

        protected abstract String type();
    }

}
