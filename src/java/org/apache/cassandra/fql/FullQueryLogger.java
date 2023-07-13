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
package org.apache.cassandra.fql;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;

import org.apache.cassandra.io.util.File;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.wire.ValueOut;
import net.openhft.chronicle.wire.WireOut;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryEvents;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.CBUtil;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.binlog.BinLog;
import org.apache.cassandra.utils.binlog.BinLogOptions;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;
import org.apache.cassandra.utils.concurrent.WeightedQueue;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A logger that logs entire query contents after the query finishes (or times out).
 */
public class FullQueryLogger implements QueryEvents.Listener
{
    protected static final Logger logger = LoggerFactory.getLogger(FullQueryLogger.class);

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

    private static final int EMPTY_LIST_SIZE = Ints.checkedCast(ObjectSizes.measureDeep(new ArrayList<>(0)));
    private static final int EMPTY_BYTEBUF_SIZE;

    public static final FullQueryLogger instance = new FullQueryLogger();

    volatile BinLog binLog;

    public synchronized void enable(Path path, String rollCycle, boolean blocking, int maxQueueWeight, long maxLogSize, String archiveCommand, int maxArchiveRetries)
    {
        if (this.binLog != null)
            throw new IllegalStateException("Binlog is already configured");
        this.binLog = new BinLog.Builder().path(path)
                                          .rollCycle(rollCycle)
                                          .blocking(blocking)
                                          .maxQueueWeight(maxQueueWeight)
                                          .maxLogSize(maxLogSize)
                                          .archiveCommand(archiveCommand)
                                          .maxArchiveRetries(maxArchiveRetries)
                                          .build(true);
        QueryEvents.instance.registerListener(this);
    }

    public synchronized void enableWithoutClean(Path path, String rollCycle, boolean blocking, int maxQueueWeight, long maxLogSize, String archiveCommand, int maxArchiveRetries)
    {
        if (this.binLog != null)
            throw new IllegalStateException("Binlog is already configured");
        this.binLog = new BinLog.Builder().path(path)
                                          .rollCycle(rollCycle)
                                          .blocking(blocking)
                                          .maxQueueWeight(maxQueueWeight)
                                          .maxLogSize(maxLogSize)
                                          .archiveCommand(archiveCommand)
                                          .maxArchiveRetries(maxArchiveRetries)
                                          .build(false);
        QueryEvents.instance.registerListener(this);
    }


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

    public FullQueryLoggerOptions getFullQueryLoggerOptions()
    {
        if (isEnabled())
        {
            final FullQueryLoggerOptions options = new FullQueryLoggerOptions();
            final BinLogOptions binLogOptions = binLog.getBinLogOptions();

            options.archive_command = binLogOptions.archive_command;
            options.roll_cycle = binLogOptions.roll_cycle;
            options.block = binLogOptions.block;
            options.max_archive_retries = binLogOptions.max_archive_retries;
            options.max_queue_weight = binLogOptions.max_queue_weight;
            options.max_log_size = binLogOptions.max_log_size;
            options.log_dir = binLog.path.toString();

            return options;
        }
        else
        {
            // otherwise get what database is configured with from cassandra.yaml
            return DatabaseDescriptor.getFullQueryLogOptions();
        }
    }

    public synchronized void stop()
    {
        try
        {
            BinLog binLog = this.binLog;
            if (binLog != null)
            {
                logger.info("Stopping full query logging to {}", binLog.path);
                binLog.stop();
            }
            else
            {
                logger.info("Full query log already stopped");
            }
        }
        catch (InterruptedException e)
        {
            throw new UncheckedInterruptedException(e);
        }
        finally
        {
            QueryEvents.instance.unregisterListener(this);
            this.binLog = null;
        }
    }

    /**
     * Need the path as a parameter as well because if the process is restarted the config file might be the only
     * location for retrieving the path to the full query log files, but JMX also allows you to specify a path
     * that isn't persisted anywhere so we have to clean that one as well.
     */
    public synchronized void reset(String fullQueryLogPath)
    {
        try
        {
            Set<File> pathsToClean = Sets.newHashSet();

            //First decide whether to clean the path configured in the YAML
            if (fullQueryLogPath != null)
            {
                File fullQueryLogPathFile = new File(fullQueryLogPath);
                if (fullQueryLogPathFile.exists())
                {
                    pathsToClean.add(fullQueryLogPathFile);
                }
            }

            //Then decide whether to clean the last used path, possibly configured by JMX
            if (binLog != null && binLog.path != null)
            {
                File pathFile = new File(binLog.path);
                if (pathFile.exists())
                {
                    pathsToClean.add(pathFile);
                }
            }

            logger.info("Reset (and deactivation) of full query log requested.");
            if (binLog != null)
            {
                logger.info("Stopping full query log. Cleaning {}.", pathsToClean);
                binLog.stop();
                binLog = null;
            }
            else
            {
                logger.info("Full query log already deactivated. Cleaning {}.", pathsToClean);
            }

            Throwable accumulate = null;
            for (File f : pathsToClean)
            {
                accumulate = BinLog.cleanDirectory(f, accumulate);
            }
            if (accumulate != null)
            {
                throw new RuntimeException(accumulate);
            }
        }
        catch (Exception e)
        {
            if (e instanceof RuntimeException)
            {
                throw (RuntimeException)e;
            }
            throw new RuntimeException(e);
        }
        finally
        {
            QueryEvents.instance.unregisterListener(this);
        }
    }

    public boolean isEnabled()
    {
        return this.binLog != null;
    }

    /**
     * Log an invocation of a batch of queries
     * @param type The type of the batch
     * @param statements the prepared cql statements (unused here)
     * @param queries CQL text of the queries
     * @param values Values to bind to as parameters for the queries
     * @param queryOptions Options associated with the query invocation
     * @param queryState Timestamp state associated with the query invocation
     * @param batchTimeMillis Approximate time in milliseconds since the epoch since the batch was invoked
     * @param response the response from the batch query
     */
    public void batchSuccess(BatchStatement.Type type,
                             List<? extends CQLStatement> statements,
                             List<String> queries,
                             List<List<ByteBuffer>> values,
                             QueryOptions queryOptions,
                             QueryState queryState,
                             long batchTimeMillis,
                             Message.Response response)
    {
        checkNotNull(type, "type was null");
        checkNotNull(queries, "queries was null");
        checkNotNull(values, "value was null");
        checkNotNull(queryOptions, "queryOptions was null");
        checkNotNull(queryState, "queryState was null");
        Preconditions.checkArgument(batchTimeMillis > 0, "batchTimeMillis must be > 0");

        //Don't construct the wrapper if the log is disabled
        BinLog binLog = this.binLog;
        if (binLog == null)
        {
            return;
        }

        Batch wrappedBatch = new Batch(type, queries, values, queryOptions, queryState, batchTimeMillis);
        binLog.logRecord(wrappedBatch);
    }

    /**
     * Log a single CQL query
     * @param query CQL query text
     * @param queryOptions Options associated with the query invocation
     * @param queryState Timestamp state associated with the query invocation
     * @param queryTimeMillis Approximate time in milliseconds since the epoch since the batch was invoked
     * @param response the response from this query
     */
    public void querySuccess(CQLStatement statement,
                             String query,
                             QueryOptions queryOptions,
                             QueryState queryState,
                             long queryTimeMillis,
                             Message.Response response)
    {
        checkNotNull(query, "query was null");
        checkNotNull(queryOptions, "queryOptions was null");
        checkNotNull(queryState, "queryState was null");
        Preconditions.checkArgument(queryTimeMillis > 0, "queryTimeMillis must be > 0");

        //Don't construct the wrapper if the log is disabled
        BinLog binLog = this.binLog;
        if (binLog == null)
            return;

        Query wrappedQuery = new Query(query, queryOptions, queryState, queryTimeMillis);
        binLog.logRecord(wrappedQuery);
    }

    public void executeSuccess(CQLStatement statement, String query, QueryOptions options, QueryState state, long queryTime, Message.Response response)
    {
        querySuccess(statement, query, options, state, queryTime, response);
    }

    public static class Query extends AbstractLogEntry
    {
        /**
         * The shallow size of a {@code Query} object.
         */
        private static final long EMPTY_SIZE = ObjectSizes.measure(new Query());

        private final String query;

        public Query(String query, QueryOptions queryOptions, QueryState queryState, long queryStartTime)
        {
            super(queryOptions, queryState, queryStartTime);
            this.query = query;
        }

        /**
         * Constructor only use to compute this class shallow size.
         */
        private Query()
        {
            this.query = null;
        }

        @Override
        protected String type()
        {
            return SINGLE_QUERY;
        }

        @Override
        public void writeMarshallablePayload(WireOut wire)
        {
            super.writeMarshallablePayload(wire);
            wire.write(QUERY).text(query);
        }

        @Override
        public int weight()
        {
            // Object deep size = Object' shallow size + query field deep size + deep size of the parent fields
            return Ints.checkedCast(EMPTY_SIZE + ObjectSizes.sizeOf(query) + super.fieldsSize());
        }
    }

    public static class Batch extends AbstractLogEntry
    {
        /**
         * The shallow size of a {@code Batch} object (which includes primitive fields).
         */
        private static final long EMPTY_SIZE = ObjectSizes.measure(new Batch());

        /**
         * The weight is pre-computed in the constructor and represent the object deep size.
         */
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

            // We assume that all the lists are ArrayLists and that the size of each underlying array is the one of the list 
            // (which is obviously wrong but not worst than the previous computation that was ignoring part of the arrays size in the computation).
            long queriesSize = EMPTY_LIST_SIZE + ObjectSizes.sizeOfReferenceArray(queries.size());

            for (String query : queries)
                queriesSize += ObjectSizes.sizeOf(checkNotNull(query));

            long valuesSize = EMPTY_LIST_SIZE + ObjectSizes.sizeOfReferenceArray(values.size());
            for (List<ByteBuffer> subValues : values)
            {
                valuesSize += EMPTY_LIST_SIZE + ObjectSizes.sizeOfReferenceArray(subValues.size());
                for (ByteBuffer subValue : subValues)
                    valuesSize += ObjectSizes.sizeOnHeapOf(subValue);
            }

            // No need to add the batch type which is an enum.
            this.weight = Ints.checkedCast(EMPTY_SIZE            // Shallow size object
                                            + super.fieldsSize() // deep size of the parent fields (non-primitives as they are included in the shallow size) 
                                            + queriesSize        // deep size queries field
                                            + valuesSize);       // deep size values field
        }

        /**
         * Constructor only use to compute this class shallow size.
         */
        private Batch()
        {
            this.weight = 0;
            this.batchType = null;
            this.queries = null;
            this.values = null;
        }

        @Override
        protected String type()
        {
            return BATCH;
        }

        @Override
        public void writeMarshallablePayload(WireOut wire)
        {
            super.writeMarshallablePayload(wire);
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
        private final long generatedNowInSeconds;
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

        /**
         * Constructor only use to compute sub-classes shallow size.
         */
        private AbstractLogEntry()
        {
            this.queryStartTime = 0;
            this.protocolVersion = 0;
            this.queryOptionsBuffer = null;
            this.generatedTimestamp = 0;
            this.generatedNowInSeconds = 0;
            this.keyspace = null;
        }

        @Override
        protected long version()
        {
            return CURRENT_VERSION;
        }

        @Override
        public void writeMarshallablePayload(WireOut wire)
        {
            wire.write(QUERY_START_TIME).int64(queryStartTime);
            wire.write(PROTOCOL_VERSION).int32(protocolVersion);
            wire.write(QUERY_OPTIONS).bytes(BytesStore.wrap(queryOptionsBuffer.nioBuffer()));

            wire.write(GENERATED_TIMESTAMP).int64(generatedTimestamp);
            wire.write(GENERATED_NOW_IN_SECONDS).int64(generatedNowInSeconds);

            wire.write(KEYSPACE).text(keyspace);
        }

        @Override
        public void release()
        {
            queryOptionsBuffer.release();
        }

        /**
         * Returns the sum of the non-primitive fields' deep sizes.
         * @return the sum of the non-primitive fields' deep sizes.
         */
        protected long fieldsSize()
        {
            return EMPTY_BYTEBUF_SIZE + queryOptionsBuffer.capacity() // queryOptionsBuffer
                   + ObjectSizes.sizeOf(keyspace);                    // keyspace
        }
    }

}
