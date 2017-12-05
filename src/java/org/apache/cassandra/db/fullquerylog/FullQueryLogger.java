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

package org.apache.cassandra.db.fullquerylog;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.wire.ValueOut;
import net.openhft.chronicle.wire.WireOut;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.io.FSError;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.transport.CBUtil;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.binlog.BinLog;
import org.apache.cassandra.utils.concurrent.WeightedQueue;
import org.github.jamm.MemoryLayoutSpecification;

/**
 * A logger that logs entire query contents after the query finishes (or times out).
 */
public class FullQueryLogger
{
    private static final int EMPTY_BYTEBUFFER_SIZE = Ints.checkedCast(ObjectSizes.sizeOnHeapExcludingData(ByteBuffer.allocate(0)));
    private static final int EMPTY_LIST_SIZE = Ints.checkedCast(ObjectSizes.measureDeep(new ArrayList(0)));
    private static final int EMPTY_BYTEBUF_SIZE;
    private static final int OBJECT_HEADER_SIZE = MemoryLayoutSpecification.SPEC.getObjectHeaderSize();
    static
    {
        int tempSize = 0;
        ByteBuf buf = CBUtil.allocator.buffer(0, 0);
        try
        {
            tempSize = Ints.checkedCast(ObjectSizes.measure(buf));
        }
        finally
        {
            buf.release();
        }
        EMPTY_BYTEBUF_SIZE = tempSize;
    }

    private static final Logger logger = LoggerFactory.getLogger(FullQueryLogger.class);
    private static final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 1, TimeUnit.MINUTES);
    private static final NoSpamLogger.NoSpamLogStatement droppedSamplesStatement = noSpamLogger.getStatement("Dropped {} binary log samples", 1, TimeUnit.MINUTES);

    public static final FullQueryLogger instance = new FullQueryLogger();

    volatile BinLog binLog;
    private volatile boolean blocking;
    private Path path;

    private final AtomicLong droppedSamplesSinceLastLog = new AtomicLong();

    private FullQueryLogger()
    {
    }

    /**
     * Configure the global instance of the FullQueryLogger
     * @param path Dedicated path where the FQL can store it's files.
     * @param rollCycle How often to roll FQL log segments so they can potentially be reclaimed
     * @param blocking Whether the FQL should block if the FQL falls behind or should drop log records
     * @param maxQueueWeight Maximum weight of in memory queue for records waiting to be written to the file before blocking or dropping
     * @param maxLogSize Maximum size of the rolled files to retain on disk before deleting the oldest file
     */
    public synchronized void configure(Path path, String rollCycle, boolean blocking, int maxQueueWeight, long maxLogSize)
    {
        Preconditions.checkNotNull(path, "path was null");
        File pathAsFile = path.toFile();
        Preconditions.checkNotNull(rollCycle, "rollCycle was null");
        rollCycle = rollCycle.toUpperCase();

        //Exists and is a directory or can be created
        Preconditions.checkArgument((pathAsFile.exists() && pathAsFile.isDirectory()) || (!pathAsFile.exists() && pathAsFile.mkdirs()), "path exists and is not a directory or couldn't be created");
        Preconditions.checkArgument(pathAsFile.canRead() && pathAsFile.canWrite() && pathAsFile.canExecute(), "path is not readable, writable, and executable");
        Preconditions.checkNotNull(RollCycles.valueOf(rollCycle), "unrecognized roll cycle");
        Preconditions.checkArgument(maxQueueWeight > 0, "maxQueueWeight must be > 0");
        Preconditions.checkArgument(maxLogSize > 0, "maxLogSize must be > 0");
        logger.info("Attempting to configure full query logger path: {} Roll cycle: {} Blocking: {} Max queue weight: {} Max log size:{}", path, rollCycle, blocking, maxQueueWeight, maxLogSize);
        if (binLog != null)
        {
            logger.warn("Full query logger already configured. Ignoring requested configuration.");
            throw new IllegalStateException("Already configured");
        }

        if (path.toFile().exists())
        {
            Throwable error = cleanDirectory(path.toFile(), null);
            if (error != null)
            {
                throw new RuntimeException(error);
            }
        }

        this.path = path;
        this.blocking = blocking;
        binLog = new BinLog(path, RollCycles.valueOf(rollCycle), maxQueueWeight, maxLogSize);
        binLog.start();
    }

    /**
     * Need the path as a parameter as well because if the process is restarted the config file might be the only
     * location for retrieving the path to the full query log files, but JMX also allows you to specify a path
     * that isn't persisted anywhere so we have to clean that one a well.
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
            if (path != null)
            {
                File pathFile = path.toFile();
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
                accumulate = cleanDirectory(f, accumulate);
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
    }

    /**
     * Stop the full query log leaving behind any generated files.
     */
    public synchronized void stop()
    {
        try
        {
            logger.info("Deactivation of full query log requested.");
            if (binLog != null)
            {
                logger.info("Stopping full query log");
                binLog.stop();
                binLog = null;
            }
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Check whether the full query log is enabled.
     * @return true if records are recorded and false otherwise.
     */
    public boolean enabled()
    {
        return binLog != null;
    }

    /**
     * This is potentially lossy, but it's not super critical as we will always generally know
     * when this is happening and roughly how bad it is.
     */
    private void logDroppedSample()
    {
        droppedSamplesSinceLastLog.incrementAndGet();
        if (droppedSamplesStatement.warn(new Object[] {droppedSamplesSinceLastLog.get()}))
        {
            droppedSamplesSinceLastLog.set(0);
        }
    }

    /**
     * Log an invocation of a batch of queries
     * @param type The type of the batch
     * @param queries CQL text of the queries
     * @param values Values to bind to as parameters for the queries
     * @param queryOptions Options associated with the query invocation
     * @param batchTimeMillis Approximate time in milliseconds since the epoch since the batch was invoked
     */
    public void logBatch(String type, List<String> queries,  List<List<ByteBuffer>> values, QueryOptions queryOptions, long batchTimeMillis)
    {
        Preconditions.checkNotNull(type, "type was null");
        Preconditions.checkNotNull(queries, "queries was null");
        Preconditions.checkNotNull(values, "value was null");
        Preconditions.checkNotNull(queryOptions, "queryOptions was null");
        Preconditions.checkArgument(batchTimeMillis > 0, "batchTimeMillis must be > 0");

        //Don't construct the wrapper if the log is disabled
        BinLog binLog = this.binLog;
        if (binLog == null)
        {
            return;
        }

        WeighableMarshallableBatch wrappedBatch = new WeighableMarshallableBatch(type, queries, values, queryOptions, batchTimeMillis);
        logRecord(wrappedBatch, binLog);
    }

    void logRecord(AbstractWeighableMarshallable record, BinLog binLog)
    {

        boolean putInQueue = false;
        try
        {
            if (blocking)
            {
                try
                {
                    binLog.put(record);
                    putInQueue = true;
                }
                catch (InterruptedException e)
                {
                    throw new RuntimeException(e);
                }
            }
            else
            {
                if (!binLog.offer(record))
                {
                    logDroppedSample();
                }
                else
                {
                    putInQueue = true;
                }
            }
        }
        finally
        {
            if (!putInQueue)
            {
                record.release();
            }
        }
    }

    /**
     * Log a single CQL query
     * @param query CQL query text
     * @param queryOptions Options associated with the query invocation
     * @param queryTimeMillis Approximate time in milliseconds since the epoch since the batch was invoked
     */
    public void logQuery(String query, QueryOptions queryOptions, long queryTimeMillis)
    {
        Preconditions.checkNotNull(query, "query was null");
        Preconditions.checkNotNull(queryOptions, "queryOptions was null");
        Preconditions.checkArgument(queryTimeMillis > 0, "queryTimeMillis must be > 0");

        //Don't construct the wrapper if the log is disabled
        BinLog binLog = this.binLog;
        if (binLog == null)
        {
            return;
        }

        WeighableMarshallableQuery wrappedQuery = new WeighableMarshallableQuery(query, queryOptions, queryTimeMillis);
        logRecord(wrappedQuery, binLog);
    }

    private static abstract class AbstractWeighableMarshallable extends BinLog.ReleaseableWriteMarshallable implements WeightedQueue.Weighable
    {
        private final ByteBuf queryOptionsBuffer;
        private final long timeMillis;
        private final int protocolVersion;

        private AbstractWeighableMarshallable(QueryOptions queryOptions, long timeMillis)
        {
            this.timeMillis = timeMillis;
            ProtocolVersion version = queryOptions.getProtocolVersion();
            this.protocolVersion = version.asInt();
            int optionsSize = QueryOptions.codec.encodedSize(queryOptions, version);
            queryOptionsBuffer = CBUtil.allocator.buffer(optionsSize, optionsSize);
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
            boolean success = false;
            try
            {
                QueryOptions.codec.encode(queryOptions, queryOptionsBuffer, version);
                success = true;
            }
            finally
            {
                if (!success)
                {
                    queryOptionsBuffer.release();
                }
            }
        }

        @Override
        public void writeMarshallable(WireOut wire)
        {
            wire.write("protocol-version").int32(protocolVersion);
            wire.write("query-options").bytes(BytesStore.wrap(queryOptionsBuffer.nioBuffer()));
            wire.write("query-time").int64(timeMillis);
        }

        @Override
        public void release()
        {
            queryOptionsBuffer.release();
        }

        //8-bytes for protocol version (assume alignment cost), 8-byte timestamp, 8-byte object header + other contents
        @Override
        public int weight()
        {
            return 8 + 8 + OBJECT_HEADER_SIZE + EMPTY_BYTEBUF_SIZE + queryOptionsBuffer.capacity();
        }
    }

    static class WeighableMarshallableBatch extends AbstractWeighableMarshallable
    {
        private final int weight;
        private final String batchType;
        private final List<String> queries;
        private final List<List<ByteBuffer>> values;

        public WeighableMarshallableBatch(String batchType, List<String> queries, List<List<ByteBuffer>> values, QueryOptions queryOptions, long batchTimeMillis)
        {
           super(queryOptions, batchTimeMillis);
           this.queries = queries;
           this.values = values;
           this.batchType = batchType;
           boolean success = false;
           try
           {

               //weight, batch type, queries, values
               int weightTemp = 8 + EMPTY_LIST_SIZE + EMPTY_LIST_SIZE;
               for (int ii = 0; ii < queries.size(); ii++)
               {
                   weightTemp += ObjectSizes.sizeOf(queries.get(ii));
               }

               weightTemp += EMPTY_LIST_SIZE * values.size();
               for (int ii = 0; ii < values.size(); ii++)
               {
                   List<ByteBuffer> sublist = values.get(ii);
                   weightTemp += EMPTY_BYTEBUFFER_SIZE * sublist.size();
                   for (int zz = 0; zz < sublist.size(); zz++)
                   {
                       weightTemp += sublist.get(zz).capacity();
                   }
               }
               weightTemp += super.weight();
               weightTemp += ObjectSizes.sizeOf(batchType);
               weight = weightTemp;
               success = true;
           }
           finally
           {
               if (!success)
               {
                   release();
               }
           }
        }

        @Override
        public void writeMarshallable(WireOut wire)
        {
            wire.write("type").text("batch");
            super.writeMarshallable(wire);
            wire.write("batch-type").text(batchType);
            ValueOut valueOut = wire.write("queries");
            valueOut.int32(queries.size());
            for (String query : queries)
            {
                valueOut.text(query);
            }
            valueOut = wire.write("values");
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

    static class WeighableMarshallableQuery extends AbstractWeighableMarshallable
    {
        private final String query;

        public WeighableMarshallableQuery(String query, QueryOptions queryOptions, long queryTimeMillis)
        {
            super(queryOptions, queryTimeMillis);
            this.query = query;
        }

        @Override
        public void writeMarshallable(WireOut wire)
        {
            wire.write("type").text("single");
            super.writeMarshallable(wire);
            wire.write("query").text(query);
        }

        @Override
        public int weight()
        {
            return Ints.checkedCast(ObjectSizes.sizeOf(query)) + super.weight();
        }
    }

    static Throwable cleanDirectory(File directory, Throwable accumulate)
    {
        if (!directory.exists())
        {
            return Throwables.merge(accumulate, new RuntimeException(String.format("%s does not exists", directory)));
        }
        if (!directory.isDirectory())
        {
            return Throwables.merge(accumulate, new RuntimeException(String.format("%s is not a directory", directory)));
        }
        for (File f : directory.listFiles())
        {
            accumulate = deleteRecursively(f, accumulate);
        }
        if (accumulate instanceof FSError)
        {
            FileUtils.handleFSError((FSError)accumulate);
        }
        return accumulate;
    }

    private static Throwable deleteRecursively(File fileOrDirectory, Throwable accumulate)
    {
        if (fileOrDirectory.isDirectory())
        {
            for (File f : fileOrDirectory.listFiles())
            {
                accumulate = FileUtils.deleteWithConfirm(f, true, accumulate);
            }
        }
        return FileUtils.deleteWithConfirm(fileOrDirectory, true , accumulate);
    }
}
