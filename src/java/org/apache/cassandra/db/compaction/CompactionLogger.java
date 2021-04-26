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

package org.apache.cassandra.db.compaction;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.lang.ref.WeakReference;
import java.nio.file.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.MapMaker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.Throwables;

/**
 * This is a Compaction logger that logs compaction events in a file called compactions.log.
 * It was added by CASSANDRA-10805.
 */
public class CompactionLogger
{
    private static final DateFormat dateFormatter = new SimpleDateFormat("HH:mm:ss.SSS");

    public interface Strategy
    {
        JsonNode sstable(SSTableReader sstable);

        JsonNode options();

        static Strategy none = new Strategy()
        {
            public JsonNode sstable(SSTableReader sstable)
            {
                return null;
            }

            public JsonNode options()
            {
                return null;
            }
        };
    }

    /**
     * This will produce the compaction strategy's starting information.
     */
    public interface StrategySummary
    {
        JsonNode getSummary();
    }

    /**
     * This is an interface to allow writing to a different interface.
     */
    public interface Writer extends Closeable
    {
        /**
         * @param toWrite This should be written out to the medium capturing the logs
         */
        void write(String toWrite);

        /**
         * This is used when we are already trying to write out the start of a
         * @param statement This should be written out to the medium capturing the logs
         * @param tag       This is an identifier for a strategy; each strategy should have a distinct Object
         */
        void writeStart(JsonNode statement, Object tag);

        /**
         * @param statement This should be written out to the medium capturing the logs
         * @param summary   This can be used when a tag is not recognized by this writer; this can be because the file
         *                  has been rolled, or otherwise the writer had to start over
         * @param tag       This is an identifier for a strategy; each strategy should have a distinct Object
         */
        void write(JsonNode statement, StrategySummary summary, Object tag);

        /**
         * Closes the writer
         */
        @Override
        void close();
    }

    private interface CompactionStrategyAndTableFunction
    {
        JsonNode apply(AbstractCompactionStrategy strategy, SSTableReader sstable);
    }

    private static final JsonNodeFactory json = JsonNodeFactory.instance;
    private static final Logger logger = LoggerFactory.getLogger(CompactionLogger.class);

    private static final ExecutorService loggerService = Executors.newFixedThreadPool(1);
    private static final Writer jsonWriter = new CompactionLogSerializer("compaction", "log", loggerService);

    private final WeakReference<ColumnFamilyStore> cfsRef;
    private final WeakReference<CompactionStrategyManager> csmRef;
    private final AtomicInteger identifier = new AtomicInteger(0);
    private final Map<AbstractCompactionStrategy, String> compactionStrategyMapping = new MapMaker().weakKeys().makeMap();
    private final Map<AbstractCompactionStrategy, Writer> csvWriters = new MapMaker().makeMap();
    private final AtomicBoolean enabled = new AtomicBoolean(false);

    CompactionLogger(ColumnFamilyStore cfs, CompactionStrategyManager csm)
    {
        csmRef = new WeakReference<>(csm);
        cfsRef = new WeakReference<>(cfs);
    }

    /**
     * Visit all the strategies in the {@link CompactionStrategyManager} reference, if available.
     *
     * @param consumer a consumer function that receives all the strategies one by one
     */
    private void visitStrategies(Consumer<AbstractCompactionStrategy> consumer)
    {
        CompactionStrategyManager csm = csmRef.get();
        if (csm == null)
            return;
        csm.getStrategies()
           .forEach(l -> l.forEach(consumer));
    }

    /**
     * Rely on {@link this#visitStrategies(Consumer)} to visit all the strategies in the {@link CompactionStrategyManager}
     * reference and add the properties extracted by the function passed in to a json node that is returned.
     *
     * @param select a function that given a strategy returns a json node
     *
     * @return a json node containing information on all the strategies returned by the strategy manager and the function passed in.
     */
    private ArrayNode getStrategiesJsonNode(Function<AbstractCompactionStrategy, JsonNode> select)
    {
        ArrayNode node = json.arrayNode();
        visitStrategies(acs -> node.add(select.apply(acs)));
        return node;
    }

    private ArrayNode sstableMap(Collection<SSTableReader> sstables, CompactionStrategyAndTableFunction csatf)
    {
        CompactionStrategyManager csm = csmRef.get();
        ArrayNode node = json.arrayNode();
        if (csm == null)
            return node;
        sstables.forEach(t -> node.add(csatf.apply(csm.getCompactionStrategyFor(t), t)));
        return node;
    }

    private String getId(AbstractCompactionStrategy strategy)
    {
        return compactionStrategyMapping.computeIfAbsent(strategy, s -> String.valueOf(identifier.getAndIncrement()));
    }

    private JsonNode formatSSTables(AbstractCompactionStrategy strategy)
    {
        ArrayNode node = json.arrayNode();
        CompactionStrategyManager csm = csmRef.get();
        ColumnFamilyStore cfs = cfsRef.get();
        if (csm == null || cfs == null)
            return node;
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            if (csm.getCompactionStrategyFor(sstable) == strategy)
                node.add(formatSSTable(strategy, sstable));
        }
        return node;
    }

    private JsonNode formatSSTable(AbstractCompactionStrategy strategy, SSTableReader sstable)
    {
        ObjectNode node = json.objectNode();
        node.put("generation", sstable.descriptor.id.toString());
        node.put("version", sstable.descriptor.version.getVersion());
        node.put("size", sstable.onDiskLength());
        JsonNode logResult = strategy.strategyLogger().sstable(sstable);
        if (logResult != null)
            node.set("details", logResult);
        return node;
    }

    private JsonNode getStrategyDetails(AbstractCompactionStrategy strategy)
    {
        ObjectNode node = json.objectNode();
        CompactionStrategyManager csm = csmRef.get();
        if (csm == null)
            return node;
        node.put("strategyId", getId(strategy));
        node.put("type", strategy.getName());
        node.set("tables", formatSSTables(strategy));
        node.put("repaired", csm.isRepaired(strategy));
        List<String> folders = csm.getStrategyFolders(strategy);
        ArrayNode folderNode = json.arrayNode();
        for (String folder : folders)
        {
            folderNode.add(folder);
        }
        node.set("folders", folderNode);

        JsonNode logResult = strategy.strategyLogger().options();
        if (logResult != null)
            node.set("options", logResult);
        return node;
    }

    private JsonNode getStrategyId(AbstractCompactionStrategy strategy)
    {
        ObjectNode node = json.objectNode();
        node.put("strategyId", getId(strategy));
        return node;
    }

    private JsonNode describeSSTable(AbstractCompactionStrategy strategy, SSTableReader sstable)
    {
        ObjectNode node = json.objectNode();
        node.put("strategyId", getId(strategy));
        node.set("table", formatSSTable(strategy, sstable));
        return node;
    }

    private void maybeAddSchemaAndTimeInfo(ObjectNode node)
    {
        ColumnFamilyStore cfs = cfsRef.get();
        if (cfs == null)
            return;
        node.put("keyspace", cfs.getKeyspaceName());
        node.put("table", cfs.getTableName());
        node.put("time", System.currentTimeMillis());
    }

    private JsonNode getEventJsonNode()
    {
        ObjectNode node = json.objectNode();
        node.put("type", "enable");
        maybeAddSchemaAndTimeInfo(node);
        node.set("strategies", getStrategiesJsonNode(this::getStrategyDetails));
        return node;
    }

    public void enable()
    {
        if (enabled.compareAndSet(false, true))
        {
            jsonWriter.writeStart(getEventJsonNode(), this);
        }
    }

    public void disable()
    {
        if (enabled.compareAndSet(true, false))
        {
            ObjectNode node = json.objectNode();
            node.put("type", "disable");
            maybeAddSchemaAndTimeInfo(node);
            node.set("strategies", getStrategiesJsonNode(this::getStrategyId));
            jsonWriter.write(node, this::getEventJsonNode, this);

            visitStrategies(strategy -> csvWriters.computeIfPresent(strategy, (s, w) -> { w.close(); return null; }));
        }
    }

    public boolean enabled()
    {
        return enabled.get();
    }

    public void flush(Collection<SSTableReader> sstables)
    {
        if (enabled.get())
        {
            ObjectNode node = json.objectNode();
            node.put("type", "flush");
            maybeAddSchemaAndTimeInfo(node);
            node.set("tables", sstableMap(sstables, this::describeSSTable));
            jsonWriter.write(node, this::getEventJsonNode, this);
        }
    }

    public void compaction(long startTime, Collection<SSTableReader> input, long endTime, Collection<SSTableReader> output)
    {
        if (enabled.get())
        {
            ObjectNode node = json.objectNode();
            node.put("type", "compaction");
            maybeAddSchemaAndTimeInfo(node);
            node.put("start", String.valueOf(startTime));
            node.put("end", String.valueOf(endTime));
            node.set("input", sstableMap(input, this::describeSSTable));
            node.set("output", sstableMap(output, this::describeSSTable));
            jsonWriter.write(node, this::getEventJsonNode, this);
        }
    }

    public void pending(AbstractCompactionStrategy strategy, int remaining)
    {
        if (remaining != 0 && enabled.get())
        {
            ObjectNode node = json.objectNode();
            node.put("type", "pending");
            maybeAddSchemaAndTimeInfo(node);
            node.put("strategyId", getId(strategy));
            node.put("pending", remaining);
            jsonWriter.write(node, this::getEventJsonNode, this);
        }
    }

    /**
     * Write the strategy statistics formatted as CSV.
     **/
    public void statistics(AbstractCompactionStrategy strategy, String event, CompactionStrategyStatistics statistics)
    {
        if (logger.isTraceEnabled())
            logger.trace("Compaction statistics for strategy {} and event {}: {}", strategy, event, statistics);

        if (!enabled.get())
            return;

        Writer writer = getCsvWriter(strategy, statistics.getHeader());
        for (Collection<String> data : statistics.getData())
            writer.write(String.join(",", Iterables.concat(ImmutableList.of(currentTime(), event), data)) + System.lineSeparator());
    }

    private Writer getCsvWriter(AbstractCompactionStrategy strategy, Collection<String> header)
    {
        Writer writer = csvWriters.get(strategy);
        if (writer != null)
            return writer;

        // TODO - should we add the repair status?
        String fileName = String.format("compaction-%s-%s-%s-%s",
                                        strategy.getName(),
                                        strategy.getMetadata().keyspace,
                                        strategy.getMetadata().name,
                                        getId(strategy));

        writer = new CompactionLogSerializer(fileName, "csv", loggerService);
        if (csvWriters.putIfAbsent(strategy, writer) == null)
        {
            writer.write(String.join(",", Iterables.concat(ImmutableList.of("Timestamp", "Event"), header)) + System.lineSeparator());
            return writer;
        }
        else
        {
            writer.close();
            return csvWriters.get(strategy);
        }
    }

    private String currentTime()
    {
        return dateFormatter.format(new Date(System.currentTimeMillis()));
    }

    private static class CompactionLogSerializer implements Writer
    {
        private static final String logDirectory = System.getProperty("cassandra.logdir", ".");

        // This is only accessed on the logger service thread, so it does not need to be thread safe
        private final String fileName;
        private final String fileExt;
        private final ExecutorService loggerService;
        private final Set<Object> rolled;
        private OutputStreamWriter stream;

        CompactionLogSerializer(String fileName, String fileExt, ExecutorService loggerService)
        {
            this.fileName = fileName;
            this.fileExt = fileExt;
            this.loggerService = loggerService;
            this.rolled = new HashSet<>();
        }

        private OutputStreamWriter createStream() throws IOException
        {
            int count = 0;
            Path compactionLog = Paths.get(logDirectory,  String.format("%s.%s", fileName, fileExt));
            if (Files.exists(compactionLog))
            {
                Path tryPath = compactionLog;
                while (Files.exists(tryPath))
                {
                    tryPath = Paths.get(logDirectory, String.format("%s-%d.%s", fileName, count++, fileExt));
                }
                Files.move(compactionLog, tryPath);
            }

            return new OutputStreamWriter(Files.newOutputStream(compactionLog, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE));
        }

        private interface ThrowingConsumer<T>
        {
            void accept(T stream) throws IOException;
        }

        private void performWrite(ThrowingConsumer<OutputStreamWriter> writeTask)
        {
            loggerService.execute(() ->
            {
              try
              {
                  if (stream == null)
                      stream = createStream();

                  writeTask.accept(stream);
                  stream.flush();
              }
              catch (IOException ioe)
              {
                  // We'll drop the change and log the error to the logger.
                  NoSpamLogger.log(logger, NoSpamLogger.Level.ERROR, 1, TimeUnit.MINUTES,
                                   "Could not write to the log file: {}", ioe);
              }
            });
        }

        public void write(String toWrite)
        {
            performWrite(s -> s.write(toWrite));
        }

        public void writeStart(JsonNode statement, Object tag)
        {
            final String toWrite = statement.toString() + System.lineSeparator();
            performWrite(s -> {
                rolled.add(tag);
                s.write(toWrite);
            });
        }

        public void write(JsonNode statement, StrategySummary summary, Object tag)
        {
            final String toWrite = statement.toString() + System.lineSeparator();
            performWrite(s -> {
                if (!rolled.contains(tag))
                {
                    s.write(toWrite);
                    rolled.add(tag);
                }
            });
        }

        public void close()
        {
            if (stream != null)
            {
                Throwable err = Throwables.close(null, stream);
                if (err != null)
                {
                    JVMStabilityInspector.inspectThrowable(err);
                    logger.error("Failed to close {}: {}", String.format("%s.%s", fileName, fileExt), err);
                }

                stream = null;
            }
        }
    }
}
