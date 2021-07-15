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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
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
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.Throwables;

/**
 * This is a Compaction logger that logs compaction events in a file called compactions.log.
 * It was added by CASSANDRA-10805.
 */
public class CompactionLogger
{
    private static final DateTimeFormatter dateFormatter = DateTimeFormatter
                                                           .ofPattern("yyyy-MM-dd' 'HH:mm:ss.SSS")
                                                           .withZone(ZoneId.systemDefault() );

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

    private static final JsonNodeFactory json = JsonNodeFactory.instance;
    private static final Logger logger = LoggerFactory.getLogger(CompactionLogger.class);

    private static final ExecutorService loggerService = Executors.newFixedThreadPool(1, new NamedThreadFactory("compaction-logger"));
    private static final Writer jsonWriter = new CompactionLogSerializer("compaction", "log", loggerService);

    private final String keyspace;
    private final String table;
    private final AtomicInteger identifier = new AtomicInteger(0);
    private final Map<CompactionStrategy, String> compactionStrategyMapping = new MapMaker().weakKeys().makeMap();
    private final Map<CompactionStrategy, Map<String, Writer>> csvWriters = new MapMaker().makeMap();
    private final AtomicBoolean enabled = new AtomicBoolean(false);

    CompactionLogger(TableMetadata metadata)
    {
        this.keyspace = metadata.keyspace;
        this.table = metadata.name;
    }

    void strategyCreated(CompactionStrategy strategy)
    {
        compactionStrategyMapping.computeIfAbsent(strategy, s -> String.valueOf(identifier.getAndIncrement()));
    }

    /**
     * Visit all the strategies.
     *
     * @param consumer a consumer function that receives all the strategies one by one
     */
    private void visitStrategies(Consumer<CompactionStrategy> consumer)
    {
        compactionStrategyMapping.keySet().forEach(consumer);
    }

    /**
     * Rely on {@link this#visitStrategies(Consumer)} to visit all the strategies
     * and add the properties extracted by the function passed in to a json node that is returned.
     *
     * @param select a function that given a strategy returns a json node
     *
     * @return a json node containing information on all the strategies returned by the strategy manager and the function passed in.
     */
    private ArrayNode getStrategiesJsonNode(Function<CompactionStrategy, JsonNode> select)
    {
        ArrayNode node = json.arrayNode();
        visitStrategies(acs -> node.add(select.apply(acs)));
        return node;
    }

    private ArrayNode sstableMap(Collection<SSTableReader> sstables)
    {
        ArrayNode node = json.arrayNode();
        sstables.forEach(t -> node.add(describeSSTable(t)));
        return node;
    }

    private String getId(CompactionStrategy strategy)
    {
        return compactionStrategyMapping.getOrDefault(strategy, "-1"); // there should always be a strategy because of strategyCreated()
    }

    private JsonNode formatSSTables(CompactionStrategy strategy)
    {
        ArrayNode node = json.arrayNode();
        for (SSTableReader sstable : strategy.getSSTables())
            node.add(formatSSTable(sstable));

        return node;
    }

    private JsonNode formatSSTable(SSTableReader sstable)
    {
        ObjectNode node = json.objectNode();
        node.put("generation", sstable.descriptor.generation);
        node.put("version", sstable.descriptor.version.getVersion());
        node.put("size", sstable.onDiskLength());

        // The details are only relevant or available for some strategies, e.g. LCS or Date tiered but
        // it doesn't hurt to log them all the time in order to simplify things
        ObjectNode details = json.objectNode();
        details.put("level", sstable.getSSTableLevel());
        details.put("min_token", sstable.first.getToken().toString());
        details.put("max_token", sstable.last.getToken().toString());
        details.put("min_timestamp", sstable.getMinTimestamp());
        details.put("max_timestamp", sstable.getMaxTimestamp());

        node.put("details", details);

        return node;
    }

    private JsonNode getStrategyDetails(CompactionStrategy strategy)
    {
        ObjectNode node = json.objectNode();
        node.put("strategyId", getId(strategy));
        node.put("type", strategy.getName());
        node.set("tables", formatSSTables(strategy));
        return node;
    }

    private JsonNode getStrategyId(CompactionStrategy strategy)
    {
        ObjectNode node = json.objectNode();
        node.put("strategyId", getId(strategy));
        return node;
    }

    private JsonNode describeSSTable(SSTableReader sstable)
    {
        ObjectNode node = json.objectNode();
        node.put("table", formatSSTable(sstable));
        return node;
    }

    private void maybeAddSchemaAndTimeInfo(ObjectNode node)
    {
        node.put("keyspace", keyspace);
        node.put("table", table);
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

            visitStrategies(strategy -> csvWriters.computeIfPresent(strategy, (s, writers) -> { writers.values().forEach(Writer::close); return null; }));
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
            node.set("tables", sstableMap(sstables));
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
            node.set("input", sstableMap(input));
            node.set("output", sstableMap(output));
            jsonWriter.write(node, this::getEventJsonNode, this);
        }
    }

    public void pending(CompactionStrategy strategy, int remaining)
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
    public void statistics(CompactionStrategy strategy, String event, CompactionStrategyStatistics statistics)
    {
        if (logger.isTraceEnabled())
            logger.trace("Compaction statistics for strategy {} and event {}: {}", strategy, event, statistics);

        if (!enabled.get())
            return;

        for (CompactionAggregateStatistics aggregateStatistics : statistics.aggregates())
        {
            Writer writer = getCsvWriter(strategy, statistics.getHeader(), aggregateStatistics);
            writer.write(String.join(",", Iterables.concat(ImmutableList.of(currentTime(), event), aggregateStatistics.data())) + System.lineSeparator());
        }
    }

    private Writer getCsvWriter(CompactionStrategy strategy, Collection<String> header, CompactionAggregateStatistics statistics)
    {
        Map<String, Writer> writers = csvWriters.get(strategy);
        if (writers == null)
        {
            writers = new MapMaker().makeMap();
            if (csvWriters.putIfAbsent(strategy, writers) != null)
            {
                writers = csvWriters.get(strategy);
            }
        }

        String shard = statistics.shard();
        Writer writer = writers.get(shard);
        if (writer != null)
            return writer;

        String fileName = String.format("compaction-%s-%s-%s-%s",
                                        strategy.getName(),
                                        keyspace,
                                        table,
                                        getId(strategy));

        if (!shard.isEmpty())
            fileName += '-' + shard;

        writer = new CompactionLogSerializer(fileName, "csv", loggerService);
        if (writers.putIfAbsent(shard, writer) == null)
        {
            writer.write(String.join(",", Iterables.concat(ImmutableList.of("Timestamp", "Event"), header)) + System.lineSeparator());
            return writer;
        }
        else
        {
            writer.close();
            return writers.get(shard);
        }
    }

    private String currentTime()
    {
        return dateFormatter.format(Instant.ofEpochMilli(System.currentTimeMillis()));
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
