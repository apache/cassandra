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

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.lang.ref.WeakReference;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

import com.google.common.collect.MapMaker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.utils.ExecutorUtils;
import org.apache.cassandra.utils.NoSpamLogger;

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.config.CassandraRelevantProperties.LOG_DIR;
import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;

public class CompactionLogger
{
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
    public interface Writer
    {
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
    }

    private interface CompactionStrategyAndTableFunction
    {
        JsonNode apply(AbstractCompactionStrategy strategy, SSTableReader sstable);
    }

    private static final JsonNodeFactory json = JsonNodeFactory.instance;
    private static final Logger logger = LoggerFactory.getLogger(CompactionLogger.class);
    private static final CompactionLogSerializer serializer = new CompactionLogSerializer();
    private final WeakReference<ColumnFamilyStore> cfsRef;
    private final WeakReference<CompactionStrategyManager> csmRef;
    private final AtomicInteger identifier = new AtomicInteger(0);
    private final Map<AbstractCompactionStrategy, String> compactionStrategyMapping = new MapMaker().weakKeys().makeMap();
    private final AtomicBoolean enabled = new AtomicBoolean(false);

    public CompactionLogger(ColumnFamilyStore cfs, CompactionStrategyManager csm)
    {
        csmRef = new WeakReference<>(csm);
        cfsRef = new WeakReference<>(cfs);
    }

    private void forEach(Consumer<AbstractCompactionStrategy> consumer)
    {
        CompactionStrategyManager csm = csmRef.get();
        if (csm == null)
            return;
        csm.getStrategies()
           .forEach(l -> l.forEach(consumer));
    }

    private ArrayNode compactionStrategyMap(Function<AbstractCompactionStrategy, JsonNode> select)
    {
        ArrayNode node = json.arrayNode();
        forEach(acs -> node.add(select.apply(acs)));
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
        node.put("version", sstable.descriptor.version.version);
        node.put("size", sstable.onDiskLength());
        JsonNode logResult = strategy.strategyLogger().sstable(sstable);
        if (logResult != null)
            node.set("details", logResult);
        return node;
    }

    private JsonNode startStrategy(AbstractCompactionStrategy strategy)
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

    private JsonNode shutdownStrategy(AbstractCompactionStrategy strategy)
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

    private void describeStrategy(ObjectNode node)
    {
        ColumnFamilyStore cfs = cfsRef.get();
        if (cfs == null)
            return;
        node.put("keyspace", cfs.getKeyspaceName());
        node.put("table", cfs.getTableName());
        node.put("time", currentTimeMillis());
    }

    private JsonNode startStrategies()
    {
        ObjectNode node = json.objectNode();
        node.put("type", "enable");
        describeStrategy(node);
        node.set("strategies", compactionStrategyMap(this::startStrategy));
        return node;
    }

    public void enable()
    {
        if (enabled.compareAndSet(false, true))
        {
            serializer.writeStart(startStrategies(), this);
        }
    }

    public void disable()
    {
        if (enabled.compareAndSet(true, false))
        {
            ObjectNode node = json.objectNode();
            node.put("type", "disable");
            describeStrategy(node);
            node.set("strategies", compactionStrategyMap(this::shutdownStrategy));
            serializer.write(node, this::startStrategies, this);
        }
    }

    public void flush(Collection<SSTableReader> sstables)
    {
        if (enabled.get())
        {
            ObjectNode node = json.objectNode();
            node.put("type", "flush");
            describeStrategy(node);
            node.set("tables", sstableMap(sstables, this::describeSSTable));
            serializer.write(node, this::startStrategies, this);
        }
    }

    public void compaction(long startTime, Collection<SSTableReader> input, long endTime, Collection<SSTableReader> output)
    {
        if (enabled.get())
        {
            ObjectNode node = json.objectNode();
            node.put("type", "compaction");
            describeStrategy(node);
            node.put("start", String.valueOf(startTime));
            node.put("end", String.valueOf(endTime));
            node.set("input", sstableMap(input, this::describeSSTable));
            node.set("output", sstableMap(output, this::describeSSTable));
            serializer.write(node, this::startStrategies, this);
        }
    }

    public void pending(AbstractCompactionStrategy strategy, int remaining)
    {
        if (remaining != 0 && enabled.get())
        {
            ObjectNode node = json.objectNode();
            node.put("type", "pending");
            describeStrategy(node);
            node.put("strategyId", getId(strategy));
            node.put("pending", remaining);
            serializer.write(node, this::startStrategies, this);
        }
    }

    private static class CompactionLogSerializer implements Writer
    {
        private static final String logDirectory = LOG_DIR.getString();
        private final ExecutorPlus loggerService = executorFactory().sequential("CompactionLogger");
        // This is only accessed on the logger service thread, so it does not need to be thread safe
        private final Set<Object> rolled = new HashSet<>();
        private OutputStreamWriter stream;

        private static OutputStreamWriter createStream() throws IOException
        {
            int count = 0;
            Path compactionLog = new File(logDirectory, "compaction.log").toPath();
            if (Files.exists(compactionLog))
            {
                Path tryPath = compactionLog;
                while (Files.exists(tryPath))
                {
                    tryPath = new File(logDirectory, String.format("compaction-%d.log", count++)).toPath();
                }
                Files.move(compactionLog, tryPath);
            }

            return new OutputStreamWriter(Files.newOutputStream(compactionLog, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE));
        }

        private void writeLocal(String toWrite)
        {
            try
            {
                if (stream == null)
                    stream = createStream();
                stream.write(toWrite);
                stream.flush();
            }
            catch (IOException ioe)
            {
                // We'll drop the change and log the error to the logger.
                NoSpamLogger.log(logger, NoSpamLogger.Level.ERROR, 1, TimeUnit.MINUTES,
                                 "Could not write to the log file: {}", ioe);
            }

        }

        public void writeStart(JsonNode statement, Object tag)
        {
            final String toWrite = statement.toString() + System.lineSeparator();
            loggerService.execute(() -> {
                rolled.add(tag);
                writeLocal(toWrite);
            });
        }

        public void write(JsonNode statement, StrategySummary summary, Object tag)
        {
            final String toWrite = statement.toString() + System.lineSeparator();
            loggerService.execute(() -> {
                if (!rolled.contains(tag))
                {
                    writeLocal(summary.getSummary().toString() + System.lineSeparator());
                    rolled.add(tag);
                }
                writeLocal(toWrite);
            });
        }
    }

    public static void shutdownNowAndWait(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException
    {
        ExecutorUtils.shutdownNowAndWait(timeout, unit, serializer.loggerService);
    }

}
