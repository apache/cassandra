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

package org.apache.cassandra.distributed.test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.management.LockInfo;
import java.lang.management.ManagementFactory;
import java.lang.management.MonitorInfo;
import java.lang.management.ThreadInfo;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import javax.annotation.Nullable;
import javax.management.openmbean.CompositeData;

import com.github.benmanes.caffeine.cache.Cache;
import com.google.common.util.concurrent.Uninterruptibles;

import org.junit.Test;

import accord.utils.DefaultRandom;
import accord.utils.Property.Command;
import accord.utils.Property.StateOnlyCommand;
import accord.utils.RandomSource;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compression.CompressionDictionary;
import org.apache.cassandra.db.compression.CompressionDictionary.DictId;
import org.apache.cassandra.db.compression.CompressionDictionary.Kind;
import org.apache.cassandra.db.compression.CompressionDictionaryDetailsTabularData;
import org.apache.cassandra.db.compression.CompressionDictionaryFuzzing.Compression;
import org.apache.cassandra.db.compression.CompressionDictionaryFuzzing.TableSpec;
import org.apache.cassandra.db.compression.CompressionDictionaryFuzzing.WriteRow;
import org.apache.cassandra.db.compression.CompressionDictionaryManager;
import org.apache.cassandra.db.compression.TrainingState;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.LogResult;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.SystemDistributedKeyspace;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.concurrent.Ref;

import static accord.utils.Property.commands;
import static accord.utils.Property.stateful;
import static java.lang.String.format;
import static org.apache.cassandra.db.compression.CompressionDictionaryFuzzing.DATA_PER_SAMPLE_SIZE;
import static org.apache.cassandra.db.compression.CompressionDictionaryFuzzing.ROWS_PER_PARTITION;
import static org.apache.cassandra.db.compression.CompressionDictionaryFuzzing.checkCached;
import static org.apache.cassandra.db.compression.CompressionDictionaryFuzzing.checkCompressors;
import static org.apache.cassandra.db.compression.CompressionDictionaryFuzzing.compressorCache;
import static org.apache.cassandra.db.compression.CompressionDictionaryFuzzing.copy;
import static org.apache.cassandra.db.compression.CompressionDictionaryFuzzing.describeEviction;
import static org.apache.cassandra.db.compression.CompressionDictionaryFuzzing.dictionaryCache;
import static org.apache.cassandra.db.compression.CompressionDictionaryFuzzing.difference;
import static org.apache.cassandra.db.compression.CompressionDictionaryFuzzing.evict;
import static org.apache.cassandra.db.compression.CompressionDictionaryFuzzing.isNotNewer;
import static org.apache.cassandra.db.compression.CompressionDictionaryFuzzing.isReleased;
import static org.apache.cassandra.db.compression.CompressionDictionaryFuzzing.key;
import static org.apache.cassandra.db.compression.CompressionDictionaryFuzzing.messageStartsWith;
import static org.apache.cassandra.db.compression.CompressionDictionaryFuzzing.partitionKey;
import static org.apache.cassandra.db.compression.CompressionDictionaryFuzzing.randomRows;
import static org.apache.cassandra.db.compression.CompressionDictionaryFuzzing.topUpRows;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.io.compress.IDictionaryCompressor.TRAINING_MAX_DICTIONARY_SIZE_PARAMETER_NAME;
import static org.apache.cassandra.io.compress.IDictionaryCompressor.TRAINING_MAX_TOTAL_SAMPLE_SIZE_PARAMETER_NAME;
import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Runs the steps of {@code CompressionDictionaryFuzzTest} on a cluster of 3 nodes with a keyspace replicated to all of
 * them, on 15 to 20 tables. Writes and deletes have consistency level ALL, so every node has all rows; reads check the
 * rows of one node, or read them with consistency level ALL. Each step runs on a random node: training, importing a
 * dictionary, flushes, compactions, evictions, restoring sstables. On top of that, a node can lose the rows of a table
 * and get them back by a repair, which streams parts of sstables, or restart.
 * <p>
 * A dictionary trained or imported on a node must reach the other nodes: the node announces it with a message, and
 * each other node then reads it from the system table. Nodes 1 and 2 never refresh their dictionaries from the system
 * table, so they learn about a new dictionary only from that message; node 3 also refreshes them every second.
 * <p>
 * Before each step, the test checks on each node that no cache holds a released dictionary, that the dictionary cache
 * of a dropped table stays empty, and that nothing logged an error or a warning about dictionaries. At the end, all
 * tables are read once more on each node and dropped, and every dictionary each node saw in a cache must be released.
 */
public class CompressionDictionaryClusterFuzzTest extends TestBaseImpl
{
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CompressionDictionaryClusterFuzzTest.class);

    private static final int NODES = 3;
    private static final int MIN_TABLES = 15;
    private static final int MAX_TABLES = 20;
    private static final int STEPS = 300;

    private static final int COMPRESSOR_CACHE_SIZE = 3;
    private static final String COMPRESSOR_CACHE_EXPIRE = "2s";
    // at least 5 seconds, so that a node does not expire a new dictionary before the test sees that it got it
    private static final Integer[] DICTIONARY_CACHE_SIZES = { 2, 3, 10 };
    private static final String[] DICTIONARY_CACHE_EXPIRES = { "5s", "10s", "1h" };
    // the only node which refreshes dictionaries from the system table
    private static final int REFRESHING_NODE = 3;
    private static final Duration DICTIONARY_DISPERSION_TIMEOUT = Duration.ofSeconds(10);

    // rows written together in an unlogged batch
    private static final int BATCH_SIZE = 20;

    @Test
    public void fuzz() throws IOException
    {
        try (Cluster cluster = Cluster.build(NODES)
                                      .withConfig(c -> c.with(NETWORK, GOSSIP)
                                                        .set("compression_dictionary_cache_size", COMPRESSOR_CACHE_SIZE)
                                                        .set("compression_dictionary_cache_expire", COMPRESSOR_CACHE_EXPIRE)
                                                        .set("compression_dictionary_refresh_initial_delay", c.num() == REFRESHING_NODE ? "1s" : "1h")
                                                        .set("compression_dictionary_refresh_interval", c.num() == REFRESHING_NODE ? "1s" : "1h")
                                                        // a repair streams parts of sstables, with their dictionaries
                                                        .set("stream_entire_sstables", false)
                                                        .set("batch_size_warn_threshold", "1024KiB")
                                                        .set("batch_size_fail_threshold", "4096KiB")
                                                        .set("unlogged_batch_across_partitions_warn_threshold", 1000)
                                                        // the nodes share the CPUs of one JVM with trainings and compactions, which
                                                        // slows them down for seconds; a node which stops responding still fails
                                                        .set("write_request_timeout", "30s")
                                                        .set("read_request_timeout", "30s")
                                                        .set("range_request_timeout", "30s")
                                                        .set("truncate_request_timeout", "60s"))
                                      .start())
        {
            stateful()
            .withExamples(1)
            .withSteps(STEPS)
            .withStepTimeout(Duration.ofMinutes(5))
            .check(commands(() -> rs -> new State(rs, cluster))
                   .preCommands(State::checkInvariants)
                   .add(20, (rs, state) -> command(writeOperation(rs, state.pick(rs))))
                   .add(5, (rs, state) -> command(deleteOperation(rs, state.pick(rs))))
                   .add(15, (rs, state) -> command(readOperation(rs, state.pick(rs))))
                   .add(8, CompressionDictionaryClusterFuzzTest::flushTables)
                   .add(5, (rs, state) -> command(compactOperation(rs, state.pick(rs))))
                   .add(2, CompressionDictionaryClusterFuzzTest::toggleAutoCompaction)
                   .add(2, (rs, state) -> command("Read all tables on all nodes", State::verifyAll))
                   .add(6, CompressionDictionaryClusterFuzzTest::alterCompression)
                   .addIf(State::hasDictionaryTable, 8, (rs, state) -> command(trainOperation(rs, state.pickDictionaryTable(rs))))
                   .addIf(State::hasDictionaryTable, 3, CompressionDictionaryClusterFuzzTest::importDictionary)
                   .addIf(state -> state.tables.size() < MAX_TABLES, 3, CompressionDictionaryClusterFuzzTest::createNewTable)
                   .addIf(state -> state.tables.size() > MIN_TABLES, 3, CompressionDictionaryClusterFuzzTest::dropOneTable)
                   .add(2, CompressionDictionaryClusterFuzzTest::dropAndRestore)
                   .add(2, (rs, state) -> command(reloadOperation(rs, state.pick(rs))))
                   .add(3, (rs, state) -> command(repairOperation(rs, state.pick(rs))))
                   .add(1, CompressionDictionaryClusterFuzzTest::truncateTable)
                   .add(3, (rs, state) -> command(evictDictionariesOperation(rs, state.pick(rs))))
                   .add(3, (rs, state) -> command(evictCompressorsOperation(rs)))
                   .add(1, CompressionDictionaryClusterFuzzTest::waitForExpiry)
                   .add(2, CompressionDictionaryClusterFuzzTest::changeFlushCompression)
                   .add(1, CompressionDictionaryClusterFuzzTest::restartNode)
                   .add(5, CompressionDictionaryClusterFuzzTest::concurrently)
                   .destroyState((state, failure) -> state.destroy(failure))
                   .build());
        }
    }

    private static int node(RandomSource rs)
    {
        return rs.nextInt(1, NODES + 1);
    }

    private static Command<State, Void, ?> flushTables(RandomSource rs, State state)
    {
        List<TableModel> tables = state.pickSome(rs, rs.nextInt(1, 4));
        int node = rs.nextInt(0, NODES + 1);
        return command(format("Flush %s on %s", names(tables), node == 0 ? "all nodes" : "node " + node), s -> {
            for (TableModel table : tables)
                s.flush(table, node);
        });
    }

    private static Command<State, Void, ?> toggleAutoCompaction(RandomSource rs, State state)
    {
        TableModel table = state.pick(rs);
        int node = node(rs);
        boolean enable = rs.nextBoolean();
        return command(format("%s automatic compaction of %s on node %d", enable ? "Enable" : "Disable", table.name, node),
                       s -> s.onNode(node, table, (ks, t) -> OnNode.setAutoCompaction(ks, t, enable)));
    }

    private static Command<State, Void, ?> alterCompression(RandomSource rs, State state)
    {
        TableModel table = state.pick(rs);
        // changing only the level keeps the dictionary of the table
        Compression compression = table.compression.hasDictionary() && rs.nextBoolean()
                                  ? table.compression.withLevel(rs.nextInt(-3, 20))
                                  : Compression.random(rs);
        return command(format("Alter compression of %s to %s", table.name, compression), s -> s.alter(table, compression));
    }

    private static Command<State, Void, ?> importDictionary(RandomSource rs, State state)
    {
        // the target gets the dictionary with the id it has in the source, so two tables have equal dictionaries
        TableModel source = state.pick(rs);
        TableModel target = state.pickDictionaryTable(rs);
        int node = node(rs);
        return command(format("Import the current dictionary of %s into %s on node %d", source.name, target.name, node),
                       s -> s.importDictionary(source, target, node));
    }

    private static Command<State, Void, ?> createNewTable(RandomSource rs, State state)
    {
        String name = !state.droppedNames.isEmpty() && rs.nextBoolean() ? rs.pick(state.droppedNames) : state.newName();
        TableSpec spec = TableSpec.random(rs, name, DICTIONARY_CACHE_SIZES, DICTIONARY_CACHE_EXPIRES);
        List<WriteRow> rows = randomRows(rs, rs.nextInt(0, 2000));
        int node = node(rs);
        return command(format("Create table %s with %d rows", spec, rows.size()), s -> {
            TableModel table = s.create(spec);
            s.write(table, rows, node);
            s.flush(table, 0);
        });
    }

    private static Command<State, Void, ?> dropOneTable(RandomSource rs, State state)
    {
        TableModel table = state.pick(rs);
        return command("Drop table " + table.name, s -> s.drop(table));
    }

    private static Command<State, Void, ?> dropAndRestore(RandomSource rs, State state)
    {
        TableModel table = state.pick(rs);
        TableSpec spec = rs.nextBoolean() ? table.spec : TableSpec.random(rs, table.name, DICTIONARY_CACHE_SIZES, DICTIONARY_CACHE_EXPIRES);
        boolean copy = rs.nextBoolean();
        return command(format("Copy the sstables of %s on each node, drop it, create it as %s and import the copies on each node%s",
                              table.name, spec, copy ? "" : " by moving them"),
                       s -> s.dropAndRestore(table, spec, copy));
    }

    private static Command<State, Void, ?> truncateTable(RandomSource rs, State state)
    {
        TableModel table = state.pick(rs);
        return command("Truncate " + table.name, s -> s.truncate(table));
    }

    private static Command<State, Void, ?> waitForExpiry(RandomSource rs, State state)
    {
        int millis = rs.nextInt(1000, 6000);
        return command(format("Wait %d ms for cached dictionaries and compressors to expire", millis), s -> {
            Uninterruptibles.sleepUninterruptibly(millis, TimeUnit.MILLISECONDS);
            String keyspace = s.keyspace;
            for (int node = 1; node <= NODES; node++)
                s.cluster.get(node).runOnInstance(() -> OnNode.cleanUpCaches(keyspace));
        });
    }

    private static Command<State, Void, ?> changeFlushCompression(RandomSource rs, State state)
    {
        String flushCompression = rs.pick(Config.FlushCompression.values()).name();
        int node = node(rs);
        return command(format("Set flush_compression of node %d to %s", node, flushCompression),
                       s -> s.cluster.get(node).runOnInstance(() -> OnNode.setFlushCompression(flushCompression)));
    }

    private static Command<State, Void, ?> restartNode(RandomSource rs, State state)
    {
        int node = node(rs);
        return command("Restart node " + node, s -> s.restart(node));
    }

    private static Command<State, Void, ?> concurrently(RandomSource rs, State state)
    {
        List<Operation> operations = new ArrayList<>();
        for (TableModel table : state.pickSome(rs, rs.nextInt(2, 6)))
        {
            switch (rs.nextInt(9))
            {
                case 0: operations.add(writeOperation(rs, table)); break;
                case 1: operations.add(deleteOperation(rs, table)); break;
                case 2: operations.add(readOperation(rs, table)); break;
                case 3: operations.add(compactOperation(rs, table)); break;
                case 4: operations.add(reloadOperation(rs, table)); break;
                case 5: operations.add(evictDictionariesOperation(rs, table)); break;
                case 6: operations.add(repairOperation(rs, table)); break;
                default:
                    operations.add(table.compression.hasDictionary() ? trainOperation(rs, table) : writeOperation(rs, table));
            }
        }
        if (rs.nextBoolean())
            operations.add(evictCompressorsOperation(rs));

        String description = operations.stream().map(operation -> operation.description).collect(Collectors.joining("; ", "Concurrently: ", ""));
        return command(description, s -> s.runConcurrently(operations));
    }

    private static Operation writeOperation(RandomSource rs, TableModel table)
    {
        List<WriteRow> rows = randomRows(rs, rs.nextInt(1, 400));
        int node = node(rs);
        // 0 flushes on all nodes, -1 does not flush
        int flushOn = rs.decide(0.3f) ? rs.nextInt(0, NODES + 1) : -1;
        return new Operation(format("Write %d rows into %s via node %d%s", rows.size(), table.name, node,
                                    flushOn < 0 ? "" : flushOn == 0 ? " and flush on all nodes" : " and flush on node " + flushOn), s -> {
            s.write(table, rows, node);
            if (flushOn >= 0)
                s.flush(table, flushOn);
        });
    }

    private static Operation deleteOperation(RandomSource rs, TableModel table)
    {
        int pk = partitionKey(rs, table.rows);
        int node = node(rs);
        switch (rs.nextInt(3))
        {
            case 0:
            {
                int ck = rs.nextInt(ROWS_PER_PARTITION);
                return new Operation(format("Delete row %d:%d of %s via node %d", pk, ck, table.name, node),
                                     s -> s.delete(table, node, "pk = ? AND ck = ?", key(pk, ck), key(pk, ck + 1), pk, ck));
            }
            case 1:
                return new Operation(format("Delete partition %d of %s via node %d", pk, table.name, node),
                                     s -> s.delete(table, node, "pk = ?", key(pk, 0), key(pk + 1, 0), pk));
            default:
            {
                int from = rs.nextInt(ROWS_PER_PARTITION);
                int to = rs.nextInt(from + 1, ROWS_PER_PARTITION + 1);
                return new Operation(format("Delete rows %d:[%d, %d) of %s via node %d", pk, from, to, table.name, node),
                                     s -> s.delete(table, node, "pk = ? AND ck >= ? AND ck < ?", key(pk, from), key(pk, to), pk, from, to));
            }
        }
    }

    private static Operation readOperation(RandomSource rs, TableModel table)
    {
        int pk = partitionKey(rs, table.rows);
        int node = node(rs);
        // reads the rows of the node, or reads them with consistency level ALL
        boolean local = rs.nextBoolean();
        String how = local ? " on node " + node : " via node " + node + " with consistency level ALL";
        switch (rs.nextInt(4))
        {
            case 0:
                return new Operation(format("Read partition %d of %s%s", pk, table.name, how),
                                     s -> s.verify(table, node, local, "WHERE pk = ?", key(pk, 0), key(pk + 1, 0), pk));
            case 1:
                return new Operation(format("Read partition %d of %s in reverse%s", pk, table.name, how),
                                     s -> s.verify(table, node, local, "WHERE pk = ? ORDER BY ck DESC", key(pk, 0), key(pk + 1, 0), pk));
            case 2:
            {
                int from = rs.nextInt(ROWS_PER_PARTITION);
                int to = rs.nextInt(from + 1, ROWS_PER_PARTITION + 1);
                return new Operation(format("Read rows %d:[%d, %d) of %s%s", pk, from, to, table.name, how),
                                     s -> s.verify(table, node, local, "WHERE pk = ? AND ck >= ? AND ck < ?", key(pk, from), key(pk, to), pk, from, to));
            }
            default:
                return new Operation(format("Read all rows of %s%s", table.name, how),
                                     s -> s.verify(table, node, local, "", Long.MIN_VALUE, Long.MAX_VALUE));
        }
    }

    private static Operation compactOperation(RandomSource rs, TableModel table)
    {
        int node = node(rs);
        return new Operation(format("Compact %s on node %d", table.name, node), s -> s.onNode(node, table, OnNode::compact));
    }

    private static Operation trainOperation(RandomSource rs, TableModel table)
    {
        int node = node(rs);
        boolean force = rs.nextBoolean();
        boolean await = rs.decide(0.8f);
        Compression compression = table.compression;
        int maxTotalSampleSizeInKiB = compression.maxTotalSampleSizeInKiB;
        Map<String, String> parameters = new HashMap<>();
        if (rs.nextBoolean())
        {
            maxTotalSampleSizeInKiB = Compression.maxTotalSampleSizeInKiB(rs, compression.chunkLengthInKiB);
            parameters.put(TRAINING_MAX_DICTIONARY_SIZE_PARAMETER_NAME, Compression.maxDictionarySizeInKiB(rs, maxTotalSampleSizeInKiB) + "KiB");
            parameters.put(TRAINING_MAX_TOTAL_SAMPLE_SIZE_PARAMETER_NAME, maxTotalSampleSizeInKiB + "KiB");
        }
        long minDataSize = (long) (DATA_PER_SAMPLE_SIZE * maxTotalSampleSizeInKiB * 1024);
        long seed = rs.nextLong();
        return new Operation(format("Train a dictionary for %s on node %d%s%s%s", table.name, node, force ? " forcibly" : "",
                                    parameters.isEmpty() ? "" : " with " + parameters, await ? "" : " without waiting for it"),
                             s -> {
                                 s.topUp(table, node, minDataSize, new DefaultRandom(seed));
                                 s.train(table, node, force, parameters, await);
                             });
    }

    private static Operation reloadOperation(RandomSource rs, TableModel table)
    {
        int node = node(rs);
        boolean copy = rs.nextBoolean();
        return new Operation(format("Copy the sstables of %s on node %d, truncate it there and import the copies%s",
                                    table.name, node, copy ? "" : " by moving them"),
                             s -> s.reload(table, node, copy));
    }

    private static Operation repairOperation(RandomSource rs, TableModel table)
    {
        int node = node(rs);
        return new Operation(format("Truncate %s on node %d only and repair it there", table.name, node), s -> s.repair(table, node));
    }

    private static Operation evictDictionariesOperation(RandomSource rs, TableModel table)
    {
        int node = node(rs);
        int how = rs.nextInt(3);
        int index = rs.nextInt(Integer.MAX_VALUE);
        return new Operation(describeEviction(how, format("the dictionary cache of %s on node %d", table.name, node)),
                             s -> s.onNode(node, table, (ks, t) -> OnNode.evictDictionaries(ks, t, how, index)));
    }

    private static Operation evictCompressorsOperation(RandomSource rs)
    {
        int node = node(rs);
        int how = rs.nextInt(3);
        int index = rs.nextInt(Integer.MAX_VALUE);
        return new Operation(describeEviction(how, "the compressor cache on node " + node),
                             s -> s.cluster.get(node).runOnInstance(() -> OnNode.evictCompressors(how, index)));
    }

    private static String names(List<TableModel> tables)
    {
        return tables.stream().map(table -> table.name).collect(Collectors.joining(", "));
    }

    /**
     * Errors, and warnings about dictionaries, except the ones expected in a cluster whose tables are dropped and whose
     * nodes restart.
     */
    private static boolean isUnexpected(String line)
    {
        if (line.startsWith("ERROR"))
            // Ref's strong reference leak detector reports every live sstable it reaches from global singletons
            return !line.contains("Strong self-ref loop detected");
        if (line.startsWith("WARN"))
            return line.contains("ictionar")
                   // a node announced a dictionary of a table which is dropped, or of a node which restarts
                   && !line.contains("Received dictionary update for unknown table")
                   && !line.contains("Failed to send dictionary update notification");
        return false;
    }

    private interface Action
    {
        void run(State state) throws Exception;
    }

    private static class Operation
    {
        final String description;
        final Action action;

        Operation(String description, Action action)
        {
            this.description = description;
            this.action = action;
        }
    }

    private static Command<State, Void, ?> command(Operation operation)
    {
        return command(operation.description, operation.action);
    }

    private static Command<State, Void, ?> command(String description, Action action)
    {
        return new StateOnlyCommand<>()
        {
            @Override
            public void applyUnit(State state) throws Throwable
            {
                action.run(state);
            }

            @Override
            public String detailed(State state)
            {
                return description;
            }
        };
    }

    private static class TableModel
    {
        final String name;
        final TableSpec spec;
        // the rows the table must have, by key(pk, ck)
        final NavigableMap<Long, String> rows = new TreeMap<>();
        volatile Compression compression;

        TableModel(TableSpec spec)
        {
            this.name = spec.name;
            this.spec = spec;
            this.compression = spec.compression;
        }

        @Override
        public String toString()
        {
            return format("%s (compression %s)", name, compression);
        }
    }

    private interface TableAction extends java.io.Serializable
    {
        void run(String keyspace, String table);
    }

    private static class State
    {
        private static final AtomicInteger runs = new AtomicInteger();

        final Cluster cluster;
        final String keyspace = "dictionary_fuzz_" + runs.incrementAndGet();
        final Map<String, TableModel> tables = new LinkedHashMap<>();
        final List<String> droppedNames = new ArrayList<>();
        final ExecutorService executor = Executors.newFixedThreadPool(8);
        final AtomicInteger backupCount = new AtomicInteger();
        // timestamps of writes and deletes, so that rows written via different nodes are ordered as the test wrote them
        final AtomicLong timestamp = new AtomicLong(currentTimeMillis() * 1000);
        final long[] logMarks = new long[NODES + 1];
        final Path backups;
        int tableCount;

        State(RandomSource rs, Cluster cluster)
        {
            this.cluster = cluster;
            try
            {
                backups = Files.createTempDirectory("dictionary_cluster_fuzz");
            }
            catch (IOException e)
            {
                throw new UncheckedIOException(e);
            }
            for (int node = 1; node <= NODES; node++)
                logMarks[node] = cluster.get(node).logs().mark();

            cluster.schemaChange(format("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': %d}", keyspace, NODES));
            for (int i = rs.nextInt(MIN_TABLES, MAX_TABLES + 1); i > 0; i--)
            {
                TableModel table = create(TableSpec.random(rs, newName(), DICTIONARY_CACHE_SIZES, DICTIONARY_CACHE_EXPIRES));
                write(table, randomRows(rs, rs.nextInt(500, 3000)), node(rs));
                flush(table, 0);
                logger.info("Created {}", table.spec);
            }
        }

        String newName()
        {
            return "t" + tableCount++;
        }

        TableModel pick(RandomSource rs)
        {
            return rs.pick(new ArrayList<>(tables.values()));
        }

        boolean hasDictionaryTable()
        {
            return tables.values().stream().anyMatch(table -> table.compression.hasDictionary());
        }

        TableModel pickDictionaryTable(RandomSource rs)
        {
            return rs.pick(tables.values().stream().filter(table -> table.compression.hasDictionary()).collect(Collectors.toList()));
        }

        List<TableModel> pickSome(RandomSource rs, int count)
        {
            List<TableModel> candidates = new ArrayList<>(tables.values());
            for (int i = candidates.size() - 1; i > 0; i--)
                Collections.swap(candidates, i, rs.nextInt(i + 1));
            return candidates.subList(0, Math.min(count, candidates.size()));
        }

        void onNode(int node, TableModel table, TableAction action)
        {
            String keyspace = this.keyspace;
            String name = table.name;
            cluster.get(node).runOnInstance(() -> action.run(keyspace, name));
        }

        TableModel create(TableSpec spec)
        {
            String keyspace = this.keyspace;
            String name = spec.name;
            int cacheSize = spec.cacheSize;
            String cacheExpire = spec.cacheExpire;
            // the dictionary cache of a table takes its configuration when a node creates the table
            for (int node = 1; node <= NODES; node++)
                cluster.get(node).runOnInstance(() -> OnNode.configureNextTable(cacheSize, cacheExpire));
            cluster.schemaChange(spec.createStatement(keyspace));
            if (!spec.autoCompaction)
            {
                for (int node = 1; node <= NODES; node++)
                    cluster.get(node).runOnInstance(() -> OnNode.setAutoCompaction(keyspace, name, false));
            }
            TableModel table = new TableModel(spec);
            tables.put(table.name, table);
            droppedNames.removeIf(table.name::equals);
            return table;
        }

        void drop(TableModel table)
        {
            String description = table + ", dropped";
            for (int node = 1; node <= NODES; node++)
                onNode(node, table, (ks, t) -> OnNode.rememberDroppedTable(ks, t, description));
            cluster.schemaChange(format("DROP TABLE %s.%s", keyspace, table.name));
            tables.remove(table.name);
            droppedNames.add(table.name);
        }

        void alter(TableModel table, Compression compression)
        {
            cluster.schemaChange(format("ALTER TABLE %s.%s WITH compression = %s", keyspace, table.name, compression));
            table.compression = compression;
        }

        /**
         * Writes the rows in unlogged batches with consistency level ALL. Each row has its own timestamp, so the last
         * one of rows with the same key wins, also in a batch.
         */
        void write(TableModel table, List<WriteRow> rows, int node)
        {
            String insert = format("INSERT INTO %s.%s (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP ?; ", keyspace, table.name);
            for (int from = 0; from < rows.size(); from += BATCH_SIZE)
            {
                List<WriteRow> batch = rows.subList(from, Math.min(from + BATCH_SIZE, rows.size()));
                StringBuilder query = new StringBuilder("BEGIN UNLOGGED BATCH ");
                List<Object> values = new ArrayList<>();
                for (WriteRow row : batch)
                {
                    query.append(insert);
                    Collections.addAll(values, row.pk, row.ck, row.value, timestamp.incrementAndGet());
                }
                query.append("APPLY BATCH");
                cluster.coordinator(node).execute(query.toString(), ConsistencyLevel.ALL, values.toArray());
                for (WriteRow row : batch)
                    table.rows.put(key(row.pk, row.ck), row.value);
            }
        }

        /**
         * Deletes the rows matching the restriction with consistency level ALL; the rows from a key up to another key
         * are the ones it deletes.
         */
        void delete(TableModel table, int node, String restriction, long fromKey, long toKey, Object... values)
        {
            Object[] withTimestamp = new Object[values.length + 1];
            withTimestamp[0] = timestamp.incrementAndGet();
            System.arraycopy(values, 0, withTimestamp, 1, values.length);
            cluster.coordinator(node).execute(format("DELETE FROM %s.%s USING TIMESTAMP ? WHERE %s", keyspace, table.name, restriction),
                                              ConsistencyLevel.ALL, withTimestamp);
            table.rows.subMap(fromKey, true, toKey, false).clear();
        }

        /**
         * Flushes the table on a node, or on all nodes for 0.
         */
        void flush(TableModel table, int node)
        {
            for (int n = node == 0 ? 1 : node; n <= (node == 0 ? NODES : node); n++)
                onNode(n, table, OnNode::flush);
        }

        void truncate(TableModel table)
        {
            cluster.coordinator(1).execute(format("TRUNCATE %s.%s", keyspace, table.name), ConsistencyLevel.ALL);
            table.rows.clear();
        }

        /**
         * Writes rows into the table until its sstables on the node have at least the given amount of uncompressed
         * data, so that a training on the node samples enough chunks from them. The node compacts the table, so that
         * all its sstables have the chunks of its current compression.
         */
        void topUp(TableModel table, int node, long minDataSize, RandomSource rs)
        {
            String keyspace = this.keyspace;
            String name = table.name;
            for (int round = 0; ; round++)
            {
                long dataSize = cluster.get(node).callOnInstance(() -> OnNode.compactAndMeasure(keyspace, name));
                if (dataSize >= minDataSize)
                    return;
                if (round == 50)
                    throw new AssertionError(format("%s has only %d bytes of data on node %d after writing to it %d times, %d bytes are needed for training",
                                                    table, dataSize, node, round, minDataSize));
                write(table, topUpRows(rs, minDataSize - dataSize), node);
            }
        }

        void train(TableModel table, int node, boolean force, Map<String, String> parameters, boolean await)
        {
            String keyspace = this.keyspace;
            String name = table.name;
            HashMap<String, String> trainingParameters = new HashMap<>(parameters);
            boolean started = cluster.get(node).callOnInstance(() -> OnNode.train(keyspace, name, force, trainingParameters));
            // a training which was not waited for still runs on the node
            if (!started || !await)
                return;

            long deadline = nanoTime() + TimeUnit.MINUTES.toNanos(1);
            String training;
            while ((training = cluster.get(node).callOnInstance(() -> OnNode.trainingState(keyspace, name))).startsWith("SAMPLING")
                   || training.startsWith("TRAINING") || training.startsWith("NOT_STARTED"))
            {
                if (nanoTime() > deadline)
                    throw new AssertionError(format("training a dictionary for %s on node %d did not finish within a minute, it is %s", table, node, training));
                Uninterruptibles.sleepUninterruptibly(50, TimeUnit.MILLISECONDS);
            }
            assertThat(training).as("training a dictionary for %s on node %d", table, node).isEqualTo("COMPLETED");

            long dictId = cluster.get(node).callOnInstance(() -> OnNode.latestStoredDictionaryId(keyspace, name));
            assertThat(dictId).as("id of the dictionary stored for %s after the training on node %d completed", table, node).isNotNegative();
            awaitDictionary(table, node, dictId);
        }

        void importDictionary(TableModel source, TableModel target, int node)
        {
            String keyspace = this.keyspace;
            String sourceName = source.name;
            String targetName = target.name;
            long dictId = cluster.get(node).callOnInstance(() -> OnNode.importDictionary(keyspace, sourceName, targetName));
            // negative when the source has no current dictionary on the node, or the target has a newer one
            if (dictId >= 0)
                awaitDictionary(target, node, dictId);
        }

        /**
         * Waits until each node but the one which trained or imported the dictionary has it, or a newer one.
         */
        void awaitDictionary(TableModel table, int source, long dictId)
        {
            String keyspace = this.keyspace;
            String name = table.name;
            for (int node = 1; node <= NODES; node++)
            {
                if (node == source)
                    continue;
                long deadline = nanoTime() + DICTIONARY_DISPERSION_TIMEOUT.toNanos();
                while (!cluster.get(node).callOnInstance(() -> OnNode.knowsDictionary(keyspace, name, dictId)))
                {
                    if (nanoTime() > deadline)
                        throw new AssertionError(format("node %d does not have the dictionary %d of %s %s after node %d stored it",
                                                        node, dictId, table, DICTIONARY_DISPERSION_TIMEOUT, source));
                    Uninterruptibles.sleepUninterruptibly(50, TimeUnit.MILLISECONDS);
                }
            }
        }

        void reload(TableModel table, int node, boolean copy)
        {
            String directory = backup(table, node);
            onNode(node, table, OnNode::truncateLocally);
            if (directory != null)
                importSSTables(table, node, directory, copy);
            verify(table, node, true, "", Long.MIN_VALUE, Long.MAX_VALUE);
        }

        void repair(TableModel table, int node)
        {
            onNode(node, table, OnNode::truncateLocally);
            cluster.get(node).nodetoolResult("repair", "--full", keyspace, table.name).asserts().success();
            verify(table, node, true, "", Long.MIN_VALUE, Long.MAX_VALUE);
        }

        void dropAndRestore(TableModel table, TableSpec spec, boolean copy)
        {
            String[] directories = new String[NODES + 1];
            for (int node = 1; node <= NODES; node++)
                directories[node] = backup(table, node);
            drop(table);
            TableModel restored = create(spec);
            restored.rows.putAll(table.rows);
            for (int node = 1; node <= NODES; node++)
            {
                if (directories[node] != null)
                    importSSTables(restored, node, directories[node], copy);
            }
            for (int node = 1; node <= NODES; node++)
                verify(restored, node, true, "", Long.MIN_VALUE, Long.MAX_VALUE);
        }

        /**
         * @return the directory with copies of the sstables of the table on the node, or null when it has none
         */
        @Nullable
        String backup(TableModel table, int node)
        {
            String keyspace = this.keyspace;
            String name = table.name;
            String root = backups.resolve(format("backup-%d-node%d", backupCount.incrementAndGet(), node)).toString();
            return cluster.get(node).callOnInstance(() -> OnNode.backup(keyspace, name, root));
        }

        void importSSTables(TableModel table, int node, String directory, boolean copy)
        {
            String keyspace = this.keyspace;
            String name = table.name;
            List<String> failed = cluster.get(node).callOnInstance(() -> OnNode.importSSTables(keyspace, name, directory, copy));
            assertThat(failed).as("directories which failed to import into %s on node %d", table, node).isEmpty();
        }

        void restart(int node)
        {
            List<String> unexpected = unexpectedLogs();
            try
            {
                cluster.get(node).shutdown().get();
            }
            catch (InterruptedException | ExecutionException e)
            {
                throw new AssertionError(e);
            }
            cluster.get(node).startup();
            for (int n = 1; n <= NODES; n++)
                ClusterUtils.awaitRingHealthy(cluster.get(n));
            awaitQuorumReads(node);

            // until the node reads with consistency level QUORUM, its refresh can not read dictionaries
            List<String> whileRestarting = unexpectedLogs();
            whileRestarting.removeIf(line -> line.contains("Could not read the latest compression dictionary"));
            unexpected.addAll(whileRestarting);
            if (!unexpected.isEmpty())
                throw new AssertionError(String.join("\n", unexpected));
        }

        /**
         * A restarted node can read with consistency level QUORUM a few seconds after the ring looks healthy to every
         * node. Waits until it reads dictionaries so, as its refresh does, and until a refresh which started before
         * finished, so the refresh logged whether it could read them before this returns.
         */
        void awaitQuorumReads(int node)
        {
            long deadline = nanoTime() + TimeUnit.MINUTES.toNanos(1);
            while (!cluster.get(node).callOnInstance(OnNode::readsWithQuorum))
            {
                if (nanoTime() > deadline)
                    throw new AssertionError(format("node %d can not read dictionaries with consistency level QUORUM after it restarted", node));
                Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
            }
            cluster.get(node).runOnInstance(OnNode::awaitScheduledTasks);
        }

        void verifyAll()
        {
            for (TableModel table : tables.values())
            {
                for (int node = 1; node <= NODES; node++)
                    verify(table, node, true, "", Long.MIN_VALUE, Long.MAX_VALUE);
            }
        }

        /**
         * Reads the rows matching the clauses on a node, or via a node with consistency level ALL, which must be the
         * rows of the model from a key up to another key.
         */
        void verify(TableModel table, int node, boolean local, String clauses, long fromKey, long toKey, Object... values)
        {
            String query = format("SELECT pk, ck, v FROM %s.%s %s", keyspace, table.name, clauses);
            Object[][] result = local ? cluster.get(node).executeInternal(query, values)
                                      : cluster.coordinator(node).execute(query, ConsistencyLevel.ALL, values);
            boolean reversed = clauses.contains("DESC");
            NavigableMap<Long, String> actual = new TreeMap<>();
            Long previous = null;
            for (Object[] row : result)
            {
                long key = key((Integer) row[0], (Integer) row[1]);
                if (previous != null && (int) (key >>> 32) == (int) (previous >>> 32) && (reversed ? key >= previous : key <= previous))
                    throw new AssertionError(format("reading %s of %s %s returned row %d:%d after row %d:%d", clauses, table,
                                                    local ? "on node " + node : "via node " + node, key >>> 32, (int) key, previous >>> 32, (int) (long) previous));
                previous = key;
                actual.put(key, (String) row[2]);
            }
            String difference = difference(table.rows.subMap(fromKey, true, toKey, false), actual);
            if (difference != null)
                throw new AssertionError(format("reading %s of %s %s returned %s", clauses.isEmpty() ? "all rows" : clauses, table,
                                                local ? "on node " + node : "via node " + node + " with consistency level ALL", difference));
        }

        void runConcurrently(List<Operation> operations) throws Exception
        {
            List<Future<?>> futures = new ArrayList<>(operations.size());
            for (Operation operation : operations)
                futures.add(executor.submit(() -> {
                    operation.action.run(this);
                    return null;
                }));

            Throwable failure = null;
            for (Future<?> future : futures)
            {
                try
                {
                    future.get(4, TimeUnit.MINUTES);
                }
                catch (ExecutionException e)
                {
                    failure = Throwables.merge(failure, e.getCause());
                }
            }
            if (failure instanceof Error)
                throw (Error) failure;
            if (failure != null)
                throw (Exception) failure;
        }

        void checkInvariants()
        {
            String keyspace = this.keyspace;
            List<String> problems = new ArrayList<>();
            for (int node = 1; node <= NODES; node++)
            {
                String problem = cluster.get(node).callOnInstance(() -> OnNode.checkInvariants(keyspace));
                if (problem != null)
                    problems.add(format("node %d: %s", node, problem));
            }
            problems.addAll(unexpectedLogs());
            if (!problems.isEmpty())
                throw new AssertionError(String.join("\n", problems));
        }

        List<String> unexpectedLogs()
        {
            List<String> unexpected = new ArrayList<>();
            for (int node = 1; node <= NODES; node++)
            {
                LogResult<List<String>> result = cluster.get(node).logs().grep(logMarks[node], CompressionDictionaryClusterFuzzTest::isUnexpected);
                logMarks[node] = result.getMark();
                for (String line : result.getResult())
                    unexpected.add(format("node %d logged: %s", node, line));
            }
            return unexpected;
        }

        void destroy(Throwable failure)
        {
            try
            {
                if (failure != null)
                {
                    // the nodes run in this JVM, so this shows e.g. what a node is blocked on when it stopped responding
                    logger.warn("Threads when the test failed:\n{}", threadDump());
                    return;
                }

                verifyAll();
                checkInvariants();

                // dropping all tables releases every dictionary, also the ones only compressors still reference
                cluster.schemaChange("DROP KEYSPACE " + keyspace);
                for (int node = 1; node <= NODES; node++)
                {
                    long deadline = nanoTime() + TimeUnit.SECONDS.toNanos(30);
                    List<String> unreleased;
                    while (!(unreleased = cluster.get(node).callOnInstance(OnNode::unreleased)).isEmpty())
                    {
                        if (nanoTime() > deadline)
                            throw new AssertionError(format("node %d did not release %d dictionaries after all tables were dropped: %s", node, unreleased.size(), unreleased));
                        Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
                    }
                }
                List<String> unexpected = unexpectedLogs();
                if (!unexpected.isEmpty())
                    throw new AssertionError(String.join("\n", unexpected));
            }
            finally
            {
                executor.shutdownNow();
                new File(backups).deleteRecursive();
            }
        }
    }

    private static String threadDump()
    {
        StringBuilder dump = new StringBuilder();
        for (ThreadInfo thread : ManagementFactory.getThreadMXBean().dumpAllThreads(true, true))
        {
            dump.append('"').append(thread.getThreadName()).append("\" ").append(thread.getThreadState());
            if (thread.getLockInfo() != null)
                dump.append(" on ").append(thread.getLockInfo()).append(" owned by \"").append(thread.getLockOwnerName()).append('"');
            dump.append('\n');
            StackTraceElement[] stack = thread.getStackTrace();
            for (int i = 0; i < stack.length; i++)
            {
                dump.append("\tat ").append(stack[i]).append('\n');
                for (MonitorInfo monitor : thread.getLockedMonitors())
                {
                    if (monitor.getLockedStackDepth() == i)
                        dump.append("\t- locked ").append(monitor).append('\n');
                }
            }
            for (LockInfo synchronizer : thread.getLockedSynchronizers())
                dump.append("\t- holds ").append(synchronizer).append('\n');
            dump.append('\n');
        }
        return dump.toString();
    }

    /**
     * Runs on a node. Each node loads this class itself, so the state below is the state of one node.
     */
    public static class OnNode
    {
        // every dictionary object seen in a cache; each must be released once all tables are dropped
        private static final Set<CompressionDictionary> seen = Collections.synchronizedSet(Collections.newSetFromMap(new IdentityHashMap<>()));
        // dictionary caches of dropped tables; nothing may cache a dictionary in them anymore
        private static final Map<Cache<DictId, CompressionDictionary>, String> droppedCaches = Collections.synchronizedMap(new IdentityHashMap<>());

        private static ColumnFamilyStore cfs(String keyspace, String table)
        {
            return Keyspace.open(keyspace).getColumnFamilyStore(table);
        }

        public static void configureNextTable(int cacheSize, String cacheExpire)
        {
            Config config = DatabaseDescriptor.getRawConfig();
            config.compression_dictionary_cache_size = cacheSize;
            config.compression_dictionary_cache_expire = new DurationSpec.IntSecondsBound(cacheExpire);
        }

        public static void setAutoCompaction(String keyspace, String table, boolean enable)
        {
            if (enable)
                cfs(keyspace, table).enableAutoCompaction();
            else
                cfs(keyspace, table).disableAutoCompaction();
        }

        public static void setFlushCompression(String flushCompression)
        {
            DatabaseDescriptor.setFlushCompression(Config.FlushCompression.valueOf(flushCompression));
        }

        public static void flush(String keyspace, String table)
        {
            cfs(keyspace, table).forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);
        }

        public static void compact(String keyspace, String table)
        {
            cfs(keyspace, table).forceMajorCompaction();
        }

        /**
         * @return the uncompressed size of the sstables of the table, after flushing and compacting it
         */
        public static long compactAndMeasure(String keyspace, String table)
        {
            ColumnFamilyStore cfs = cfs(keyspace, table);
            cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);
            cfs.forceMajorCompaction();
            return cfs.getLiveSSTables().stream().mapToLong(SSTableReader::uncompressedLength).sum();
        }

        /**
         * @return whether this node reads dictionaries with consistency level QUORUM, as the refresh does
         */
        public static boolean readsWithQuorum()
        {
            try
            {
                QueryProcessor.execute(format("SELECT dict_id FROM %s.%s WHERE keyspace_name = 'none' AND table_name = 'none'",
                                              SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, SystemDistributedKeyspace.COMPRESSION_DICTIONARIES),
                                       org.apache.cassandra.db.ConsistencyLevel.QUORUM);
                return true;
            }
            catch (RequestExecutionException e)
            {
                return false;
            }
        }

        /**
         * The refresh of each table runs on the single thread of this executor, so a task submitted to it runs after
         * the refresh which runs now.
         */
        public static void awaitScheduledTasks()
        {
            ScheduledExecutors.scheduledTasks.submit(() -> {}).syncUninterruptibly();
        }

        /**
         * @return false when a training of the table which was not waited for still runs
         */
        public static boolean train(String keyspace, String table, boolean force, HashMap<String, String> parameters)
        {
            try
            {
                cfs(keyspace, table).compressionDictionaryManager().train(force, parameters);
                return true;
            }
            catch (IllegalStateException e)
            {
                if (messageStartsWith(e, "Training already in progress"))
                    return false;
                throw e;
            }
        }

        /**
         * @return the status of the last training, followed by its failure if it failed
         */
        public static String trainingState(String keyspace, String table)
        {
            TrainingState training = TrainingState.fromCompositeData(cfs(keyspace, table).compressionDictionaryManager().getTrainingState());
            return training.isFailed() ? training.status + ": " + training.failureMessage : training.status.toString();
        }

        public static long latestStoredDictionaryId(String keyspace, String table)
        {
            CompressionDictionary.LightweightCompressionDictionary dictionary =
            SystemDistributedKeyspace.retrieveLightweightLatestCompressionDictionary(keyspace, table, cfs(keyspace, table).metadata().id.toLongString());
            return dictionary == null ? -1 : dictionary.dictId.id;
        }

        public static boolean knowsDictionary(String keyspace, String table, long dictId)
        {
            CompressionDictionaryManager manager = cfs(keyspace, table).compressionDictionaryManager();
            CompressionDictionary current = manager.getCurrent();
            return manager.get(new DictId(Kind.ZSTD, dictId)) != null || (current != null && current.dictId().id >= dictId);
        }

        /**
         * @return the id of the imported dictionary, -1 when the source has no current dictionary, -2 when the target
         *         has a newer one or this one
         */
        public static long importDictionary(String keyspace, String source, String target)
        {
            // referenced, so an eviction of the source can not release it meanwhile
            Ref<? extends CompressionDictionary> ref = cfs(keyspace, source).compressionDictionaryManager().refCurrent();
            if (ref == null)
                return -1;

            ColumnFamilyStore cfs = cfs(keyspace, target);
            try
            {
                CompositeData data = CompressionDictionaryDetailsTabularData.fromCompressionDictionary(keyspace, target, cfs.metadata().id.toLongString(), ref.get());
                cfs.compressionDictionaryManager().importCompressionDictionary(data);
                return ref.get().dictId().id;
            }
            catch (IllegalArgumentException e)
            {
                if (isNotNewer(e))
                    return -2;
                throw e;
            }
            finally
            {
                ref.release();
            }
        }

        public static void truncateLocally(String keyspace, String table)
        {
            cfs(keyspace, table).truncateBlockingWithoutSnapshot();
        }

        /**
         * Flushes the table and copies its sstables into a directory under the root, named like the directory of the
         * table.
         *
         * @return the directory, or null when the table has no sstables
         */
        @Nullable
        public static String backup(String keyspace, String table, String root)
        {
            ColumnFamilyStore cfs = cfs(keyspace, table);
            cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);
            // referenced, so a compaction does not delete them while they are copied
            try (ColumnFamilyStore.RefViewFragment view = cfs.selectAndReference(View.selectFunction(SSTableSet.CANONICAL)))
            {
                if (view.sstables.isEmpty())
                    return null;

                Path backup = Path.of(root).resolve(keyspace).resolve(view.sstables.get(0).descriptor.directory.name());
                Files.createDirectories(backup);
                for (SSTableReader sstable : view.sstables)
                    copy(sstable, backup);
                return backup.toString();
            }
            catch (IOException e)
            {
                throw new UncheckedIOException(e);
            }
        }

        public static List<String> importSSTables(String keyspace, String table, String directory, boolean copy)
        {
            return new ArrayList<>(cfs(keyspace, table).importNewSSTables(new HashSet<>(List.of(directory)), false, false, true, true, true, false, copy));
        }

        public static void evictDictionaries(String keyspace, String table, int how, int index)
        {
            evict(dictionaryCache(cfs(keyspace, table)), how, index);
        }

        public static void evictCompressors(int how, int index)
        {
            evict(compressorCache(), how, index);
        }

        public static void cleanUpCaches(String keyspace)
        {
            for (ColumnFamilyStore cfs : Keyspace.open(keyspace).getColumnFamilyStores())
                dictionaryCache(cfs).cleanUp();
            compressorCache().cleanUp();
        }

        public static void rememberDroppedTable(String keyspace, String table, String description)
        {
            droppedCaches.put(dictionaryCache(cfs(keyspace, table)), description);
        }

        /**
         * @return null, or what is wrong
         */
        @Nullable
        public static String checkInvariants(String keyspace)
        {
            List<CompressionDictionary> cached = new ArrayList<>();
            List<String> problems = new ArrayList<>();
            for (ColumnFamilyStore cfs : Keyspace.open(keyspace).getColumnFamilyStores())
                problems.add(checkCached(dictionaryCache(cfs), "the dictionary cache of " + cfs.name, cached));
            synchronized (droppedCaches)
            {
                droppedCaches.forEach((cache, table) -> {
                    if (!cache.asMap().isEmpty())
                        problems.add(format("the dictionary cache of %s holds %s", table, cache.asMap().keySet()));
                });
            }
            problems.add(checkCompressors(cached));
            seen.addAll(cached);
            problems.removeIf(problem -> problem == null);
            return problems.isEmpty() ? null : String.join("; ", problems);
        }

        /**
         * @return the dictionaries seen in a cache which are not released yet
         */
        public static List<String> unreleased()
        {
            compressorCache().cleanUp();
            synchronized (seen)
            {
                return seen.stream()
                           .filter(dictionary -> dictionary.selfRef() != null && !isReleased(dictionary))
                           .map(dictionary -> dictionary.dictId() + " referenced " + dictionary.selfRef().globalCount() + " times")
                           .collect(Collectors.toList());
            }
        }
    }
}
