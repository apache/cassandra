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

package org.apache.cassandra.db.compression;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import javax.management.openmbean.CompositeData;

import com.github.benmanes.caffeine.cache.Cache;
import com.google.common.util.concurrent.Uninterruptibles;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import accord.utils.DefaultRandom;
import accord.utils.Property.Command;
import accord.utils.Property.StateOnlyCommand;
import accord.utils.RandomSource;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compression.CompressionDictionary.DictId;
import org.apache.cassandra.db.compression.CompressionDictionaryFuzzing.Compression;
import org.apache.cassandra.db.compression.CompressionDictionaryFuzzing.TableSpec;
import org.apache.cassandra.db.compression.CompressionDictionaryFuzzing.WriteRow;
import org.apache.cassandra.db.compression.ICompressionDictionaryTrainer.TrainingStatus;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.SystemDistributedKeyspace;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.concurrent.Ref;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.core.AppenderBase;

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
import static org.apache.cassandra.io.compress.IDictionaryCompressor.TRAINING_MAX_DICTIONARY_SIZE_PARAMETER_NAME;
import static org.apache.cassandra.io.compress.IDictionaryCompressor.TRAINING_MAX_TOTAL_SAMPLE_SIZE_PARAMETER_NAME;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Runs random steps on 15 to 20 tables, most of them compressed with Zstd dictionaries: writes, deletes, reads, flushes,
 * compactions, changes of compression, trainings of dictionaries, imports of a dictionary of one table into another,
 * creating and dropping tables, restoring a table from a copy of its sstables after it was dropped, reloading
 * sstables, evicting dictionaries and compressors from their caches, waiting for them to expire, and several of these
 * at once on different tables.
 * <p>
 * The caches are small and expire after seconds, and each table with a dictionary reloads its latest dictionary from
 * the system table every second, so dictionaries are evicted, expire and come back as new objects all the time.
 * <p>
 * Before each step, the test checks that
 * <ul>
 *     <li>no dictionary cache of a table and no cached compressor holds a released dictionary;</li>
 *     <li>the dictionary cache of a dropped table stays empty;</li>
 *     <li>nothing logged an error, e.g. a "BAD RELEASE" of a reference or a failed compaction, or a warning about
 *     dictionaries.</li>
 * </ul>
 * Reads must return the rows the test wrote, and each training must complete. At the end, all tables are read once
 * more and dropped, and every dictionary seen in a cache during the run must be released.
 * <p>
 * {@code CompressionDictionaryClusterFuzzTest} runs such steps on a cluster.
 */
public class CompressionDictionaryFuzzTest extends CQLTester
{
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(CompressionDictionaryFuzzTest.class);

    private static final int MIN_TABLES = 15;
    private static final int MAX_TABLES = 20;
    private static final int EXAMPLES = 2;
    private static final int STEPS = 500;

    // The compressor cache of ZstdDictionaryCompressor is static, so it takes its size and expiration from the
    // configuration once, when the class is loaded. Dictionary caches of tables take it when a table is created.
    private static final int COMPRESSOR_CACHE_SIZE = 3;
    private static final String COMPRESSOR_CACHE_EXPIRE = "2s";
    private static final Integer[] DICTIONARY_CACHE_SIZES = { 1, 2, 3, 10 };
    private static final String[] DICTIONARY_CACHE_EXPIRES = { "1s", "2s", "5s", "1h" };

    private static final AtomicInteger runs = new AtomicInteger();
    private static final Queue<String> unexpectedLogs = new ConcurrentLinkedQueue<>();

    @BeforeClass
    public static void setUpClass()
    {
        prePrepareServer();
        Config config = DatabaseDescriptor.getRawConfig();
        config.compression_dictionary_cache_size = COMPRESSOR_CACHE_SIZE;
        config.compression_dictionary_cache_expire = new DurationSpec.IntSecondsBound(COMPRESSOR_CACHE_EXPIRE);
        config.compression_dictionary_refresh_initial_delay = new DurationSpec.IntSecondsBound("1s");
        config.compression_dictionary_refresh_interval = new DurationSpec.IntSecondsBound("1s");
        // tables get megabytes of data for training, which the test reads all at once
        config.coordinator_read_size_warn_threshold = null;
        config.coordinator_read_size_fail_threshold = null;
        config.local_read_size_warn_threshold = null;
        config.local_read_size_fail_threshold = null;
        config.row_index_read_size_warn_threshold = null;
        config.row_index_read_size_fail_threshold = null;
        prepareServer();

        assertThat(compressorCache().policy().eviction().orElseThrow().getMaximum())
        .as("size of the compressor cache, which must be created after this test configured it")
        .isEqualTo(COMPRESSOR_CACHE_SIZE);

        AppenderBase<ILoggingEvent> appender = new AppenderBase<>()
        {
            @Override
            protected void append(ILoggingEvent event)
            {
                if (isUnexpected(event))
                    unexpectedLogs.add(event.getLevel() + " " + event.getLoggerName() + ": " + describe(event));
            }
        };
        appender.start();
        ((Logger) LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME)).addAppender(appender);
    }

    @Test
    public void fuzz()
    {
        stateful()
        .withExamples(EXAMPLES)
        .withSteps(STEPS)
        .withStepTimeout(Duration.ofMinutes(3))
        .check(commands(() -> State::new)
               .preCommands(State::checkInvariants)
               .add(20, (rs, state) -> command(writeOperation(rs, state.pick(rs))))
               .add(5, (rs, state) -> command(deleteOperation(rs, state.pick(rs))))
               .add(15, (rs, state) -> command(readOperation(rs, state.pick(rs))))
               .add(8, CompressionDictionaryFuzzTest::flushTables)
               .add(5, (rs, state) -> command(compactOperation(state.pick(rs))))
               .add(2, CompressionDictionaryFuzzTest::toggleAutoCompaction)
               .add(2, (rs, state) -> command("Read all tables", State::verifyAll))
               .add(6, CompressionDictionaryFuzzTest::alterCompression)
               .addIf(State::hasDictionaryTable, 8, (rs, state) -> command(trainOperation(rs, state.pickDictionaryTable(rs))))
               .addIf(State::hasDictionaryTable, 3, CompressionDictionaryFuzzTest::importDictionary)
               .addIf(state -> state.tables.size() < MAX_TABLES, 3, CompressionDictionaryFuzzTest::createNewTable)
               .addIf(state -> state.tables.size() > MIN_TABLES, 3, CompressionDictionaryFuzzTest::dropOneTable)
               .add(3, CompressionDictionaryFuzzTest::dropAndRestore)
               .add(3, (rs, state) -> command(reloadOperation(rs, state.pick(rs))))
               .add(1, CompressionDictionaryFuzzTest::truncateTable)
               .add(3, (rs, state) -> command(evictDictionariesOperation(rs, state.pick(rs))))
               .add(3, (rs, state) -> command(evictCompressorsOperation(rs)))
               .add(1, CompressionDictionaryFuzzTest::waitForExpiry)
               .add(2, CompressionDictionaryFuzzTest::changeFlushCompression)
               .add(5, CompressionDictionaryFuzzTest::concurrently)
               .destroyState(State::destroy)
               .build());
    }

    private static Command<State, Void, ?> flushTables(RandomSource rs, State state)
    {
        List<TableModel> tables = state.pickSome(rs, rs.nextInt(1, 4));
        return command("Flush " + names(tables), s -> {
            for (TableModel table : tables)
                s.flush(table);
        });
    }

    private static Command<State, Void, ?> toggleAutoCompaction(RandomSource rs, State state)
    {
        TableModel table = state.pick(rs);
        boolean enable = rs.nextBoolean();
        return command(format("%s automatic compaction of %s", enable ? "Enable" : "Disable", table.name), s -> {
            if (enable)
                s.cfs(table).enableAutoCompaction();
            else
                s.cfs(table).disableAutoCompaction();
        });
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
        return command(format("Import the current dictionary of %s into %s", source.name, target.name),
                       s -> s.importDictionary(source, target));
    }

    private static Command<State, Void, ?> createNewTable(RandomSource rs, State state)
    {
        String name = !state.droppedNames.isEmpty() && rs.nextBoolean() ? rs.pick(state.droppedNames) : state.newName();
        TableSpec spec = TableSpec.random(rs, name, DICTIONARY_CACHE_SIZES, DICTIONARY_CACHE_EXPIRES);
        List<WriteRow> rows = randomRows(rs, rs.nextInt(0, 2000));
        return command(format("Create table %s with %d rows", spec, rows.size()), s -> {
            TableModel table = s.create(spec);
            s.write(table, rows);
            s.flush(table);
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
        return command(format("Copy the sstables of %s, drop it, create it as %s and import the copies%s",
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
        int millis = rs.nextInt(1000, 3000);
        return command(format("Wait %d ms for cached dictionaries and compressors to expire", millis), s -> {
            Uninterruptibles.sleepUninterruptibly(millis, TimeUnit.MILLISECONDS);
            for (TableModel table : s.tables.values())
                dictionaryCache(s.cfs(table)).cleanUp();
            compressorCache().cleanUp();
        });
    }

    private static Command<State, Void, ?> changeFlushCompression(RandomSource rs, State state)
    {
        Config.FlushCompression flushCompression = rs.pick(Config.FlushCompression.values());
        return command("Set flush_compression to " + flushCompression, s -> DatabaseDescriptor.setFlushCompression(flushCompression));
    }

    private static Command<State, Void, ?> concurrently(RandomSource rs, State state)
    {
        List<Operation> operations = new ArrayList<>();
        for (TableModel table : state.pickSome(rs, rs.nextInt(2, 6)))
        {
            switch (rs.nextInt(8))
            {
                case 0: operations.add(writeOperation(rs, table)); break;
                case 1: operations.add(deleteOperation(rs, table)); break;
                case 2: operations.add(readOperation(rs, table)); break;
                case 3: operations.add(compactOperation(table)); break;
                case 4: operations.add(reloadOperation(rs, table)); break;
                case 5: operations.add(evictDictionariesOperation(rs, table)); break;
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
        boolean flush = rs.decide(0.3f);
        return new Operation(format("Write %d rows into %s%s", rows.size(), table.name, flush ? " and flush" : ""), s -> {
            s.write(table, rows);
            if (flush)
                s.flush(table);
        });
    }

    private static Operation deleteOperation(RandomSource rs, TableModel table)
    {
        int pk = partitionKey(rs, table.rows);
        switch (rs.nextInt(3))
        {
            case 0:
            {
                int ck = rs.nextInt(ROWS_PER_PARTITION);
                return new Operation(format("Delete row %d:%d of %s", pk, ck, table.name), s -> s.deleteRow(table, pk, ck));
            }
            case 1:
                return new Operation(format("Delete partition %d of %s", pk, table.name), s -> s.deletePartition(table, pk));
            default:
            {
                int from = rs.nextInt(ROWS_PER_PARTITION);
                int to = rs.nextInt(from + 1, ROWS_PER_PARTITION + 1);
                return new Operation(format("Delete rows %d:[%d, %d) of %s", pk, from, to, table.name), s -> s.deleteRows(table, pk, from, to));
            }
        }
    }

    private static Operation readOperation(RandomSource rs, TableModel table)
    {
        int pk = partitionKey(rs, table.rows);
        switch (rs.nextInt(4))
        {
            case 0:
                return new Operation(format("Read partition %d of %s", pk, table.name), s -> s.verifyPartition(table, pk, false));
            case 1:
                return new Operation(format("Read partition %d of %s in reverse", pk, table.name), s -> s.verifyPartition(table, pk, true));
            case 2:
            {
                int from = rs.nextInt(ROWS_PER_PARTITION);
                int to = rs.nextInt(from + 1, ROWS_PER_PARTITION + 1);
                return new Operation(format("Read rows %d:[%d, %d) of %s", pk, from, to, table.name), s -> s.verifyRows(table, pk, from, to));
            }
            default:
                return new Operation("Read all rows of " + table.name, s -> s.verify(table));
        }
    }

    private static Operation compactOperation(TableModel table)
    {
        return new Operation("Compact " + table.name, s -> s.cfs(table).forceMajorCompaction());
    }

    private static Operation trainOperation(RandomSource rs, TableModel table)
    {
        boolean force = rs.nextBoolean();
        boolean await = rs.decide(0.8f);
        Compression compression = table.compression;
        int maxTotalSampleSizeInKiB = compression.maxTotalSampleSizeInKiB;
        Map<String, String> parameters = Map.of();
        if (rs.nextBoolean())
        {
            maxTotalSampleSizeInKiB = Compression.maxTotalSampleSizeInKiB(rs, compression.chunkLengthInKiB);
            parameters = Map.of(TRAINING_MAX_DICTIONARY_SIZE_PARAMETER_NAME, Compression.maxDictionarySizeInKiB(rs, maxTotalSampleSizeInKiB) + "KiB",
                                TRAINING_MAX_TOTAL_SAMPLE_SIZE_PARAMETER_NAME, maxTotalSampleSizeInKiB + "KiB");
        }
        long minDataSize = (long) (DATA_PER_SAMPLE_SIZE * maxTotalSampleSizeInKiB * 1024);
        long seed = rs.nextLong();
        Map<String, String> trainingParameters = parameters;
        return new Operation(format("Train a dictionary for %s%s%s%s", table.name, force ? " forcibly" : "",
                                    parameters.isEmpty() ? "" : " with " + parameters, await ? "" : " without waiting for it"),
                             s -> {
                                 s.topUp(table, minDataSize, new DefaultRandom(seed));
                                 s.train(table, force, trainingParameters, await);
                             });
    }

    private static Operation reloadOperation(RandomSource rs, TableModel table)
    {
        boolean copy = rs.nextBoolean();
        return new Operation(format("Copy the sstables of %s, truncate it and import the copies%s", table.name, copy ? "" : " by moving them"),
                             s -> s.reload(table, copy));
    }

    private static Operation evictDictionariesOperation(RandomSource rs, TableModel table)
    {
        int how = rs.nextInt(3);
        int index = rs.nextInt(Integer.MAX_VALUE);
        return new Operation(describeEviction(how, "the dictionary cache of " + table.name),
                             s -> evict(dictionaryCache(s.cfs(table)), how, index));
    }

    private static Operation evictCompressorsOperation(RandomSource rs)
    {
        int how = rs.nextInt(3);
        int index = rs.nextInt(Integer.MAX_VALUE);
        return new Operation(describeEviction(how, "the compressor cache"), s -> evict(compressorCache(), how, index));
    }

    private static String names(List<TableModel> tables)
    {
        return tables.stream().map(table -> table.name).collect(Collectors.joining(", "));
    }

    private static boolean isUnexpected(ILoggingEvent event)
    {
        boolean dictionaryWarning = event.getLevel() == Level.WARN
                                    && (event.getLoggerName().startsWith("org.apache.cassandra.db.compression")
                                        || event.getLoggerName().startsWith("org.apache.cassandra.io.compress"));
        if (!event.getLevel().isGreaterOrEqual(Level.ERROR) && !dictionaryWarning)
            return false;
        // Ref's strong reference leak detector, running in tests 2 minutes after the start, reports every live sstable
        // of every table, system tables too, as it reaches them from global singletons like the chunk cache or loggers
        return !event.getFormattedMessage().startsWith("Strong self-ref loop detected");
    }

    private static String describe(ILoggingEvent event)
    {
        StringBuilder description = new StringBuilder(event.getFormattedMessage());
        for (IThrowableProxy throwable = event.getThrowableProxy(); throwable != null; throwable = throwable.getCause())
            description.append(" <- ").append(throwable.getClassName()).append(": ").append(throwable.getMessage());
        return description.toString();
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

    private static class State
    {
        final String keyspace = "dictionary_fuzz_" + runs.incrementAndGet();
        final Map<String, TableModel> tables = new LinkedHashMap<>();
        final List<String> droppedNames = new ArrayList<>();
        // dictionary caches of dropped tables; nothing may cache a dictionary in them anymore
        final Map<Cache<DictId, CompressionDictionary>, String> droppedCaches = new IdentityHashMap<>();
        // every dictionary object seen in a cache; each must be released once all tables are dropped
        final Set<CompressionDictionary> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        final ExecutorService executor = Executors.newFixedThreadPool(8);
        final AtomicInteger backupCount = new AtomicInteger();
        final Path backups;
        int tableCount;

        State(RandomSource rs)
        {
            try
            {
                backups = Files.createTempDirectory("dictionary_fuzz");
            }
            catch (IOException e)
            {
                throw new UncheckedIOException(e);
            }

            schemaChange(format("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}", keyspace));
            for (int i = rs.nextInt(MIN_TABLES, MAX_TABLES + 1); i > 0; i--)
            {
                TableModel table = create(TableSpec.random(rs, newName(), DICTIONARY_CACHE_SIZES, DICTIONARY_CACHE_EXPIRES));
                write(table, randomRows(rs, rs.nextInt(500, 3000)));
                flush(table);
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

        ColumnFamilyStore cfs(TableModel table)
        {
            return Keyspace.open(keyspace).getColumnFamilyStore(table.name);
        }

        String tableId(TableModel table)
        {
            return cfs(table).metadata().id.toLongString();
        }

        TableModel create(TableSpec spec)
        {
            // the dictionary cache of a table takes its configuration when the table is created
            Config config = DatabaseDescriptor.getRawConfig();
            config.compression_dictionary_cache_size = spec.cacheSize;
            config.compression_dictionary_cache_expire = new DurationSpec.IntSecondsBound(spec.cacheExpire);
            schemaChange(spec.createStatement(keyspace));
            TableModel table = new TableModel(spec);
            if (!spec.autoCompaction)
                cfs(table).disableAutoCompaction();
            tables.put(table.name, table);
            droppedNames.removeIf(table.name::equals);
            return table;
        }

        void drop(TableModel table)
        {
            Cache<DictId, CompressionDictionary> cache = dictionaryCache(cfs(table));
            schemaChange(format("DROP TABLE %s.%s", keyspace, table.name));
            tables.remove(table.name);
            droppedNames.add(table.name);
            droppedCaches.put(cache, table + ", dropped");
        }

        void alter(TableModel table, Compression compression)
        {
            schemaChange(format("ALTER TABLE %s.%s WITH compression = %s", keyspace, table.name, compression));
            table.compression = compression;
        }

        void write(TableModel table, List<WriteRow> rows)
        {
            String query = format("INSERT INTO %s.%s (pk, ck, v) VALUES (?, ?, ?)", keyspace, table.name);
            for (WriteRow row : rows)
            {
                QueryProcessor.executeInternal(query, row.pk, row.ck, row.value);
                table.rows.put(key(row.pk, row.ck), row.value);
            }
        }

        void deleteRow(TableModel table, int pk, int ck)
        {
            QueryProcessor.executeInternal(format("DELETE FROM %s.%s WHERE pk = ? AND ck = ?", keyspace, table.name), pk, ck);
            table.rows.remove(key(pk, ck));
        }

        void deletePartition(TableModel table, int pk)
        {
            QueryProcessor.executeInternal(format("DELETE FROM %s.%s WHERE pk = ?", keyspace, table.name), pk);
            table.rows.subMap(key(pk, 0), key(pk + 1, 0)).clear();
        }

        void deleteRows(TableModel table, int pk, int from, int to)
        {
            QueryProcessor.executeInternal(format("DELETE FROM %s.%s WHERE pk = ? AND ck >= ? AND ck < ?", keyspace, table.name), pk, from, to);
            table.rows.subMap(key(pk, from), key(pk, to)).clear();
        }

        void flush(TableModel table)
        {
            cfs(table).forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);
        }

        void truncate(TableModel table)
        {
            cfs(table).truncateBlocking();
            table.rows.clear();
        }

        void train(TableModel table, boolean force, Map<String, String> parameters, boolean await)
        {
            CompressionDictionaryManager manager = cfs(table).compressionDictionaryManager();
            try
            {
                manager.train(force, parameters);
            }
            catch (IllegalStateException e)
            {
                // a training which was not waited for still runs
                if (messageStartsWith(e, "Training already in progress"))
                    return;
                throw e;
            }

            if (!await)
                return;

            TrainingState training = awaitTraining(table, manager);
            assertThat(training.status).as("training a dictionary for %s, which failed with: %s", table, training.failureMessage)
                                       .isEqualTo(TrainingStatus.COMPLETED);
            assertThat(SystemDistributedKeyspace.retrieveLightweightLatestCompressionDictionary(keyspace, table.name, tableId(table)))
            .as("dictionary stored for %s after the training completed", table)
            .isNotNull();
        }

        /**
         * Writes rows into the table until its sstables have at least the given amount of uncompressed data, so that a
         * training samples enough chunks from them. The table is compacted, so that all sstables have the chunks of
         * its current compression: flushed sstables or sstables written before its compression changed can have much
         * bigger chunks, of which a training samples too few.
         */
        void topUp(TableModel table, long minDataSize, RandomSource rs)
        {
            ColumnFamilyStore cfs = cfs(table);
            flush(table);
            for (int round = 0; ; round++)
            {
                cfs.forceMajorCompaction();
                long dataSize = cfs.getLiveSSTables().stream().mapToLong(SSTableReader::uncompressedLength).sum();
                if (dataSize >= minDataSize)
                    return;
                if (round == 50)
                    throw new AssertionError(format("%s has only %d bytes of data after writing to it %d times, %d bytes are needed for training", table, dataSize, round, minDataSize));

                write(table, topUpRows(rs, minDataSize - dataSize));
                flush(table);
            }
        }

        TrainingState awaitTraining(TableModel table, CompressionDictionaryManager manager)
        {
            long deadline = nanoTime() + TimeUnit.MINUTES.toNanos(1);
            while (true)
            {
                TrainingState training = TrainingState.fromCompositeData(manager.getTrainingState());
                if (training.status == TrainingStatus.COMPLETED || training.status == TrainingStatus.FAILED)
                    return training;
                if (nanoTime() > deadline)
                    throw new AssertionError(format("training a dictionary for %s did not finish within a minute, it is %s", table, training.status));
                Uninterruptibles.sleepUninterruptibly(50, TimeUnit.MILLISECONDS);
            }
        }

        void importDictionary(TableModel source, TableModel target)
        {
            // referenced, so an eviction of the source can not release it meanwhile
            Ref<? extends CompressionDictionary> ref = cfs(source).compressionDictionaryManager().refCurrent();
            if (ref == null)
                return;

            try
            {
                CompositeData data = CompressionDictionaryDetailsTabularData.fromCompressionDictionary(keyspace, target.name, tableId(target), ref.get());
                cfs(target).compressionDictionaryManager().importCompressionDictionary(data);
            }
            catch (IllegalArgumentException e)
            {
                if (!isNotNewer(e))
                    throw e;
            }
            finally
            {
                ref.release();
            }
        }

        void reload(TableModel table, boolean copy) throws IOException
        {
            Path backup = backup(table);
            cfs(table).truncateBlockingWithoutSnapshot();
            if (backup != null)
                importSSTables(table, backup, copy);
            verify(table);
        }

        void dropAndRestore(TableModel table, TableSpec spec, boolean copy) throws IOException
        {
            Path backup = backup(table);
            drop(table);
            TableModel restored = create(spec);
            restored.rows.putAll(table.rows);
            if (backup != null)
                importSSTables(restored, backup, copy);
            verify(restored);
        }

        /**
         * Flushes the table and copies its sstables into a new directory, named like the directory of the table.
         *
         * @return the directory, or null when the table has no sstables
         */
        Path backup(TableModel table) throws IOException
        {
            ColumnFamilyStore cfs = cfs(table);
            cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);
            // referenced, so a compaction does not delete them while they are copied
            try (ColumnFamilyStore.RefViewFragment view = cfs.selectAndReference(View.selectFunction(SSTableSet.CANONICAL)))
            {
                if (view.sstables.isEmpty())
                    return null;

                Path backup = backups.resolve("backup-" + backupCount.incrementAndGet())
                                     .resolve(keyspace)
                                     .resolve(view.sstables.get(0).descriptor.directory.name());
                Files.createDirectories(backup);
                for (SSTableReader sstable : view.sstables)
                    copy(sstable, backup);
                return backup;
            }
        }

        void importSSTables(TableModel table, Path directory, boolean copy)
        {
            List<String> failed = cfs(table).importNewSSTables(Set.of(directory.toString()), false, false, true, true, true, false, copy);
            assertThat(failed).as("directories which failed to import into %s", table).isEmpty();
        }

        void verifyAll()
        {
            for (TableModel table : tables.values())
                verify(table);
        }

        void verify(TableModel table)
        {
            NavigableMap<Long, String> actual = new TreeMap<>();
            for (UntypedResultSet.Row row : select(format("SELECT pk, ck, v FROM %s.%s", keyspace, table.name)))
                actual.put(key(row.getInt("pk"), row.getInt("ck")), row.getString("v"));
            assertRows("all rows", table, table.rows, actual);
        }

        void verifyPartition(TableModel table, int pk, boolean reversed)
        {
            NavigableMap<Long, String> actual = new TreeMap<>();
            Integer previous = null;
            for (UntypedResultSet.Row row : select(format("SELECT ck, v FROM %s.%s WHERE pk = ?%s", keyspace, table.name, reversed ? " ORDER BY ck DESC" : ""), pk))
            {
                int ck = row.getInt("ck");
                if (previous != null && (reversed ? ck >= previous : ck <= previous))
                    throw new AssertionError(format("partition %d of %s returned row %d after row %d", pk, table, ck, previous));
                previous = ck;
                actual.put(key(pk, ck), row.getString("v"));
            }
            assertRows("partition " + pk, table, table.rows.subMap(key(pk, 0), true, key(pk + 1, 0), false), actual);
        }

        void verifyRows(TableModel table, int pk, int from, int to)
        {
            NavigableMap<Long, String> actual = new TreeMap<>();
            for (UntypedResultSet.Row row : select(format("SELECT ck, v FROM %s.%s WHERE pk = ? AND ck >= ? AND ck < ?", keyspace, table.name), pk, from, to))
                actual.put(key(pk, row.getInt("ck")), row.getString("v"));
            assertRows(format("rows %d:[%d, %d)", pk, from, to), table, table.rows.subMap(key(pk, from), true, key(pk, to), false), actual);
        }

        /**
         * {@link QueryProcessor#executeInternal} returns null for statements which return no rows, e.g. updates.
         */
        UntypedResultSet select(String query, Object... values)
        {
            UntypedResultSet rows = QueryProcessor.executeInternal(query, values);
            if (rows == null)
                throw new AssertionError("no result set for " + query);
            return rows;
        }

        void assertRows(String what, TableModel table, NavigableMap<Long, String> expected, NavigableMap<Long, String> actual)
        {
            String difference = difference(expected, actual);
            if (difference != null)
                throw new AssertionError(format("reading %s of %s returned %s", what, table, difference));
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
                    future.get(2, TimeUnit.MINUTES);
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
            List<CompressionDictionary> cached = new ArrayList<>();
            List<String> problems = new ArrayList<>();
            for (TableModel table : tables.values())
                problems.add(checkCached(dictionaryCache(cfs(table)), "the dictionary cache of " + table, cached));
            droppedCaches.forEach((cache, table) -> {
                if (!cache.asMap().isEmpty())
                    problems.add(format("the dictionary cache of %s holds %s", table, cache.asMap().keySet()));
            });
            problems.add(checkCompressors(cached));
            seen.addAll(cached);
            problems.removeIf(Objects::isNull);
            if (!problems.isEmpty())
                throw new AssertionError(String.join("\n", problems));

            assertNoUnexpectedLogs();
        }

        void assertNoUnexpectedLogs()
        {
            if (!unexpectedLogs.isEmpty())
                throw new AssertionError("unexpected errors or warnings were logged:\n" + String.join("\n", unexpectedLogs));
        }

        void destroy(Throwable failure)
        {
            try
            {
                if (failure != null)
                    return;

                verifyAll();
                checkInvariants();

                // dropping all tables releases every dictionary, also the ones only compressors still reference
                schemaChange("DROP KEYSPACE " + keyspace);
                long deadline = nanoTime() + TimeUnit.SECONDS.toNanos(30);
                List<CompressionDictionary> unreleased;
                while (!(unreleased = unreleased()).isEmpty())
                {
                    if (nanoTime() > deadline)
                        throw new AssertionError(format("%d dictionaries were not released after all tables were dropped: %s", unreleased.size(),
                                                        unreleased.stream().map(dictionary -> dictionary.dictId() + " referenced " + dictionary.selfRef().globalCount() + " times")
                                                                  .collect(Collectors.toList())));
                    compressorCache().cleanUp();
                    Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
                }
                assertNoUnexpectedLogs();
            }
            finally
            {
                if (failure != null)
                    schemaChange("DROP KEYSPACE IF EXISTS " + keyspace);
                executor.shutdownNow();
                new File(backups).deleteRecursive();
            }
        }

        List<CompressionDictionary> unreleased()
        {
            return seen.stream().filter(dictionary -> dictionary.selfRef() != null && !isReleased(dictionary)).collect(Collectors.toList());
        }
    }
}
