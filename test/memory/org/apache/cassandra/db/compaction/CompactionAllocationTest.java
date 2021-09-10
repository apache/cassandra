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
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.Uninterruptibles;

import org.apache.cassandra.Util;
import org.apache.cassandra.index.internal.CollatedViewIndexBuilder;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.monitoring.runtime.instrumentation.AllocationRecorder;
import com.google.monitoring.runtime.instrumentation.Sampler;
import com.sun.management.ThreadMXBean;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.ReadQuery;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.SecondaryIndexBuilder;
import org.apache.cassandra.io.sstable.ReducingKeyIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.UnbufferedDataOutputStreamPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.concurrent.Refs;

public class CompactionAllocationTest
{
    private static final Logger logger = LoggerFactory.getLogger(CompactionAllocationTest.class);
    private static final ThreadMXBean threadMX = (ThreadMXBean) ManagementFactory.getThreadMXBean();

    private static final boolean AGENT_MEASUREMENT = true;

    private static final boolean PROFILING_READS = false;
    private static final boolean PROFILING_COMPACTION = false;
    private static final boolean PROFILING = PROFILING_READS || PROFILING_COMPACTION;
    private static final List<String> summaries = new ArrayList<>();

    private static class CompactionSummary
    {
        final Measurement measurement;
        final int numPartitions;
        final int numRows;

        public CompactionSummary(Measurement measurement, int numPartitions, int numRows)
        {
            this.measurement = measurement;
            this.numPartitions = numPartitions;
            this.numRows = numRows;
        }

        List<String> cells()
        {
            long b = measurement.bytes();
            return Lists.newArrayList(Long.toString(b), Long.toString(b/numPartitions), Long.toString(b/numRows));
        }

        static final List<String> HEADERS = Lists.newArrayList("bytes", "/p", "/r");
        static final List<String> EMPTY = Lists.newArrayList("n/a", "n/a", "n/a");
    }

    private static class ReadSummary
    {
        final Measurement measurement;
        final int numReads;

        public ReadSummary(Measurement measurement, int numReads)
        {
            this.measurement = measurement;
            this.numReads = numReads;
        }

        List<String> cells()
        {
            long b = measurement.bytes();
            return Lists.newArrayList(Long.toString(b), Long.toString(b/numReads));
        }
        static final List<String> HEADERS = Lists.newArrayList("bytes", "/rd");
        static final List<String> EMPTY = Lists.newArrayList("n/a", "n/a");
    }

    private static final Map<String, CompactionSummary> compactionSummaries = new HashMap<>();
    private static final Map<String, ReadSummary> readSummaries = new HashMap<>();

    /*
    add to jvm args:
        -javaagent:${build.dir}/lib/jars/java-allocation-instrumenter-${allocation-instrumenter.version}.jar
     */

    private static final long MIN_OBJECTS_ALLOCATED;
    private static final long MIN_BYTES_ALLOCATED;

    static
    {
        if (AGENT_MEASUREMENT)
        {
            AgentMeasurement measurement = new AgentMeasurement();
            measurement.start();
            measurement.stop();
            MIN_OBJECTS_ALLOCATED = measurement.objectsAllocated;
            MIN_BYTES_ALLOCATED = measurement.bytesAllocated;
        }
        else
        {
            MIN_OBJECTS_ALLOCATED = 0;
            MIN_BYTES_ALLOCATED = 0;
            logger.warn("{} is using the ThreadMXBean to measure memory usage, this is less accurate than the allocation instrumenter agent", CompactionAllocationTest.class.getSimpleName());
            logger.warn("If you're running this in your IDE, add the following jvm arg: " +
                        "-javaagent:<build.dir>/lib/jars/java-allocation-instrumenter-<allocation-instrumenter.version>.jar " +
                        "(and replace <> with appropriate values from build.xml)");
        }
    }

    @BeforeClass
    public static void setupClass() throws Throwable
    {
        SchemaLoader.prepareServer();
        SchemaLoader.startGossiper();
        testTinyPartitions("warmup", 9, maybeInflate(300), true);
    }

    @AfterClass
    public static void afterClass()
    {

        logger.info("SUMMARIES:");
        for (String summary : summaries)
            logger.info(summary);


        List<List<String>> groups = new ArrayList<>();
        groups.add(Lists.newArrayList("tinyNonOverlapping3",
                                      "tinyNonOverlapping9",
                                      "tinyOverlapping3",
                                      "tinyOverlapping9"));
        groups.add(Lists.newArrayList("mediumNonOverlappingPartitions3",
                                      "mediumNonOverlappingPartitions9",
                                      "mediumOverlappingPartitions3",
                                      "mediumOverlappingPartitions9",
                                      "mediumPartitionsOverlappingRows3",
                                      "mediumPartitionsOverlappingRows9"));
        groups.add(Lists.newArrayList("wideNonOverlappingPartitions3",
                                      "wideNonOverlappingPartitions9",
                                      "wideOverlappingPartitions3",
                                      "wideOverlappingPartitions9",
                                      "widePartitionsOverlappingRows9",
                                      "widePartitionsOverlappingRows3"));
        groups.add(Lists.newArrayList("widePartitionsSingleIndexedColumn",
                                      "widePartitionsMultipleIndexedColumns"));

        Map<String, List<String>> fullRows = new HashMap<>();
        for (String workload : Iterables.concat(groups))
        {
            CompactionSummary cs = compactionSummaries.get(workload);
            ReadSummary rs = readSummaries.get(workload);
            fullRows.put(workload, Lists.newArrayList(Iterables.concat(cs != null ? cs.cells() : CompactionSummary.EMPTY,
                                                                       rs != null ? rs.cells() : ReadSummary.EMPTY)));
        }
        logger.info("");
        logger.info("TAB DELIMITED:");
        String header = Joiner.on('\t').join(Iterables.concat(CompactionSummary.HEADERS, ReadSummary.HEADERS));
        for (List<String> group: groups)
        {
            logger.info(Joiner.on('\t').join(group));
            logger.info(header);
            logger.info(Joiner.on('\t').join(Iterables.concat(Iterables.transform(group, g -> fullRows.getOrDefault(g, Collections.emptyList())))));
        }
    }

    private static int maybeInflate(int base, int inflate)
    {
        return PROFILING ? base * inflate : base;
    }

    private static int maybeInflate(int base)
    {
        return maybeInflate(base, 3);
    }

    private interface Workload
    {
        void setup();
        ColumnFamilyStore getCfs();
        String name();
        List<Runnable> getReads();

        default int executeReads()
        {
            List<Runnable> reads = getReads();
            for (int i=0; i<reads.size(); i++)
                reads.get(i).run();
            return reads.size();
        }

        default void executeCompactions()
        {
            ColumnFamilyStore cfs = getCfs();
            ActiveCompactions active = new ActiveCompactions();
            Set<SSTableReader> sstables = cfs.getLiveSSTables();

            CompactionTasks tasks = cfs.getCompactionStrategyManager()
                                       .getUserDefinedTasks(sstables, FBUtilities.nowInSeconds());
            
            Assert.assertFalse(tasks.isEmpty());

            for (AbstractCompactionTask task : tasks)
                task.execute(active);

            Assert.assertEquals(1, cfs.getLiveSSTables().size());
        }

        default int[] getSSTableStats()
        {
            int numPartitions = Ints.checkedCast(Iterables.getOnlyElement(getCfs().getLiveSSTables()).getSSTableMetadata().estimatedPartitionSize.count());
            int numRows = Ints.checkedCast(Iterables.getOnlyElement(getCfs().getLiveSSTables()).getSSTableMetadata().totalRows);
            return new int[] {numPartitions, numRows};
        }
    }

    private static Measurement createMeasurement()
    {
        return AGENT_MEASUREMENT ? new AgentMeasurement() : new MXMeasurement();
    }

    private interface Measurement
    {
        void start();

        void stop();

        long cpu();

        long bytes();

        long objects();

        default String prettyBytes()
        {
            return FBUtilities.prettyPrintMemory(bytes());
        }

    }

    public static class AgentMeasurement implements Measurement, Sampler
    {
        long objectsAllocated = 0;
        long bytesAllocated = 0;

        private final long threadID = Thread.currentThread().getId();

        public void sampleAllocation(int count, String desc, Object newObj, long bytes)
        {
            if (Thread.currentThread().getId() != threadID)
                return;

            objectsAllocated++;
            bytesAllocated += bytes;
        }

        public void start()
        {
            AllocationRecorder.addSampler(this);
        }

        public void stop()
        {
            AllocationRecorder.removeSampler(this);
            if (bytesAllocated == 0)
                logger.warn("no allocations recorded, make sure junit is run with -javaagent:${build.dir}/lib/jars/java-allocation-instrumenter-${allocation-instrumenter.version}.jar");
        }

        public long cpu()
        {
            return 0;
        }

        public long objects()
        {
            return objectsAllocated - MIN_OBJECTS_ALLOCATED;
        }

        public long bytes()
        {
            return bytesAllocated - MIN_BYTES_ALLOCATED;
        }
    }

    public static class MXMeasurement implements Measurement
    {
        private final Thread thread = Thread.currentThread();

        private class Point
        {
            long bytes;
            long cpu;

            void capture()
            {
                bytes = threadMX.getThreadAllocatedBytes(thread.getId());
                cpu = threadMX.getThreadCpuTime(thread.getId());
            }
        }

        private final Point start = new Point();
        private final Point stop = new Point();

        public void start()
        {
            start.capture();
        }

        public void stop()
        {
            stop.capture();
        }

        public long cpu()
        {
            return stop.cpu - start.cpu;
        }

        public long bytes()
        {
            return stop.bytes - start.bytes;
        }

        public long objects()
        {
            return 0;
        }
    }

    @Test
    public void allocMeasuring()
    {
        long size = ObjectSizes.measure(5);
        int numAlloc = 1000;

        Measurement measurement = createMeasurement();
        measurement.start();
        for (int i=0; i<numAlloc; i++)
            Integer.valueOf(i);

        measurement.stop();
        logger.info(" ** {}", measurement.prettyBytes());
        logger.info(" ** expected {}", size * numAlloc);
    }

    private static void measure(Workload workload) throws Throwable
    {
        workload.setup();

        Measurement readSampler = createMeasurement();
        Measurement compactionSampler = createMeasurement();

        String readSummary = "SKIPPED";
        if (!PROFILING_COMPACTION)
        {
            List<Runnable> reads = workload.getReads();
            readSampler.start();
            if (PROFILING_READS && !workload.name().equals("warmup"))
            {
                logger.info(">>> Start profiling");
                Thread.sleep(10000);
            }
            int readCount = workload.executeReads();
            Thread.sleep(1000);
            if (PROFILING_READS && !workload.name().equals("warmup"))
            {
                logger.info(">>> Stop profiling");
                Thread.sleep(10000);
            }
            readSampler.stop();
            readSummary = String.format("%s bytes, %s /read, %s cpu", readSampler.bytes(), readSampler.bytes()/readCount, readSampler.cpu());
            readSummaries.put(workload.name(), new ReadSummary(readSampler, readCount));
        }

        String compactionSummary = "SKIPPED";
        ColumnFamilyStore cfs = workload.getCfs();
        if (!PROFILING_READS)
        {
            compactionSampler.start();
            if (PROFILING_COMPACTION && !workload.name().equals("warmup"))
            {
                logger.info(">>> Start profiling");
                Thread.sleep(10000);
            }

            workload.executeCompactions();
            Thread.sleep(1000);
            if (PROFILING_COMPACTION && !workload.name().equals("warmup"))
            {
                logger.info(">>> Stop profiling");
                Thread.sleep(10000);
            }
            compactionSampler.stop();

            int[] tableStats = workload.getSSTableStats();
            int numPartitions = tableStats[0];
            int numRows = tableStats[1];

            compactionSummary = String.format("%s bytes, %s objects, %s /partition, %s /row, %s cpu", compactionSampler.bytes(), compactionSampler.objects(), compactionSampler.bytes()/numPartitions, compactionSampler.bytes()/numRows, compactionSampler.cpu());
            compactionSummaries.put(workload.name(), new CompactionSummary(compactionSampler, numPartitions, numRows));
        }

        cfs.truncateBlocking();

        logger.info("***");
        logger.info("*** {} reads summary", workload.name());
        logger.info(readSummary);
        logger.info("*** {} compaction summary", workload.name());
        logger.info(compactionSummary);
        if (!workload.name().equals("warmup"))
        {
            summaries.add(workload.name() + " reads summary: " + readSummary);
            summaries.add(workload.name() + " compaction summary: " + compactionSummary);
        }
        Thread.sleep(1000); // avoid losing report when running in IDE
    }

    private static final DataOutputPlus NOOP_OUT = new UnbufferedDataOutputStreamPlus()
    {
        public void write(byte[] buffer, int offset, int count) throws IOException {}

        public void write(int oneByte) throws IOException {}
    };

    private static void runQuery(ReadQuery query, TableMetadata metadata)
    {
        try (ReadExecutionController executionController = query.executionController();
             UnfilteredPartitionIterator partitions = query.executeLocally(executionController))
        {
            UnfilteredPartitionIterators.serializerForIntraNode().serialize(partitions, ColumnFilter.all(metadata), NOOP_OUT, MessagingService.current_version);
        }
        catch (IOException e)
        {
            throw new AssertionError(e);
        }
    }

    private static void testTinyPartitions(String name, int numSSTable, int sstablePartitions, boolean overlap) throws Throwable
    {
        String ksname = "ks_" + name.toLowerCase();

        SchemaLoader.createKeyspace(ksname, KeyspaceParams.simple(1),
                                    CreateTableStatement.parse("CREATE TABLE tbl (k INT PRIMARY KEY, v INT)", ksname).build());

        ColumnFamilyStore cfs = Schema.instance.getColumnFamilyStoreInstance(Schema.instance.getTableMetadata(ksname, "tbl").id);
        Assert.assertNotNull(cfs);
        cfs.disableAutoCompaction();
        List<Runnable> reads = new ArrayList<>(numSSTable * (overlap ? 1 : sstablePartitions));

        measure(new Workload()
        {
            public void setup()
            {
                cfs.disableAutoCompaction();
                String insert = String.format("INSERT INTO %s.%s (k, v) VALUES (?,?)", ksname, "tbl");
                String read = String.format("SELECT * FROM %s.%s WHERE k = ?", ksname, "tbl");
                SelectStatement select = (SelectStatement) QueryProcessor.parseStatement(read).prepare(ClientState.forInternalCalls());
                QueryState queryState = QueryState.forInternalCalls();
                for (int f = 0; f < numSSTable; f++)
                {
                    for (int p = 0; p < sstablePartitions; p++)
                    {
                        int key = overlap ? p : (f * sstablePartitions) + p;
                        QueryProcessor.executeInternal(insert, key, key);
                        if (!overlap || f == 0)
                        {
                            QueryOptions options = QueryProcessor.makeInternalOptions(select, new Object[]{f});
                            ReadQuery query = select.getQuery(options, queryState.getNowInSeconds());
                            reads.add(() -> runQuery(query, cfs.metadata.get()));
                        }
                    }
                    Util.flush(cfs);
                }

                Assert.assertEquals(numSSTable, cfs.getLiveSSTables().size());
            }

            public List<Runnable> getReads()
            {
                return reads;
            }

            public ColumnFamilyStore getCfs()
            {
                return cfs;
            }

            public String name()
            {
                return name;
            }
        });
    }

    @Test
    public void tinyNonOverlapping3() throws Throwable
    {
        testTinyPartitions("tinyNonOverlapping3", 3, maybeInflate(900, 6), false);
    }

    @Test
    public void tinyNonOverlapping9() throws Throwable
    {
        testTinyPartitions("tinyNonOverlapping9", 9, maybeInflate(300, 6), false);
    }

    @Test
    public void tinyOverlapping3() throws Throwable
    {
        testTinyPartitions("tinyOverlapping3", 3, maybeInflate(900, 6), true);
    }

    @Test
    public void tinyOverlapping9() throws Throwable
    {
        testTinyPartitions("tinyOverlapping9", 9, maybeInflate(300, 6), true);
    }

    private static final Random globalRandom = new Random();
    private static final Random localRandom = new Random();

    public static String makeRandomString(int length)
    {
        return makeRandomString(length, -1);

    }

    public static String makeRandomString(int length, int seed)
    {
        Random r;
        if (seed < 0)
        {
            r = globalRandom;
        }
        else
        {
            r = localRandom;
            r.setSeed(seed);
        }

        char[] chars = new char[length];
        for (int i = 0; i < length; ++i)
            chars[i] = (char) ('a' + r.nextInt('z' - 'a' + 1));
        return new String(chars);
    }

    private static void testMediumPartitions(String name, int numSSTable, int sstablePartitions, boolean overlap, boolean overlapCK) throws Throwable
    {
        String ksname = "ks_" + name.toLowerCase();

        SchemaLoader.createKeyspace(ksname, KeyspaceParams.simple(1),
                                    CreateTableStatement.parse("CREATE TABLE tbl (k text, c text, v1 text, v2 text, v3 text, v4 text, PRIMARY KEY (k, c))", ksname).build());

        ColumnFamilyStore cfs = Schema.instance.getColumnFamilyStoreInstance(Schema.instance.getTableMetadata(ksname, "tbl").id);
        Assert.assertNotNull(cfs);
        cfs.disableAutoCompaction();
        int rowsPerPartition = 200;
        List<Runnable> reads = new ArrayList<>(numSSTable * (overlap ? 1 : sstablePartitions));
        measure(new Workload()
        {
            public void setup()
            {
                cfs.disableAutoCompaction();
                String insert = String.format("INSERT INTO %s.%s (k, c, v1, v2, v3, v4) VALUES (?, ?, ?, ?, ?, ?)", ksname, "tbl");
                String read = String.format("SELECT * FROM %s.%s WHERE k = ?", ksname, "tbl");
                SelectStatement select = (SelectStatement) QueryProcessor.parseStatement(read).prepare(ClientState.forInternalCalls());
                QueryState queryState = QueryState.forInternalCalls();
                for (int f = 0; f < numSSTable; f++)
                {
                    for (int p = 0; p < sstablePartitions; p++)
                    {
                        String key = String.format("%08d", overlap ? p : (f * sstablePartitions) + p);
                        for (int r = 0; r < rowsPerPartition; r++)
                        {
                            QueryProcessor.executeInternal(insert, key, makeRandomString(6, overlapCK ? r : -1),
                                                           makeRandomString(8), makeRandomString(8),
                                                           makeRandomString(8), makeRandomString(8));

                        }
                        if (!overlap || f == 0)
                        {
                            QueryOptions options = QueryProcessor.makeInternalOptions(select, new Object[]{key});
                            ReadQuery query = select.getQuery(options, queryState.getNowInSeconds());
                            reads.add(() -> runQuery(query, cfs.metadata.get()));
                        }
                    }
                    Util.flush(cfs);
                }

                Assert.assertEquals(numSSTable, cfs.getLiveSSTables().size());
            }

            public ColumnFamilyStore getCfs()
            {
                return cfs;
            }

            public List<Runnable> getReads()
            {
                return reads;
            }

            public String name()
            {
                return name;
            }
        });
    }

    @Test
    public void mediumNonOverlappingPartitions3() throws Throwable
    {
        testMediumPartitions("mediumNonOverlappingPartitions3", 3, maybeInflate(60), false, false);
    }

    @Test
    public void mediumNonOverlappingPartitions9() throws Throwable
    {
        testMediumPartitions("mediumNonOverlappingPartitions9", 9, maybeInflate(20), false, false);
    }

    @Test
    public void mediumOverlappingPartitions3() throws Throwable
    {
        testMediumPartitions("mediumOverlappingPartitions3", 3, maybeInflate(60), true, false);
    }

    @Test
    public void mediumOverlappingPartitions9() throws Throwable
    {
        testMediumPartitions("mediumOverlappingPartitions9", 9, maybeInflate(20), true, false);
    }

    @Test
    public void mediumPartitionsOverlappingRows3() throws Throwable
    {
        testMediumPartitions("mediumPartitionsOverlappingRows3", 3, maybeInflate(60), true, true);
    }

    @Test
    public void mediumPartitionsOverlappingRows9() throws Throwable
    {
        testMediumPartitions("mediumPartitionsOverlappingRows9", 9, maybeInflate(20), true, true);
    }

    private static void testWidePartitions(String name, int numSSTable, int sstablePartitions, boolean overlap, boolean overlapCK) throws Throwable
    {
        String ksname = "ks_" + name.toLowerCase();

        SchemaLoader.createKeyspace(ksname, KeyspaceParams.simple(1),
                                    CreateTableStatement.parse("CREATE TABLE tbl (k text, c text, v1 text, v2 text, v3 text, v4 text, PRIMARY KEY (k, c))", ksname).build());

        ColumnFamilyStore cfs = Schema.instance.getColumnFamilyStoreInstance(Schema.instance.getTableMetadata(ksname, "tbl").id);
        Assert.assertNotNull(cfs);
        cfs.disableAutoCompaction();
        int rowWidth = 100;
        int rowsPerPartition = 1000;
        List<Runnable> reads = new ArrayList<>(numSSTable * (overlap ? 1 : sstablePartitions));

        measure(new Workload()
        {
            public void setup()
            {
                cfs.disableAutoCompaction();
                String insert = String.format("INSERT INTO %s.%s (k, c, v1, v2, v3, v4) VALUES (?, ?, ?, ?, ?, ?)", ksname, "tbl");
                String read = String.format("SELECT * FROM %s.%s WHERE k = ?", ksname, "tbl");
                SelectStatement select = (SelectStatement) QueryProcessor.parseStatement(read).prepare(ClientState.forInternalCalls());
                QueryState queryState = QueryState.forInternalCalls();
                for (int f = 0; f < numSSTable; f++)
                {
                    for (int p = 0; p < sstablePartitions; p++)
                    {
                        String key = String.format("%08d", overlap ? p : (f * sstablePartitions) + p);
                        for (int r = 0; r < rowsPerPartition; r++)
                        {
                            QueryProcessor.executeInternal(insert , key, makeRandomString(6, overlapCK ? r : -1),
                                                           makeRandomString(rowWidth>>2), makeRandomString(rowWidth>>2),
                                                           makeRandomString(rowWidth>>2), makeRandomString(rowWidth>>2));
                        }
                        if (!overlap || f == 0)
                        {
                            QueryOptions options = QueryProcessor.makeInternalOptions(select, new Object[]{key});
                            ReadQuery query = select.getQuery(options, queryState.getNowInSeconds());
                            reads.add(() -> runQuery(query, cfs.metadata.get()));
                        }
                    }
                    Util.flush(cfs);
                }

                Assert.assertEquals(numSSTable, cfs.getLiveSSTables().size());
            }

            public ColumnFamilyStore getCfs()
            {
                return cfs;
            }

            public List<Runnable> getReads()
            {
                return reads;
            }

            public String name()
            {
                return name;
            }
        });
    }

    @Test
    public void wideNonOverlappingPartitions3() throws Throwable
    {
        testWidePartitions("wideNonOverlappingPartitions3", 3, maybeInflate(24), false, false);
    }

    @Test
    public void wideNonOverlappingPartitions9() throws Throwable
    {
        testWidePartitions("wideNonOverlappingPartitions9", 9, maybeInflate(8), false, false);
    }

    @Test
    public void wideOverlappingPartitions3() throws Throwable
    {
        testWidePartitions("wideOverlappingPartitions3", 3, maybeInflate(24), true, false);
    }

    @Test
    public void wideOverlappingPartitions9() throws Throwable
    {
        testWidePartitions("wideOverlappingPartitions9", 9, maybeInflate(8), true, false);
    }

    @Test
    public void widePartitionsOverlappingRows9() throws Throwable
    {
        testWidePartitions("widePartitionsOverlappingRows9", 9, maybeInflate(8), true, true);
    }

    @Test
    public void widePartitionsOverlappingRows3() throws Throwable
    {
        testWidePartitions("widePartitionsOverlappingRows3", 3, maybeInflate(24), true, true);
    }


    private static void testIndexingWidePartitions(String name,
                                                   int numSSTable,
                                                   int sstablePartitions,
                                                   IndexDef...indexes) throws Throwable
    {
        String ksname = "ks_" + name.toLowerCase();
        SchemaLoader.createKeyspace(ksname, KeyspaceParams.simple(1),
                CreateTableStatement.parse("CREATE TABLE tbl (k text, c text, v1 text, v2 text, v3 text, v4 text, PRIMARY KEY (k, c))", ksname).build());

        ColumnFamilyStore cfs = Schema.instance.getColumnFamilyStoreInstance(Schema.instance.getTableMetadata(ksname, "tbl").id);
        Assert.assertNotNull(cfs);
        cfs.disableAutoCompaction();
        int rowWidth = 100;
        int rowsPerPartition = 1000;

        measure(new Workload()
        {
            @SuppressWarnings("UnstableApiUsage")
            public void setup()
            {
                cfs.disableAutoCompaction();
                String insert = String.format("INSERT INTO %s.%s (k, c, v1, v2, v3, v4) VALUES (?, ?, ?, ?, ?, ?)", ksname, "tbl");
                for (int f = 0; f < numSSTable; f++)
                {
                    for (int p = 0; p < sstablePartitions; p++)
                    {
                        String key = String.format("%08d", (f * sstablePartitions) + p);
                        for (int r = 0; r < rowsPerPartition; r++)
                        {
                            QueryProcessor.executeInternal(insert , key, makeRandomString(6, -1),
                                                           makeRandomString(rowWidth>>2), makeRandomString(rowWidth>>2),
                                                           makeRandomString(rowWidth>>2), makeRandomString(rowWidth>>2));
                        }
                    }
                    Util.flush(cfs);
                }

                for (IndexDef index : indexes)
                {
                    QueryProcessor.executeInternal(String.format(index.cql, index.name, ksname, "tbl"));
                    while (!cfs.indexManager.getBuiltIndexNames().contains(index.name))
                        Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
                }

                Assert.assertEquals(numSSTable, cfs.getLiveSSTables().size());
            }

            public ColumnFamilyStore getCfs()
            {
                return cfs;
            }

            public List<Runnable> getReads()
            {
                return new ArrayList<>();
            }

            public String name()
            {
                return name;
            }

            public int executeReads()
            {
                // return 1 to avoid divide by zero error
                return 1;
            }

            public void executeCompactions()
            {
                logger.info("Starting index re-build");
                try (ColumnFamilyStore.RefViewFragment viewFragment = cfs.selectAndReference(View.selectFunction(SSTableSet.CANONICAL));
                     Refs<SSTableReader> sstables = viewFragment.refs)
                {

                    Set<Index> indexes = new HashSet<>(cfs.indexManager.listIndexes());
                    SecondaryIndexBuilder builder = new CollatedViewIndexBuilder(cfs,
                                                                                 indexes,
                                                                                 new ReducingKeyIterator(sstables),
                                                                                 ImmutableSet.copyOf(sstables));
                    builder.build();
                }
                logger.info("Index re-build complete");
            }

            public int[] getSSTableStats()
            {
                int numPartitions = cfs.getLiveSSTables()
                                       .stream()
                                       .mapToInt(sstable -> Ints.checkedCast(sstable.getSSTableMetadata().estimatedPartitionSize.count()))
                                       .sum();
                int numRows = cfs.getLiveSSTables()
                                 .stream()
                                 .mapToInt(sstable -> Ints.checkedCast(sstable.getSSTableMetadata().totalRows))
                                 .sum();

                return new int[] {numPartitions, numRows};
            }
        });
    }

    @Test
    public void widePartitionsSingleIndexedColumn() throws Throwable
    {
        testIndexingWidePartitions("widePartitionsSingleIndexedColumn", 3, maybeInflate(24),
                new IndexDef("wide_partition_index_0", "CREATE INDEX %s on %s.%s(v1)"));
    }

    @Test
    public void widePartitionsMultipleIndexedColumns() throws Throwable
    {
        testIndexingWidePartitions("widePartitionsMultipleIndexedColumns", 3, maybeInflate(24),
                                   new IndexDef("wide_partition_index_0", "CREATE INDEX %s on %s.%s(v1)"),
                                   new IndexDef("wide_partition_index_1", "CREATE INDEX %s on %s.%s(v2)"),
                                   new IndexDef("wide_partition_index_2", "CREATE INDEX %s on %s.%s(v3)"),
                                   new IndexDef("wide_partition_index_3", "CREATE INDEX %s on %s.%s(v4)"));
    }

    static class IndexDef
    {
        final String name;
        final String cql;

        IndexDef(String name, String cql)
        {
            this.name = name;
            this.cql = cql;
        }
    }
}
