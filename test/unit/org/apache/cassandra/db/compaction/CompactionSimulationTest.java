/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.inject.Inject;

import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.commons.math3.distribution.AbstractIntegerDistribution;
import org.apache.commons.math3.distribution.UniformIntegerDistribution;
import org.apache.commons.math3.distribution.ZipfDistribution;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clearspring.analytics.hash.MurmurHash;
import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.clearspring.analytics.stream.cardinality.ICardinality;
import io.airlift.airline.Command;
import io.airlift.airline.HelpOption;
import io.airlift.airline.Option;
import io.airlift.airline.SingleCommand;

import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.compaction.unified.AdaptiveController;
import org.apache.cassandra.db.compaction.unified.Controller;
import org.apache.cassandra.db.compaction.unified.CostsCalculator;
import org.apache.cassandra.db.compaction.unified.Reservations;
import org.apache.cassandra.db.compaction.unified.StaticController;
import org.apache.cassandra.db.compaction.unified.Environment;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.utils.ExpMovingAverage;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MonotonicClock;
import org.apache.cassandra.utils.MovingAverage;
import org.apache.cassandra.utils.Overlaps;
import org.apache.cassandra.utils.PageAware;
import org.mockito.Mockito;

import static org.junit.Assert.*;
import static org.mockito.Mockito.when;

/**
 * A test that simulates compactions to see how strategies behave.
 * <p/>
 * SSTables are mocked with a specific cardinality {@link ICardinality} that provides
 * an estimated number of keys in the sstable, e.g. {@link HyperLogLogPlus}.
 * <p/>
 * Integers are sampled from a probability distribution such us {@link UniformIntegerDistribution} or {@link ZipfDistribution}
 * and offered to a cardinality object. When the estimated number of objects in the cardinality reaches a threshold,
 * an sstable is mocked using this cardinality and the compaction strategy is given the sstable and asked to check for
 * compactions. If there is a compaction task, then it is placed in a queue and the operation is repeated
 * from the beginning until the desired number of sampled integers has been reached.
 * <p/>
 * Another thread waits for compaction tasks that are put on the queue. When there is a compaction task,
 * the cardinalities of the sstables in the compaction task are merged and a new sstable is created with the
 * merged cardinality and given to the strategy, which checks again for any compaction events so that they can
 * be put on the queue as well. The process then continues until the queue is empty.
 * <p/>
 * The simulation completes when both threads have terminated.
 * <p/>
 * The size of the sstables is given by the estimated number of objects in the cardinality times a fixed size.
 * <p/>
 * The following values are calculated and reported at the end of the simulation:
 *
 * <li> Write Amplification (WA): number of entries written (by either flushing or compacting) / number of inserts </li>
 * <li> Read or Space Amplification (RA): histogram of sorted runs (anything better?) </li>
 * <li> Sorted runs existing at the end of the simulation </li>
 * <li> Compaction strategy statistics for the entire simulation </li>
 */
@Command(name = "compactionSim", description = "Compaction Simulation Tests")
@Ignore
public class CompactionSimulationTest extends BaseCompactionStrategyTest
{
    private final static Logger logger = LoggerFactory.getLogger(CompactionSimulationTest.class);

    private static final String logDirectory = System.getProperty("cassandra.logdir", ".");

    /**
     * The average time for flushing 1kb of data, as measured on Fallout tests ran on ironic.
     */
    private long flushTimeMicros = 20;

    /**
     * The average time for compacting 1kb of data, as measured on Fallout tests ran on ironic.
     */
    private long compactionTimeMicros = 45;

    /**
     * The average time for reading an entire partition, as measured on Fallout tests ran on ironic.
     */
    private long partitionReadLatencyMicros = 150;

    /** How often we append values to the csv file */
    private static final int csvUpdatePeriodMs = 500;

    /** Only collect values for final averages after this warmup period */
    private static final int warmupPeriodSec = 15;

    /** The minimum sstable size in bytes */
    private static final long sstableSize = 500 << 20; // 50 MB

    /** The number of unique keys that cause an sstable to be flushed, the value size is calculated by dividing
     * {@link this#sstableSize} by this value. The smaller this value is, the greater the number of sstables generated.
     */
    private static final int uniqueKeysPerSStable = 50000;

    /** When calculating the read cost, we multiply by this factor to simulate a Bloom Filter false positive rate of 1%. So
     * we estimate that we'll access 1% of the live sstables.
     */
    private static final double bfFactor = 0.01;

    /**
     * When calculating the read cost, we multiply by this factor to simulate a cache hit rate of 1 - cacheFactor.
     */
    private static final double cacheFactor = 0.05;

    @Inject
    public HelpOption helpOption;

    @Option(name = { "-wl", "--workload" }, description = "Workload type specified as RXX_WXX, e.g. R50_W50")
    String workload = "R50_W50";

    @Option(name = { "-t", "--type" }, description = "The test type: either \"static\" or \"adaptive\"")
    String type = "adaptive";

    @Option(name= {"-min"}, description = "The minimum value of W")
    int minW = -10;

    @Option(name= {"-max"}, description = "The maximum value of W")
    int maxW = 32;

    @Option(name= {"--data-size"}, description = "The data set size in GB")
    int datasetSizeGB = 32;

    @Option(name= {"--num-shards"}, description = "The number of compaction shards")
    int numShards = 4;

    @Option(name= {"--min-cost"}, description = "The minimum cost for adaptive analysis")
    int minCost = 5;

    @Option(name= {"--max-adaptive-compactions"}, description = "The max nunmber of concurrent adaptive compactions")
    int maxAdaptiveCompactions = 5;

    @Option(name= {"--gain"}, description = "The gain for adaptive analysis")
    double gain = 0.15;

    @Option(name= {"-step"}, description = "The step size for W for static analysis")
    int stepW = 2;

    @Option(name= {"-w"}, description = "The initial value of W for adaptive analysis")
    int W = 0;

    @Option(name= {"-update-time"}, description = "The update interval in seconds for adaptive analysis")
    int updateTimeSec = 15;

    @Option(name= {"-duration"}, description = "The duration in minutes for adaptive analysis or for each step in static analysis")
    int durationMinutes = 1;

    @Option(name= {"-expired-sstable-check-frequency"}, description = "How often to check for expired SSTables")
    long expiredSSTableCheckFrequency = 600;

    @Option(name= {"-unsafe-aggressive-sstable-expiration"}, description = "Whether to drop expired SSTables without checking if the partitions appear in other SSTables")
    boolean ignoreOverlaps = false;

    @Option(name= {"-base-shard-count"}, description = "Base shard count, 4 by default")
    int baseShardCount = 4;

    @Option(name= {"-target_sstable_size_mb"}, description = "Target sstable size in mb, 1024 by default")
    long targetSSTableSizeMB = 1024;

    @Option(name= {"-overlap-inclusion-method"}, description = "Overlap inclusion method, NONE, SINGLE or TRANSITIVE")
    Overlaps.InclusionMethod overlapInclusionMethod = Overlaps.InclusionMethod.TRANSITIVE;

    @BeforeClass
    public static void setUpClass()
    {
        BaseCompactionStrategyTest.setUpClass();
    }

    @Before
    public void setUp()
    {
        setUp(numShards);
        logger.info("Simulation set up for data size of {} GiB, {} shards", datasetSizeGB, numShards);
    }

    public static void main(String[] args) throws Exception
    {
        setUpClass();

        CompactionSimulationTest test = SingleCommand.singleCommand(CompactionSimulationTest.class).parse(args);

        if (test.helpOption.showHelpIfRequested())
            return;

        test.setUp();
        test.run();
    }

    public void run() throws Exception
    {
        Pattern WL_REGEX = Pattern.compile("^R(\\d+)_W(\\d+)$");
        Matcher matcher = WL_REGEX.matcher(workload.toUpperCase());
        if (!matcher.matches())
            throw new IllegalArgumentException(String.format("Invalid workload %s.", workload));

        int readRowsSec = Integer.parseInt(matcher.group(1)) * 10000;
        int writeRowsSec = Integer.parseInt(matcher.group(2)) * 10000;
        System.out.println(String.format("Running %s with %d read rows / sec and %d write rows /sec", workload, readRowsSec, writeRowsSec));
        logger.info("Running {} with {} read rows / sec and {} write rows /sec", workload, readRowsSec, writeRowsSec);

        if (type.toLowerCase().equals("static"))
            testStaticAnalysis(workload, readRowsSec, writeRowsSec);
        else if (type.toLowerCase().equals("adaptive"))
            testAdaptiveController(workload, readRowsSec, writeRowsSec);
        else
            throw new IllegalArgumentException("Invalid type: " + type);

    }

    @Test
    public void testAdaptiveController_R50_W50() throws Exception
    {
        int readRowsSec = 50_000;
        int writeRowsSec = 50_000;

        testAdaptiveController("R50_W50", readRowsSec, writeRowsSec);
    }

    @Test
    public void testStaticAnalysis_R50_W50() throws Exception
    {
        int readRowsSec = 50_000;
        int writeRowsSec = 50_000;

        testStaticAnalysis("R50_W50", readRowsSec, writeRowsSec);
    }

    @Test
    public void testSingleW() throws Exception
    {
        int W = 2; // similar to tiered with 4 sorted runs per bucket
        int writeRowsSec = 1_000_000;
        int readRowsSec = 1_000_000;
        int maxKey = 30_000_000;

        CsvWriter csvWriter = CsvWriter.make("testUniform_UnifiedStrategy");
        testUniform(false, csvWriter, W, sstableSize, TimeUnit.MINUTES.toMillis(1), maxKey, readRowsSec, writeRowsSec, NO_OP_OBSERVER);
    }

    /**
     * Run a simulation using {@link UnifiedCompactionStrategy} with an initial value of W and let the adaptive
     * controller choose the best value depending on the workloa
     */
    private void testAdaptiveController(String dataSetName, int readRowsSec, int writeRowsSec) throws Exception
    {
        int maxKey = 100_000_000;

        String csvFileName = "testAdaptiveController_" + dataSetName;
        CsvWriter csvWriter = CsvWriter.make(csvFileName);

        testUniform(true, csvWriter, W, sstableSize, TimeUnit.MINUTES.toMillis(durationMinutes), maxKey, readRowsSec, writeRowsSec, NO_OP_OBSERVER);
        clearSSTables();
    }

    /**
     * Run a simulation using {@link UnifiedCompactionStrategy} with different values of W and for a different number of
     * trials. Report the average IO cost over the period. This can then be plotted as a function of W to show the
     * impact that W has on the IO cost depending on the workload type (see callers).
     */
    private void testStaticAnalysis(String dataSetName, int readRowsSec, int writeRowsSec) throws Exception
    {
        int maxKey = 50_000_000;

        String csvFileName = "testStaticAnalysis_" + dataSetName;
        CsvWriter csvWriter = CsvWriter.make(csvFileName);

        for (int w = minW; w <= maxW; w += stepW)
        {
            testUniform(false, csvWriter, w, sstableSize, TimeUnit.MINUTES.toMillis(durationMinutes), maxKey, readRowsSec, writeRowsSec, NO_OP_OBSERVER);
            clearSSTables();
        }
    }

    private void testUniform(boolean adaptive,
                             CsvWriter csvWriter,
                             int W,
                             long sstableSize,
                             long durationMillis,
                             int maxKey,
                             int readRowsSec,
                             int writeRowsSec,
                             SimulationObserver observer) throws Exception
    {
        if (maxKey <= 0)
            fail("Maxkey should be positive");

        int valueSize = (int) Math.ceil(sstableSize / (double) uniqueKeysPerSStable); // value length for each key

        logger.debug("Running simulation with uniform distribution, max key: {}, duration: {} ms, maxKey: {}, keys/sstable: {}, value size: {}, min sstable size: {}",
                     maxKey, durationMillis, maxKey, uniqueKeysPerSStable, valueSize, FBUtilities.prettyPrintMemory(sstableSize));

        AbstractIntegerDistribution distribution = new UniformIntegerDistribution(random, 0, maxKey);

        Counters counters = new Counters();
        UnifiedCompactionStrategy strategy = createUnifiedCompactionStrategy(counters, adaptive, W, sstableSize, valueSize);

        Simulation simulation = new Simulation(strategy,
                                               distribution,
                                               csvWriter,
                                               counters,
                                               maxKey,
                                               uniqueKeysPerSStable,
                                               valueSize,
                                               durationMillis,
                                               readRowsSec, writeRowsSec,
                                               observer);
        simulation.run();
    }

    private void clearSSTables()
    {
        Iterable<SSTableReader> sstables = Iterables.concat(dataTracker.getLiveSSTables(), dataTracker.getCompacting());
        for (SSTableReader sstable : sstables)
            Mockito.reset(sstable);

        dataTracker.removeUnsafe(dataTracker.getLiveSSTables());
        dataTracker.removeCompactingUnsafe(dataTracker.getCompacting());
        repairedAt = System.currentTimeMillis();

        assertTrue(dataTracker.getLiveSSTables().isEmpty());
        assertTrue(dataTracker.getCompacting().isEmpty());
    }

    private UnifiedCompactionStrategy createUnifiedCompactionStrategy(Counters counters, boolean adaptive, int W, long sstableSize, int valueSize)
    {
        double o = 1.0;
        int[] Ws = new int[] { W };
        int[] previousWs = new int[] { W };
        double maxSpaceOverhead = 0.2;

        Controller controller = adaptive
                                ? new AdaptiveController(MonotonicClock.preciseTime,
                                                         new SimulatedEnvironment(counters, valueSize), Ws, previousWs,
                                                         new double[] { o },
                                                         ((long) datasetSizeGB) << 33,  // leave some room
                                                         sstableSize,
                                                         0,
                                                         0,
                                                         maxSpaceOverhead,
                                                         0,
                                                         expiredSSTableCheckFrequency,
                                                         ignoreOverlaps,
                                                         baseShardCount,
                                                         targetSSTableSizeMB << 20,
                                                         0,
                                                         0,
                                                         Reservations.Type.PER_LEVEL,
                                                         overlapInclusionMethod,
                                                         updateTimeSec,
                                                         minW,
                                                         maxW,
                                                         gain,
                                                         minCost,
                                                         maxAdaptiveCompactions,
                                                         "ks",
                                                         "tbl")
                                : new StaticController(new SimulatedEnvironment(counters, valueSize),
                                                       Ws,
                                                       new double[] { o },
                                                       ((long) datasetSizeGB) << 33,  // leave some room
                                                       sstableSize,
                                                       0,
                                                       0,
                                                       maxSpaceOverhead, // MB
                                                       0,
                                                       expiredSSTableCheckFrequency,
                                                       ignoreOverlaps,
                                                       baseShardCount,
                                                       targetSSTableSizeMB << 20,
                                                       0,
                                                       0,
                                                       Reservations.Type.PER_LEVEL,
                                                       overlapInclusionMethod,
                                                       "ks",
                                                       "tbl");

        return new UnifiedCompactionStrategy(strategyFactory, controller);
    }

    private final static class CsvWriter
    {
        private final OutputStreamWriter updateWriter;
        private final OutputStreamWriter averagesWriter;
        private boolean headerWritten;

        private CsvWriter(String fileName) throws IOException
        {
            this.updateWriter =  new OutputStreamWriter(Files.newOutputStream(Paths.get(logDirectory, fileName + ".csv"), StandardOpenOption.CREATE, StandardOpenOption.WRITE));
            this.averagesWriter =  new OutputStreamWriter(Files.newOutputStream(Paths.get(logDirectory, fileName + "-avg.csv"), StandardOpenOption.CREATE, StandardOpenOption.WRITE));
            this.headerWritten = false;
        }

        static CsvWriter make(String fileName) throws IOException
        {
            return new CsvWriter(fileName);
        }

        void writeHeader(String toWrite)
        {
            if (!headerWritten)
            {
                performWrite(toWrite, updateWriter);
                performWrite(toWrite, averagesWriter);
                headerWritten = true;
            }
        }

        void write(String toWrite)
        {
            performWrite(toWrite, updateWriter);
        }

        void writeAverages(String toWrite)
        {
            performWrite(toWrite, averagesWriter);
        }

        private synchronized void performWrite(String toWrite, OutputStreamWriter writer)
        {
            try
            {
                writer.write(toWrite);
                writer.flush();
            }
            catch (IOException ex)
            {
                logger.error("Failed to write to csv: ", ex);
            }
        }
    }

    /**
     * Some counters for the simulation
     */
    private final static class Counters
    {
        /** The simulated number of rows inserted by the user. */
        final AtomicLong numInserted = new AtomicLong(0L);

        /** The simulated number of rows requested by the user. */
        final AtomicLong numRequested = new AtomicLong(0L);

        /** The simulated number of rows flushed */
        final AtomicLong numFlushed = new AtomicLong(0L);

        /** The simulated number of rows read during compaction */
        final AtomicLong numReadForCompaction = new AtomicLong(0L);

        /** The simulated number of rows written during compaction */
        final AtomicLong numWrittenForCompaction = new AtomicLong(0L);

        /** The simulated number of rows written to disk (by flushing or compactions). */
        final AtomicLong numWritten = new AtomicLong(0L);

        /** The number of compactions simulated */
        final AtomicLong numCompactions = new AtomicLong(0L);

        /** The number of compactions submitted but not yet executed */
        final AtomicInteger numCompactionsPending = new AtomicInteger(0);

        /** The number of sstables that have been compacted away */
        final AtomicLong numCompactedSSTables = new AtomicLong(0L);

        void reset()
        {
            numInserted.set(0);
            numRequested.set(0);
            numFlushed.set(0);
            numReadForCompaction.set(0);
            numWrittenForCompaction.set(0);
            numWritten.set(0);
            numCompactions.set(0);
            numCompactionsPending.set(0);
            numCompactedSSTables.set(0);
        }

        @Override
        public String toString()
        {
            return String.format("Ins: %d (%d%%), Req: %d (%d%%), Flushed: %d, Written: %d",
                                 numInserted.get(),
                                 percentageInserted(),
                                 numRequested.get(),
                                 percentageRead(),
                                 numFlushed.get(),
                                 numWritten.get());
        }

        int percentageInserted()
        {
            double tot = Math.max(1, numInserted.get() + numRequested.get());
            return (int) ((numInserted.get() / tot) * 100);
        }

        int percentageRead()
        {
            double tot = Math.max(1, numInserted.get() + numRequested.get());
            return (int) ((numRequested.get() / tot) * 100);
        }
    }

    /**
     * An implementation of {@link Environment} that uses simulated values.
     */
    private class SimulatedEnvironment implements Environment
    {
        final Counters counters;
        final int valueSize;

        SimulatedEnvironment(Counters counters, int valueSize)
        {
            this.counters = counters;
            this.valueSize = valueSize;
        }

        @Override
        public MovingAverage makeExpMovAverage()
        {
            return ExpMovingAverage.decayBy100();
        }

        @Override
        public double cacheMissRatio()
        {
            return cacheFactor;
        }

        @Override
        public double bloomFilterFpRatio()
        {
            return bfFactor;
        }

        @Override
        public int chunkSize()
        {
            return PageAware.PAGE_SIZE;
        }

        @Override
        public long bytesInserted()
        {
            return counters.numInserted.get() * valueSize;
        }

        @Override
        public long partitionsRead()
        {
            return counters.numRequested.get();
        }

        @Override
        public double sstablePartitionReadLatencyNanos()
        {
            return TimeUnit.MICROSECONDS.toNanos(partitionReadLatencyMicros);
        }

        @Override
        public double compactionTimePerKbInNanos()
        {
            // this is slightly incorrect, we would need to measure the size of compacted sstables
            return TimeUnit.MICROSECONDS.toNanos(compactionTimeMicros);
        }

        @Override
        public double flushTimePerKbInNanos()
        {
            return TimeUnit.MICROSECONDS.toNanos(flushTimeMicros);
        }

        @Override
        public double WA()
        {
            double bytesFlushed = counters.numFlushed.get() * valueSize;
            double bytesCompacted = counters.numWrittenForCompaction.get() * valueSize;
            return bytesFlushed <= 0 ? 0 : (bytesFlushed + bytesCompacted) / bytesFlushed;
        }

        @Override
        public double flushSize()
        {
            return uniqueKeysPerSStable * valueSize; // a rough estimation should be fine
        }

        @Override
        public int maxConcurrentCompactions()
        {
            return DatabaseDescriptor.getConcurrentCompactors();
        }

        @Override
        public double maxThroughput()
        {
            return Double.MAX_VALUE;
        }

        @Override
        public long getOverheadSizeInBytes(CompactionPick compactionPick)
        {
            return compactionPick.totSizeInBytes();
        }

        @Override
        public String toString()
        {
            return String.format("Read latency: %d us / partition, flush latency: %d us / KiB, compaction latency: %d us / KiB, bfpr: %f, measured WA: %.2f, flush size %s",
                                 TimeUnit.NANOSECONDS.toMicros((long) sstablePartitionReadLatencyNanos()),
                                 TimeUnit.NANOSECONDS.toMicros((long) flushTimePerKbInNanos()),
                                 TimeUnit.NANOSECONDS.toMicros((long) compactionTimePerKbInNanos()),
                                 bloomFilterFpRatio(),
                                 WA(),
                                 FBUtilities.prettyPrintMemory((long)flushSize()));
        }
    }

    /**
     * The output of the simulation
     */
    private final class SimulationOutput
    {
        /** The initial timestamp */
        private final long start;

        /** The compaction strategy */
        private final UnifiedCompactionStrategy strategy;

        /** The compaction cost calculator */
        private final CostsCalculator calculator;

        /** Save the read IO costs after the warm-up period for calculating the final average and stddev, TODO - can we do it wihtout a list? */
        private final List<Double> readIOCosts;

        /** Save the write IO costs after the warm-up period for calculating the final average and stddev, TODO - can we do it wihtout a list? */
        private final List<Double> writeIOCosts;

        /**
         * Creates an initial empty status and writes the header to the CSV file.
         */
        SimulationOutput(long start, CsvWriter writer, UnifiedCompactionStrategy strategy)
        {
            this.start = start;
            this.strategy = strategy;
            this.calculator = strategy.getController().getCalculator();
            this.readIOCosts = new ArrayList<>();
            this.writeIOCosts = new ArrayList<>();

            writeCSVHeader(writer);
        }

        private void writeCSVHeader(CsvWriter writer)
        {
            writer.writeHeader(String.join(",",
                                           "timestamp ms",
                                           "W",
                                           "num compactions",
                                           "live sstables",
                                           "space used bytes",
                                           "Tot Num inserted",
                                           "Tot Num read",
                                           "% inserted",
                                           "% read",
                                           "Read IO",
                                           "Read IO stddev",
                                           "Write IO",
                                           "Write IO stddev",
                                           "Tot IO",
                                           "Tot IO stddev",
                                           "Num. pending",
                                           "WA")
                               + System.lineSeparator());
        }

        private void write(CsvWriter writer, Counters counters)
        {
            int W = strategy.getW(0);
            long length = (long) Math.ceil(calculator.spaceUsed());
            int RA = strategy.getController().readAmplification(length, W);
            int WA = strategy.getController().writeAmplification(length, W);

            double readIOCost = calculator.getReadCostForQueries(RA);
            double writeIOCost = calculator.getWriteCostForQueries(WA);

            if (System.currentTimeMillis() - start >= TimeUnit.SECONDS.toMillis(warmupPeriodSec))
            {
                this.readIOCosts.add(readIOCost);
                this.writeIOCosts.add(writeIOCost);
            }

            String toWrite = String.join(",",
                                         toString(System.currentTimeMillis() - start),
                                         toString(W),
                                         toString(counters.numCompactions.get()),
                                         toString(calculator.numSSTables()),
                                         toString(length),
                                         toString(counters.numInserted.get()),
                                         toString(counters.numRequested.get()),
                                         toString(counters.percentageInserted()),
                                         toString(counters.percentageRead()),
                                         toString(readIOCost),
                                         "0",
                                         toString(writeIOCost),
                                         "0",
                                         toString(readIOCost + writeIOCost),
                                         "0",
                                         toString(counters.numCompactionsPending.get() + strategy.getEstimatedRemainingTasks()),
                                         toString(calculator.getEnv().WA()))
                             + System.lineSeparator();

            writer.write(toWrite);
        }

        private void writeAverages(CsvWriter writer, Counters counters)
        {
            double writeIOCostAvg = average(writeIOCosts);
            double writeIOCostStd = stddev(writeIOCostAvg, writeIOCosts);

            double readIOCostAvg = average(readIOCosts);
            double readIOCostStd = stddev(readIOCostAvg, readIOCosts);


            String toWrite = String.join(",",
                                         toString(System.currentTimeMillis() - start),
                                         toString(strategy.getW(0)),
                                         toString(counters.numCompactions.get()),
                                         toString(calculator.numSSTables()),
                                         toString(calculator.spaceUsed()),
                                         toString(counters.numInserted.get()),
                                         toString(counters.numRequested.get()),
                                         toString(counters.percentageInserted()),
                                         toString(counters.percentageRead()),
                                         toString(readIOCostAvg),
                                         toString(readIOCostStd),
                                         toString(writeIOCostAvg),
                                         toString(writeIOCostStd),
                                         toString(readIOCostAvg + writeIOCostAvg),
                                         toString(readIOCostStd + writeIOCostStd),
                                         toString(counters.numCompactionsPending.get() + strategy.getEstimatedRemainingTasks()),
                                         toString(calculator.getEnv().WA()))
                             + System.lineSeparator();

            writer.writeAverages(toWrite);
        }

        public double average(List<Double> vals)
        {
            return vals.isEmpty() ? 0 : vals.stream().reduce(Double::sum).get() / vals.size();
        }

        public double stddev(double avg, List<Double> vals)
        {
            if (vals.isEmpty())
                return 0;

            double sd = 0;
            for (double v : vals)
                sd += Math.pow(v - avg, 2);

            return Math.sqrt(sd / vals.size());
        }

        private String toString(long val)
        {
            return String.format("%d", val);
        }

        private String toString(double val)
        {
            return String.format("%.6f", val);
        }

        @Override
        public String toString()
        {
            long elapsed = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - start);
            return String.format("W: %d, num. sstables: %d, elapsed: %d s",
                                 strategy.getW(0),
                                 dataTracker.getLiveSSTables().size(),
                                 elapsed);
        }
    }

    /**
     * A simple state machine for the simulation
     */
    private enum SimulationState
    {
        NONE, // the simulation hasn't yet started or is pre-loading data
        SETTING_UP, // the simulation is setting up, e.g. pre-loading data and waiting for initial set of compactions
        RUNNING, // the simulation is running: inserting data and/or reading data and reporting the output
        TEARING_DOWN, //the simulation is tearing down (waiting for threads to complete)
        DONE // the simulation is finishing compactions or done
    }

    /**
     * Implemented by tests that need to react to simulation progress
     */
    private interface SimulationObserver
    {
        void onChange(SimulationState state);
    }

    private static SimulationObserver NO_OP_OBSERVER = state -> {};

    /**
     * The implementation of the simulation
     */
    private final class Simulation
    {
        /** The strategy to test */
        private final UnifiedCompactionStrategy strategy;

        /** The distribution is used to generated values to be inserted */
        private final AbstractIntegerDistribution distribution;

        /** The simulation output will be passed to this csv writer */
        private final CsvWriter csvWriter;

        /** The maximum key to insert when pre-loading data, this is also normally the maximum key value of the data distribution */
        private final int maxKey;

        /** The number of unique keys that trigger an sstable to be created */
        private final int uniqueKeysPerSStable;

        /** The fixed value size for each key inserted */
        private final int valueSize;

        /** The simulation duration in milliseconds, it will keep on reading and writing for this period of time
         * according to the rate limiters below */
        private final long durationMillis;

        /** The insertion rate limited is based on the insert rows / sec received in the c.tor */
        private final RateLimiter writeRate;

        /** The read rate limited is based on the read rows / sec received in the c.tor */
        private final RateLimiter readRate;

        /** These are the compactions that have been submitted by the strategy */
        private final BlockingQueue<AbstractCompactionTask> compactions;

        /** The cardinalities for the sstables to be flushed */
        private final BlockingQueue<ICardinality> flushing;

        /** The simulation counters */
        private final Counters counters;

        /** This is set in case of error to fail the test */
        private final AtomicReference<Throwable> error;

        /** A simulation observer */
        private final SimulationObserver observer;

        /** The simulation state */
        private final AtomicReference<SimulationState> state;

        /** The start of the simulation */
        private volatile long start;

        /** Contains output parameters that the simulation should produce periodically. */
        private volatile SimulationOutput output;

        /**
         * Create a new simulation
         * @param strategy the strategy to test
         * @param distribution the distribution to generate random integer keys
         * @param csvWriter writes statistics to a csv file
         * @param maxKey the maximum value of the key,
         * @param uniqueKeysPerSStable the number of unique keys that trigger an sstable to be created
         * @param valueSize the fixed size for the value associated to each key
         * @param durationMillis the duration of the simulation read and write phases
         * @param readRowsSec the simulated number of rows to be read every second
         * @param writeRowsSec the simulated number of rows to be inserted every second
         */
        Simulation(UnifiedCompactionStrategy strategy,
                   AbstractIntegerDistribution distribution,
                   CsvWriter csvWriter,
                   Counters counters,
                   int maxKey,
                   int uniqueKeysPerSStable,
                   int valueSize,
                   long durationMillis,
                   int readRowsSec, int writeRowsSec,
                   SimulationObserver observer)
        {
            this.strategy = strategy;
            this.distribution = distribution;
            this.csvWriter = csvWriter;
            this.maxKey = maxKey;
            this.uniqueKeysPerSStable = uniqueKeysPerSStable;
            this.valueSize = valueSize;
            this.durationMillis = durationMillis;
            this.writeRate = writeRowsSec > 0 ? RateLimiter.create(writeRowsSec) : null;
            this.readRate = readRowsSec > 0 ? RateLimiter.create(readRowsSec) : null;
            this.compactions =  new ArrayBlockingQueue<>(512); // flushing / compaction thread will be blocked when queue is full
            this.flushing = new ArrayBlockingQueue<>(256); // insert thread will be blocked when queue is full
            this.counters = counters;

            this.error = new AtomicReference<>(null);
            this.state = new AtomicReference<>(SimulationState.NONE);
            this.observer = observer;
        }

        void run() throws Exception
        {
            if (state.get() != SimulationState.NONE)
                throw new IllegalStateException("Simulation already run!");

            try
            {
                NamedThreadFactory threadFactory = new NamedThreadFactory("Simulation-worker");

                setState(SimulationState.NONE, SimulationState.SETTING_UP);

                this.start = System.currentTimeMillis();
                strategy.getController().startup(strategy, ScheduledExecutors.scheduledTasks);
                this.output = new SimulationOutput(start, csvWriter, strategy);

                int numThreads = strategy.getController().maxConcurrentCompactions();

                CountDownLatch settingUpDone = new CountDownLatch(1);
                CountDownLatch runningDone = new CountDownLatch(2);
                CountDownLatch tearingDownDone = new CountDownLatch(3 + numShards); // 1 reporter, 2 flusher and num shards compacting threads

                threadFactory.newThread(new RunAndCountDown(settingUpDone, "preload", this::preloadData)).start();
                threadFactory.newThread(new RunAndCountDown(tearingDownDone, "report", this::reportOutput)).start();

                for (int i = 0; i < numThreads; i++)
                    threadFactory.newThread(new RunAndCountDown(tearingDownDone, "compact " + i, this::compactData)).start();

                settingUpDone.await();

                if (error.get() != null)
                    throw new RuntimeException("Simulation has failed");

                waitForCompactionsToSettle();

                setState(SimulationState.SETTING_UP, SimulationState.RUNNING);
                this.start = System.currentTimeMillis();
                //this.counters.reset();

                threadFactory.newThread(new RunAndCountDown(tearingDownDone, "flush 1", this::flushData)).start();
                threadFactory.newThread(new RunAndCountDown(tearingDownDone, "flush 2", this::flushData)).start();
                threadFactory.newThread(new RunAndCountDown(runningDone, "insert", () -> runOrWait(this::insertData, writeRate))).start();
                threadFactory.newThread(new RunAndCountDown(runningDone, "read", () -> runOrWait(this::readData, readRate))).start();

                runningDone.await();

                if (error.get() != null)
                    throw new RuntimeException("Simulation has failed");

                setState(SimulationState.RUNNING, SimulationState.TEARING_DOWN);

                waitForCompactionsToSettle();

                tearingDownDone.await();

                summarize();
            }
            finally
            {
                setState(SimulationState.TEARING_DOWN, SimulationState.DONE);

                if (strategy.getController().isRunning())
                    strategy.getController().shutdown();
            }
        }

        private class RunAndCountDown implements Runnable
        {
            CountDownLatch done;
            String what;
            Runnable task;

            RunAndCountDown(CountDownLatch done, String what, Runnable task)
            {
                this.done = done;
                this.what = what;
                this.task = task;
            }

            @Override
            public void run()
            {
                try
                {
                    logger.debug("Running \"{}\"", what);
                    task.run();
                }
                catch (Throwable t)
                {
                    SimulationState currentState = state.get();
                    logger.error("Unexpected error during \"{}\" with state {}:", what, currentState, t);

                    error.compareAndSet(null, t);

                    if (currentState.ordinal() < SimulationState.TEARING_DOWN.ordinal())
                        setState(currentState, SimulationState.TEARING_DOWN);
                }
                finally
                {
                    logger.debug("Finished \"{}\"", what);
                    done.countDown();
                }
            }
        }


        private void setState(SimulationState from, SimulationState to)
        {
            logger.debug("Updating simulation state from {} to {}", from, to);

            if (state.compareAndSet(from, to))
                observer.onChange(to);
            else
                throw new IllegalStateException(String.format("Failed to update simulation state from %s to %s", from, to));
        }

        void waitForCompactionsToSettle()
        {
            logger.debug("Waiting for compactions to settle...");

            for (int i = 0; i < 3; i++)
            { // 3 attempts in case the queue is temporarily empty before submitting a new compaction
                while (!compactions.isEmpty())
                {
                    FBUtilities.sleepQuietly(1000);
                    logger.debug("{}, live sstables: {}, compacting: {}, pending compactions: {}, pending flushing: {}, elapsed: {} s",
                                 counters,
                                 dataTracker.getLiveSSTables().size(),
                                 dataTracker.getCompacting().size(),
                                 compactions.size() + strategy.getEstimatedRemainingTasks(),
                                 flushing.size(),
                                 TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - start));
                }
            }

            logger.debug("Compactions settled, live sstables: {}", dataTracker.getLiveSSTables().size());
        }

        void summarize()
        {
            if (error.get() != null)
            {
                Throwable err = error.get();
                err.printStackTrace();
                fail("Simulation failed with exception: " + err.getClass().getCanonicalName() + '/' + err.getMessage());

                return;
            }

            long elapsedMs = System.currentTimeMillis() - start;
            logger.info("Total time: {}s  WA : {}", TimeUnit.SECONDS.convert(elapsedMs, TimeUnit.MILLISECONDS), strategy.getController().getEnv().WA());
            logger.info("Final outputs: {} {}", counters, output);

            logger.info("Strategy aggregated statistics:");
            logger.info(strategy.getStatistics().toString());
        }

        /**
         * If the rate limiter is null simply sleep for the entire duration, otherwise run the task.
         */
        private void runOrWait(Runnable task, RateLimiter rateLimiter)
        {
            if (rateLimiter != null)
                task.run();
            else
                FBUtilities.sleepQuietly(durationMillis);
        }

        /**
         * Insert the entire key space.
         */
        private void preloadData()
        {
            ICardinality cardinality = newCardinality();

            byte[] scratchBytes = new byte[8];
            ByteBuffer scratch = ByteBuffer.wrap(scratchBytes);
            long numToFlush;
            int lastFlushed = 0;
            long lastLogged = System.currentTimeMillis();
            long maxBytesToInsert = (long) datasetSizeGB << 30;
            long bytesInserted = 0;
            int i = 0;

            logger.info("Inserting up to {}", FBUtilities.prettyPrintMemory(maxBytesToInsert));

            try
            {
                while(bytesInserted < maxBytesToInsert)
                {
                    scratch.clear();
                    scratch.putLong(0, i);
                    long hash = MurmurHash.hash64(scratchBytes, scratchBytes.length);
                    cardinality.offerHashed(hash);

                    counters.numInserted.incrementAndGet();
                    bytesInserted += valueSize;

                    i++;
                    if (i == maxKey)
                        i = 0;

                    if (System.currentTimeMillis()- lastLogged >= TimeUnit.SECONDS.toMillis(1))
                    {
                        lastLogged = System.currentTimeMillis();
                        logger.debug("Ins: {}, keys: {}, live sstables: {}, compacting: {}, pending compactions: {}",
                                     FBUtilities.prettyPrintMemory(bytesInserted),
                                     i,
                                     dataTracker.getLiveSSTables().size(),
                                     dataTracker.getCompacting().size(),
                                     compactions.size() + strategy.getEstimatedRemainingTasks());
                    }

                    if (i >= (lastFlushed + uniqueKeysPerSStable) && // no point in checking the cardinality until we've inserted uniqueKeysPerSStable more entries
                        (numToFlush = cardinality.cardinality()) >= uniqueKeysPerSStable)
                    {
                        counters.numFlushed.addAndGet(numToFlush);
                        lastFlushed = i;
                        generateSSTables(cardinality, numToFlush, partitioner.getMinimumToken(), partitioner.getMaximumToken(), "preload", true);

                        cardinality = newCardinality();
                    }

                    if (i % 1000 == 0 && state.get() == SimulationState.TEARING_DOWN)
                    { // this happens if the compaction threads fail
                        logger.debug("Interrupting preload, simulation is tearing down");
                        break;
                    }
                }

                if ((numToFlush = cardinality.cardinality()) > 0)
                {
                    counters.numFlushed.addAndGet(numToFlush);
                    generateSSTables(cardinality, numToFlush, partitioner.getMinimumToken(), partitioner.getMaximumToken(), "preload", true);
                }
            }
            catch (Exception e)
            {
                logger.error("Exception happen during preloading", e);
            }
        }

        /**
         * Simulate inserting data and generating sstables when the cardinality has reached {@link this#uniqueKeysPerSStable} unique entries.
         */
        private void insertData()
        {
            ICardinality cardinality = newCardinality();

            int numSSTables = 0;
            long numFlushed = 0;

            byte[] scratchBytes = new byte[8];
            ByteBuffer scratch = ByteBuffer.wrap(scratchBytes);

            long now;
            long lastLogged = start;
            try
            {
                while((now = System.currentTimeMillis()) - start <= durationMillis)
                {
                    scratch.clear();
                    scratch.putLong(0, distribution.sample());
                    long hash = MurmurHash.hash64(scratchBytes, scratchBytes.length);
                    cardinality.offerHashed(hash);

                    counters.numInserted.incrementAndGet();
                    writeRate.acquire();

                    if (now - lastLogged >= TimeUnit.SECONDS.toMillis(1))
                    {
                        lastLogged = now;
                        logger.debug("{}, live sstables: {}, compacting: {}, pending compactions: {}, pending flushing: {}, elapsed: {} s",
                                     counters,
                                     dataTracker.getLiveSSTables().size(),
                                     dataTracker.getCompacting().size(),
                                     compactions.size() + strategy.getEstimatedRemainingTasks(),
                                     flushing.size(),
                                     TimeUnit.MILLISECONDS.toSeconds(now - start));

                        if (state.get() == SimulationState.TEARING_DOWN)
                            break;
                    }

                    if (counters.numInserted.get() >= (numFlushed + uniqueKeysPerSStable) && // no point in checking the cardinality until we've inserted uniqueKeysPerSStable more entries
                        cardinality.cardinality() >= uniqueKeysPerSStable)
                    {
                        numFlushed = counters.numInserted.get();
                        numSSTables++;

                        flushing.put(cardinality);
                        cardinality = newCardinality();
                    }
                }

                // generate one final sstable
                if (cardinality.cardinality() > 0)
                {
                    numSSTables++;
                    flushing.put(cardinality);
                }

                logger.debug("Status: {} {}, sstables: {}, completed inserting data", counters, output, numSSTables);
            }
            catch (InterruptedException e)
            {
                logger.error("Exception happen during insertion", e);
            }
        }

        /** Simulate reading some data */
        private void readData()
        {
            while(System.currentTimeMillis() - start <= durationMillis)
            {
                counters.numRequested.incrementAndGet();
                readRate.acquire();

                if (state.get() == SimulationState.TEARING_DOWN)
                    break;
            }
        }

        /**
         * Convert the compaction statistics to the simulation output and append it to the csv file.
         */
        void reportOutput()
        {
            while(state.get().ordinal() < SimulationState.TEARING_DOWN.ordinal())
            {
                FBUtilities.sleepQuietly(csvUpdatePeriodMs);
                doReportOutput(false);
            }

            doReportOutput(true);
        }

        private void doReportOutput(boolean isLast)
        {
            if (isLast)
                output.writeAverages(csvWriter, counters);
            else
                output.write(csvWriter, counters);

            logger.trace("{} {}", counters, output);
        }

        /**
         * Take the cardinalities from the flushing queue and generate sstables.
         * @throws Exception
         */
        private void flushData()
        {
            try
            {
                while(state.get().ordinal() < SimulationState.TEARING_DOWN.ordinal() || !flushing.isEmpty())
                {
                    ICardinality cardinality = flushing.poll(1, TimeUnit.MILLISECONDS);
                    if (cardinality == null)
                        continue;

                    long numToFlush = cardinality.cardinality();
                    counters.numFlushed.addAndGet(numToFlush);
                    generateSSTables(cardinality, numToFlush, partitioner.getMinimumToken(), partitioner.getMaximumToken(), "flushing", true);
                }
            }
            catch (InterruptedException e)
            {
                logger.error("Exception happen during flushing", e);
            }
        }

        /**
         * Perform the following:
         *
         * <li>Take compaction tasks from {@link this#compactions}</li>
         * <li>Merge the cardinality of the txn sstables</li>
         * <li>Generate a new merged sstable</li>
         * <li>Pass it to the strategy and live sstables</li>
         * <li>Check with the strategy if there is a new compaction task</li>
         */
        private void compactData()
        {
            try
            {
                while(state.get().ordinal() < SimulationState.TEARING_DOWN.ordinal() || !compactions.isEmpty())
                {
                    AbstractCompactionTask task = compactions.poll(1, TimeUnit.SECONDS);
                    if (task == null)
                    {
                        logger.info("no task");
                        continue;
                    }

                    LifecycleTransaction txn = task.transaction();
                    Set<SSTableReader> candidates = txn.originals();
                    for (SSTableReader candidate : candidates)
                        counters.numReadForCompaction.addAndGet(candidate.keyCardinalityEstimator().cardinality());

                    UUID id = txn.opId();

                    //strategy.getBackgroundCompactions().setInProgress(mockCompletedCompactionProgress(candidates, id));
                    ICardinality merged = getMerged(candidates);

                    counters.numWrittenForCompaction.addAndGet(merged.cardinality());

                    // first remove the sstables to avoid overlaps when adding the new one for LCS
                    dataTracker.removeUnsafe(candidates);
                    dataTracker.removeCompactingUnsafe(candidates);

                    // first create the new merged sstable
                    generateSSTables(merged, merged.cardinality(),
                                     candidates.stream().map(x -> x.getFirst().getToken()).min(Comparator.naturalOrder()).get(),
                                     candidates.stream().map(x -> x.getLast().getToken()).max(Comparator.naturalOrder()).get(),
                                     "compacting", false);
                    //Thread.sleep(5);

                    // then remove the old sstables
                    strategy.onCompleted(id, true);
                    counters.numCompactions.incrementAndGet();
                    counters.numCompactionsPending.decrementAndGet();
                    counters.numCompactedSSTables.addAndGet(candidates.size());

                    logger.debug("Executed {} compactions, live sstables: {}, compacting sstables: {}, compacted sstables: {}",
                                 counters.numCompactions, dataTracker.getLiveSSTables().size(), dataTracker.getCompacting().size(), counters.numCompactedSSTables);

                    maybeSubmitCompaction();

                    txn.unsafeClose();
                }
                logger.debug("...completed monitoring compactions");
            }
            catch (InterruptedException | CardinalityMergeException e)
            {
                logger.error("Exception happen during compaction", e);
            }
        }

        /**
         * Merge the cardinalities of the input sstables.
         *
         * @return the merged cardinality
         *
         * @throws CardinalityMergeException
         */
        private ICardinality getMerged(Set<SSTableReader> candidates) throws CardinalityMergeException
        {
            ICardinality[] cardinalities = new ICardinality[candidates.size() - 1];
            int i = 0;
            ICardinality first = null;

            for (SSTableReader sstable : candidates)
            {
                if (first == null)
                    first = sstable.keyCardinalityEstimator();
                else
                    cardinalities[i++] = sstable.keyCardinalityEstimator();
            }

            return first.merge(cardinalities);
        }

        /**
         * Create a new cardinality with similar parameters as those used in {@link MetadataCollector}.
         * See CASSANDRA-5906 for error and size details. Instead of using 12, 25 we use 12,24 since that
         * halves the memory used (2.7k instead of 5.5k for 10k entries) and we can tollerate a slightly larger error.
         *
         * @return a newly constructed cardinality
         */
        private ICardinality newCardinality()
        {
            return new HyperLogLogPlus(12, 24); // for real sstables in MetadataCollector we use 13, 25
        }

        /**
         * Create one or more mocked sstables based on the cardinality received and the value size. The theoretical sstable size
         * (numEntries * valueSize) will be split across multiple compaction shards.
         *
         * @param cardinality - the cardinality used to simulate the sstable
         * @param numEntries - the total number of entries to write to disk
         * @param reason - the reason (flushing, compacting, etc)
         * @param checkForCompaction- if true we check if a compaction needs to be submitted
         */
        private void generateSSTables(ICardinality cardinality, long numEntries, Token minToken, Token maxToken, String reason, boolean checkForCompaction) throws InterruptedException
        {
            // The theoretical sstable size that is being mocked
            long sstableSize = numEntries * valueSize;
            IPartitioner partitioner = minToken.getPartitioner();

            int shards = strategy.getController().getNumShards(valueSize * numEntries / minToken.size(maxToken));
            ShardTracker boundaries = strategy.getShardManager().boundaries(shards);

            int numSStables = 0;
            boundaries.advanceTo(minToken);
            while (true)
            {
                ++numSStables;
                if (boundaries.shardEnd() == null || maxToken.compareTo(boundaries.shardEnd()) <= 0)
                    break;
                boundaries.advanceTo(boundaries.shardEnd().nextValidToken());
            }

            boundaries = strategy.getShardManager().boundaries(shards);

            List<SSTableReader> sstables = new ArrayList<>(numSStables);
            long keyCount = (long) Math.ceil(numEntries / (double) numSStables);
            long bytesOnDisk = valueSize * keyCount;
            long timestamp = System.currentTimeMillis();

            boundaries.advanceTo(minToken);
            while (true)
            {
                Range<Token> span = boundaries.shardSpan();
                Token firstToken = span.left.nextValidToken();
                if (minToken.compareTo(firstToken) > 0)
                    firstToken = minToken;
                Token lastToken = partitioner.split(span.left, span.right, 1 - Math.scalb(1, -24)); // something that is < span.right
                if (maxToken.compareTo(lastToken) < 0)
                    lastToken = maxToken;
                DecoratedKey first = new BufferDecoratedKey(firstToken, ByteBuffer.allocate(0));
                DecoratedKey last = new BufferDecoratedKey(lastToken, ByteBuffer.allocate(0));

                SSTableReader sstable = mockSSTable(0, bytesOnDisk, timestamp, 0, first, last, 0, true, null, 0);
                when(sstable.keyCardinalityEstimator()).thenReturn(cardinality);
                when(sstable.estimatedKeys()).thenReturn(keyCount);
                sstables.add(sstable);

                if (boundaries.shardEnd() == null || maxToken.compareTo(boundaries.shardEnd()) <= 0)
                    break;
                boundaries.advanceTo(boundaries.shardEnd().nextValidToken());
            }

            counters.numWritten.addAndGet(numEntries);
            dataTracker.addInitialSSTablesWithoutUpdatingSize(sstables);
            logger.debug("Generated {} new sstables for {}, live: {}, compacting: {}, tot sstable size {}",
                         sstables.size(), reason, dataTracker.getLiveSSTables().size(), dataTracker.getCompacting().size(),
                         sstableSize);

            if (checkForCompaction)
                maybeSubmitCompaction();
        }

        private void maybeSubmitCompaction() throws InterruptedException
        {
            Collection<AbstractCompactionTask> tasks = strategy.getNextBackgroundTasks(FBUtilities.nowInSeconds());
            for (AbstractCompactionTask task : tasks)
            {
                compactions.put(task);
                counters.numCompactionsPending.incrementAndGet();
                logger.debug("Submitted new compaction, live sstables: {}, compacting sstables: {}, compacted sstables: {}",
                             dataTracker.getLiveSSTables().size(), dataTracker.getCompacting().size(), counters.numCompactedSSTables);
            }
        }
    }
}