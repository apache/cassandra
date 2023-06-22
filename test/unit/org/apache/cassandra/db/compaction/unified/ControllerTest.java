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

package org.apache.cassandra.db.compaction.unified;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DiskBoundaries;
import org.apache.cassandra.db.compaction.CompactionStrategyOptions;
import org.apache.cassandra.db.compaction.UnifiedCompactionStrategy;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.ReplicationFactor;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MovingAverage;
import org.apache.cassandra.utils.Overlaps;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import static junit.framework.TestCase.assertNull;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.when;

@Ignore
public abstract class ControllerTest
{
    static final double epsilon = 0.00000001;
    static final long dataSizeGB = 512;
    static final int numShards = 4; // pick it so that dataSizeGB is exactly divisible or tests will break
    static final long sstableSizeMB = 2;
    static final double maxSpaceOverhead = 0.3d;
    static final boolean allowOverlaps = false;
    static final long checkFrequency= 600L;
    static final float tombstoneThresholdOption = 1;
    static final long tombstoneCompactionIntervalOption = 1;
    static final boolean uncheckedTombstoneCompactionOption = true;
    static final boolean logAllOption = true;
    static final String logTypeOption = "all";
    static final int logPeriodMinutesOption = 1;
    static final boolean compactionEnabled = true;
    static final double readMultiplier = 0.5;
    static final double writeMultiplier = 1.0;
    static final String tableName = "tbl";

    @Mock
    ColumnFamilyStore cfs;

    @Mock
    TableMetadata metadata;

    @Mock
    UnifiedCompactionStrategy strategy;

    @Mock
    ScheduledExecutorService executorService;

    @Mock
    ScheduledFuture fut;

    @Mock
    Environment env;

    @Mock
    AbstractReplicationStrategy replicationStrategy;

    @Mock
    DiskBoundaries boundaries;

    protected String keyspaceName = "TestKeyspace";
    protected int numDirectories = 1;

    @BeforeClass
    public static void setUpClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void setUp()
    {
        MockitoAnnotations.initMocks(this);

        when(strategy.getMetadata()).thenReturn(metadata);
        when(strategy.getEstimatedRemainingTasks()).thenReturn(0);

        when(metadata.toString()).thenReturn("");
        when(replicationStrategy.getReplicationFactor()).thenReturn(ReplicationFactor.fullOnly(3));
        when(cfs.getKeyspaceReplicationStrategy()).thenReturn(replicationStrategy);
        when(cfs.getKeyspaceName()).thenAnswer(invocation -> keyspaceName);
        when(cfs.getDiskBoundaries()).thenReturn(boundaries);
        when(cfs.getTableName()).thenReturn(tableName);
        when(boundaries.getNumBoundaries()).thenAnswer(invocation -> numDirectories);

        when(executorService.scheduleAtFixedRate(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class))).thenReturn(fut);

        when(env.flushSize()).thenReturn((double) (sstableSizeMB << 20));
    }

    Controller testFromOptions(boolean adaptive, Map<String, String> options)
    {
        addOptions(adaptive, options);
        Controller.validateOptions(options);

        Controller controller = Controller.fromOptions(cfs, options);
        assertNotNull(controller);
        assertNotNull(controller.toString());

        assertEquals(dataSizeGB << 30, controller.getDataSetSizeBytes());
        assertFalse(controller.isRunning());
        for (int i = 0; i < 5; i++) // simulate 5 levels
            assertEquals(Controller.DEFAULT_SURVIVAL_FACTOR, controller.getSurvivalFactor(i), epsilon);
        assertNull(controller.getCalculator());
        if (!options.containsKey(Controller.NUM_SHARDS_OPTION))
        {
            assertEquals(2, controller.getNumShards(0));
            assertEquals(16, controller.getNumShards(16 * 100 << 20));
            assertEquals(Overlaps.InclusionMethod.SINGLE, controller.overlapInclusionMethod());
        }
        else
        {
            int numShards = Integer.parseInt(options.get(Controller.NUM_SHARDS_OPTION));
            long minSSTableSize = FBUtilities.parseHumanReadableBytes(options.get(Controller.MIN_SSTABLE_SIZE_OPTION));
            assertEquals(1, controller.getNumShards(0));
            assertEquals(numShards, controller.getNumShards(numShards * minSSTableSize));
            assertEquals(numShards, controller.getNumShards(16 * 100 << 20));
        }

        return controller;
    }

    void testValidateOptions(Map<String, String> options, boolean adaptive)
    {
        addOptions(adaptive, options);
        options = Controller.validateOptions(options);
        assertTrue(options.toString(), options.isEmpty());
    }

    private static void putWithAlt(Map<String, String> options, String opt, String alt, int altShift, long altVal)
    {
        if (options.containsKey(opt) || options.containsKey(alt))
            return;
        if (ThreadLocalRandom.current().nextBoolean())
            options.put(opt, FBUtilities.prettyPrintMemory(altVal << altShift));
        else
            options.put(alt, Long.toString(altVal));
    }

    private static void addOptions(boolean adaptive, Map<String, String> options)
    {
        options.putIfAbsent(Controller.ADAPTIVE_OPTION, Boolean.toString(adaptive));
        putWithAlt(options, Controller.DATASET_SIZE_OPTION, Controller.DATASET_SIZE_OPTION_GB, 30, dataSizeGB);

        if (ThreadLocalRandom.current().nextBoolean())
            options.putIfAbsent(Controller.MAX_SPACE_OVERHEAD_OPTION, Double.toString(maxSpaceOverhead));
        else
            options.putIfAbsent(Controller.MAX_SPACE_OVERHEAD_OPTION, String.format("%.1f%%", maxSpaceOverhead * 100));

        options.putIfAbsent(Controller.ALLOW_UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION_OPTION, Boolean.toString(allowOverlaps));
        options.putIfAbsent(Controller.EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS_OPTION, Long.toString(checkFrequency));

        if (!options.containsKey(Controller.NUM_SHARDS_OPTION))
        {
            options.putIfAbsent(Controller.BASE_SHARD_COUNT_OPTION, Integer.toString(2));
            options.putIfAbsent(Controller.TARGET_SSTABLE_SIZE_OPTION, FBUtilities.prettyPrintMemory(100 << 20));
        }
        options.putIfAbsent(Controller.OVERLAP_INCLUSION_METHOD_OPTION, Overlaps.InclusionMethod.SINGLE.toString().toLowerCase());
    }

    void testStartShutdown(Controller controller)
    {
        assertNotNull(controller);

        assertEquals((long) dataSizeGB << 30, controller.getDataSetSizeBytes());
        assertEquals(numShards, controller.getNumShards(1));
        assertEquals((long) sstableSizeMB << 20, controller.getTargetSSTableSize());
        assertFalse(controller.isRunning());
        assertEquals(Controller.DEFAULT_SURVIVAL_FACTOR, controller.getSurvivalFactor(0), epsilon);
        assertNull(controller.getCalculator());

        controller.startup(strategy, executorService);
        assertTrue(controller.isRunning());
        assertNotNull(controller.getCalculator());

        controller.shutdown();
        assertFalse(controller.isRunning());
        assertNull(controller.getCalculator());

        controller.shutdown(); // no op
    }

    void testShutdownNotStarted(Controller controller)
    {
        assertNotNull(controller);

        controller.shutdown(); // no op.
    }

    void testStartAlreadyStarted(Controller controller)
    {
        assertNotNull(controller);

        controller.startup(strategy, executorService);
        assertTrue(controller.isRunning());
        assertNotNull(controller.getCalculator());

        controller.startup(strategy, executorService);
    }

    @Test
    public void testScalingParameterConversion()
    {
        testScalingParameterConversion("T4", 2);
        testScalingParameterConversion("L4", -2);
        testScalingParameterConversion("N", 0);
        testScalingParameterConversion("L2, T2, N", 0, 0, 0);
        testScalingParameterConversion("T10, T8, T4, N, L4, L6", 8, 6, 2, 0, -2, -4);
        testScalingParameterConversion("T10000, T1000, T100, T10, T2, L10, L100, L1000, L10000", 9998, 998, 98, 8, 0, -8, -98, -998, -9998);

        testScalingParameterParsing("-50 ,  T5  ,  3 ,  N  ,  L7 ,  +5 , -12  ,T9,L4,6,-7,+0,-0", -50, 3, 3, 0, -5, 5, -12, 7, -2, 6, -7, 0, 0);

        testScalingParameterError("Q6");
        testScalingParameterError("L4,,T5");
        testScalingParameterError("L1");
        testScalingParameterError("T1");
        testScalingParameterError("L0");
        testScalingParameterError("T0");
        testScalingParameterError("T-5");
        testScalingParameterError("T+5");
        testScalingParameterError("L-5");
        testScalingParameterError("L+5");
        testScalingParameterError("N3");
        testScalingParameterError("7T");
        testScalingParameterError("T,5");
        testScalingParameterError("L,5");
    }

    void testScalingParameterConversion(String definition, int... parameters)
    {
        testScalingParameterParsing(definition, parameters);

        String normalized = definition.replaceAll("T2|L2", "N");
        assertEquals(normalized, Controller.printScalingParameters(parameters));

        testScalingParameterParsing(Arrays.toString(parameters).replaceAll("[\\[\\]]", ""), parameters);
    }

    void testScalingParameterParsing(String definition, int... parameters)
    {
        assertArrayEquals(parameters, Controller.parseScalingParameters(definition));
    }

    void testScalingParameterError(String definition)
    {
        try
        {
            Controller.parseScalingParameters(definition);
            Assert.fail("Expected error on " + definition);
        }
        catch (ConfigurationException e)
        {
            // expected
        }
    }

    @Test
    public void testGetNumShards_growth_0()
    {
        Map<String, String> options = new HashMap<>();
        options.put(Controller.BASE_SHARD_COUNT_OPTION, Integer.toString(3));
        options.put(Controller.TARGET_SSTABLE_SIZE_OPTION, "100MiB");
        options.put(Controller.MIN_SSTABLE_SIZE_OPTION, "10MiB");
        Controller controller = Controller.fromOptions(cfs, options);
        assertEquals(0.0, controller.sstableGrowthModifier, 0.0);

        // Easy ones
        // x00 MiB = x * 100
        assertEquals(6, controller.getNumShards(Math.scalb(600, 20)));
        assertEquals(24, controller.getNumShards(Math.scalb(2400, 20)));
        assertEquals(6 * 1024, controller.getNumShards(Math.scalb(600, 30)));
        // Check rounding
        assertEquals(6, controller.getNumShards(Math.scalb(800, 20)));
        assertEquals(12, controller.getNumShards(Math.scalb(900, 20)));
        assertEquals(6 * 1024, controller.getNumShards(Math.scalb(800, 30)));
        assertEquals(12 * 1024, controller.getNumShards(Math.scalb(900, 30)));
        // Check lower limit
        assertEquals(3, controller.getNumShards(Math.scalb(200, 20)));
        assertEquals(3, controller.getNumShards(Math.scalb(100, 20)));
        assertEquals(3, controller.getNumShards(Math.scalb(50, 20)));
        assertEquals(3, controller.getNumShards(Math.scalb(30, 20)));
        // Check min size
        assertEquals(2, controller.getNumShards(Math.scalb(29, 20)));
        assertEquals(2, controller.getNumShards(Math.scalb(20, 20)));
        assertEquals(1, controller.getNumShards(Math.scalb(19, 20)));
        assertEquals(1, controller.getNumShards(5));
        assertEquals(1, controller.getNumShards(0));
        // Check upper limit
        assertEquals(3 * (int) Controller.MAX_SHARD_SPLIT, controller.getNumShards(Math.scalb(600, 40)));
        assertEquals(3 * (int) Controller.MAX_SHARD_SPLIT, controller.getNumShards(Math.scalb(10, 60)));
        assertEquals(3 * (int) Controller.MAX_SHARD_SPLIT, controller.getNumShards(Double.POSITIVE_INFINITY));
        // Check NaN
        assertEquals(3, controller.getNumShards(Double.NaN));
    }

    @Test
    public void testGetNumShards_growth_1()
    {
        Map<String, String> options = new HashMap<>();
        options.put(Controller.BASE_SHARD_COUNT_OPTION, Integer.toString(3));
        options.put(Controller.TARGET_SSTABLE_SIZE_OPTION, "100MiB");
        options.put(Controller.MIN_SSTABLE_SIZE_OPTION, "10MiB");
        options.put(Controller.SSTABLE_GROWTH_OPTION, "1.0");
        Controller controller = Controller.fromOptions(cfs, options);

        // Easy ones
        // x00 MiB = x * 100
        assertEquals(3, controller.getNumShards(Math.scalb(600, 20)));
        assertEquals(3, controller.getNumShards(Math.scalb(2400, 20)));
        assertEquals(3, controller.getNumShards(Math.scalb(600, 30)));
        // Check rounding
        assertEquals(3, controller.getNumShards(Math.scalb(800, 20)));
        assertEquals(3, controller.getNumShards(Math.scalb(900, 20)));
        assertEquals(3, controller.getNumShards(Math.scalb(800, 30)));
        assertEquals(3, controller.getNumShards(Math.scalb(900, 30)));
        // Check lower limit
        assertEquals(3, controller.getNumShards(Math.scalb(200, 20)));
        assertEquals(3, controller.getNumShards(Math.scalb(100, 20)));
        assertEquals(3, controller.getNumShards(Math.scalb(50, 20)));
        assertEquals(3, controller.getNumShards(Math.scalb(30, 20)));
        // Check min size
        assertEquals(2, controller.getNumShards(Math.scalb(29, 20)));
        assertEquals(2, controller.getNumShards(Math.scalb(20, 20)));
        assertEquals(1, controller.getNumShards(Math.scalb(19, 20)));
        assertEquals(1, controller.getNumShards(5));
        assertEquals(1, controller.getNumShards(0));
        // Check upper limit
        assertEquals(3, controller.getNumShards(Math.scalb(600, 40)));
        assertEquals(3, controller.getNumShards(Math.scalb(10, 60)));
        assertEquals(3, controller.getNumShards(Double.POSITIVE_INFINITY));
        // Check NaN
        assertEquals(3, controller.getNumShards(Double.NaN));
    }

    @Test
    public void testGetNumShards_legacy()
    {
        Map<String, String> options = new HashMap<>();
        options.put(Controller.NUM_SHARDS_OPTION, Integer.toString(3));
        mockFlushSize(100);
        Controller controller = Controller.fromOptions(cfs, options);

        // Easy ones
        // x00 MiB = x * 100
        assertEquals(3, controller.getNumShards(Math.scalb(600, 20)));
        assertEquals(3, controller.getNumShards(Math.scalb(2400, 20)));
        assertEquals(3, controller.getNumShards(Math.scalb(600, 30)));
        // Check rounding
        assertEquals(3, controller.getNumShards(Math.scalb(800, 20)));
        assertEquals(3, controller.getNumShards(Math.scalb(900, 20)));
        assertEquals(3, controller.getNumShards(Math.scalb(800, 30)));
        assertEquals(3, controller.getNumShards(Math.scalb(900, 30)));
        // Check lower limit
        assertEquals(3, controller.getNumShards(Math.scalb(600, 20)));
        assertEquals(3, controller.getNumShards(Math.scalb(500, 20)));
        assertEquals(3, controller.getNumShards(Math.scalb(400, 20)));
        assertEquals(3, controller.getNumShards(Math.scalb(300, 20)));
        // Check min size
        assertEquals(2, controller.getNumShards(Math.scalb(290, 20)));
        assertEquals(2, controller.getNumShards(Math.scalb(200, 20)));
        assertEquals(1, controller.getNumShards(Math.scalb(190, 20)));
        assertEquals(1, controller.getNumShards(5));
        assertEquals(1, controller.getNumShards(0));
        // Check upper limit
        assertEquals(3, controller.getNumShards(Math.scalb(600, 40)));
        assertEquals(3, controller.getNumShards(Math.scalb(10, 60)));
        assertEquals(3, controller.getNumShards(Double.POSITIVE_INFINITY));
        // Check NaN
        assertEquals(3, controller.getNumShards(Double.NaN));

        assertEquals(Integer.MAX_VALUE, controller.getReservedThreads());
    }

    @Test
    public void testGetNumShards_growth_1_2()
    {
        Map<String, String> options = new HashMap<>();
        options.put(Controller.BASE_SHARD_COUNT_OPTION, Integer.toString(3));
        options.put(Controller.TARGET_SSTABLE_SIZE_OPTION, "100MiB");
        options.put(Controller.MIN_SSTABLE_SIZE_OPTION, "10MiB");
        options.put(Controller.SSTABLE_GROWTH_OPTION, "0.5");
        Controller controller = Controller.fromOptions(cfs, options);

        // Easy ones
        // x00 MiB = x * 3 * 100
        assertEquals(3 * 2, controller.getNumShards(Math.scalb(4 * 3 * 100, 20)));
        assertEquals(3 * 4, controller.getNumShards(Math.scalb(16 * 3 * 100, 20)));
        assertEquals(3 * 32, controller.getNumShards(Math.scalb(3 * 100, 20 + 10)));
        // Check rounding. Note: Size must grow by 2x to get sqrt(2) times more shards.
        assertEquals(6, controller.getNumShards(Math.scalb(2350, 20)));
        assertEquals(12, controller.getNumShards(Math.scalb(2450, 20)));
        assertEquals(3 * 32, controller.getNumShards(Math.scalb(550, 30)));
        assertEquals(6 * 32, controller.getNumShards(Math.scalb(650, 30)));
        // Check lower limit
        assertEquals(3, controller.getNumShards(Math.scalb(200, 20)));
        assertEquals(3, controller.getNumShards(Math.scalb(100, 20)));
        assertEquals(3, controller.getNumShards(Math.scalb(50, 20)));
        assertEquals(3, controller.getNumShards(Math.scalb(30, 20)));
        // Check min size
        assertEquals(2, controller.getNumShards(Math.scalb(29, 20)));
        assertEquals(2, controller.getNumShards(Math.scalb(20, 20)));
        assertEquals(1, controller.getNumShards(Math.scalb(19, 20)));
        assertEquals(1, controller.getNumShards(5));
        assertEquals(1, controller.getNumShards(0));
        // Check upper limit
        assertEquals(3 * (int) Controller.MAX_SHARD_SPLIT, controller.getNumShards(Math.scalb(600, 60)));
        assertEquals(3 * (int) Controller.MAX_SHARD_SPLIT, controller.getNumShards(Math.scalb(10, 80)));
        assertEquals(3 * (int) Controller.MAX_SHARD_SPLIT, controller.getNumShards(Double.POSITIVE_INFINITY));
        // Check NaN
        assertEquals(3, controller.getNumShards(Double.NaN));
    }

    @Test
    public void testGetNumShards_growth_1_3()
    {
        Map<String, String> options = new HashMap<>();
        options.put(Controller.BASE_SHARD_COUNT_OPTION, Integer.toString(3));
        options.put(Controller.TARGET_SSTABLE_SIZE_OPTION, "100MiB");
        options.put(Controller.MIN_SSTABLE_SIZE_OPTION, "10MiB");
        options.put(Controller.SSTABLE_GROWTH_OPTION, "0.333");
        Controller controller = Controller.fromOptions(cfs, options);

        // Easy ones
        // x00 MiB = x * 3 * 100
        assertEquals(3 * 4, controller.getNumShards(Math.scalb(8 * 3 * 100, 20)));
        assertEquals(3 * 16, controller.getNumShards(Math.scalb(64 * 3 * 100, 20)));
        assertEquals(3 * 64, controller.getNumShards(Math.scalb(3 * 100, 20 + 9)));
        // Check rounding. Note: size must grow by 2 ^ 3/4 to get sqrt(2) times more shards
        assertEquals(12, controller.getNumShards(Math.scalb(4000, 20)));
        assertEquals(24, controller.getNumShards(Math.scalb(4100, 20)));
        assertEquals(3 * 64, controller.getNumShards(Math.scalb(500, 29)));
        assertEquals(6 * 64, controller.getNumShards(Math.scalb(550, 29)));
        // Check lower limit
        assertEquals(3, controller.getNumShards(Math.scalb(200, 20)));
        assertEquals(3, controller.getNumShards(Math.scalb(100, 20)));
        assertEquals(3, controller.getNumShards(Math.scalb(50, 20)));
        assertEquals(3, controller.getNumShards(Math.scalb(30, 20)));
        // Check min size
        assertEquals(2, controller.getNumShards(Math.scalb(29, 20)));
        assertEquals(2, controller.getNumShards(Math.scalb(20, 20)));
        assertEquals(1, controller.getNumShards(Math.scalb(19, 20)));
        assertEquals(1, controller.getNumShards(5));
        assertEquals(1, controller.getNumShards(0));
        // Check upper limit
        assertEquals(3 * (int) Controller.MAX_SHARD_SPLIT, controller.getNumShards(Math.scalb(600, 50)));
        assertEquals(3 * (int) Controller.MAX_SHARD_SPLIT, controller.getNumShards(Math.scalb(10, 60)));
        assertEquals(3 * (int) Controller.MAX_SHARD_SPLIT, controller.getNumShards(Double.POSITIVE_INFINITY));
        assertEquals(3, controller.getNumShards(Double.NaN));
    }

    @Test
    public void testMinSizeAuto()
    {
        Map<String, String> options = new HashMap<>();
        options.put(Controller.BASE_SHARD_COUNT_OPTION, Integer.toString(3));
        options.put(Controller.TARGET_SSTABLE_SIZE_OPTION, "200MiB");
        options.put(Controller.MIN_SSTABLE_SIZE_OPTION, "auto");
        mockFlushSize(45); // rounds up to 50MiB
        Controller controller = Controller.fromOptions(cfs, options);

        // Check min size
        assertEquals(2, controller.getNumShards(Math.scalb(149, 20)));
        assertEquals(2, controller.getNumShards(Math.scalb(100, 20)));
        assertEquals(1, controller.getNumShards(Math.scalb(99, 20)));
        assertEquals(1, controller.getNumShards(Math.scalb(50, 20)));
        assertEquals(1, controller.getNumShards(Math.scalb(49, 20)));
        assertEquals(1, controller.getNumShards(Math.scalb(10, 20)));

        // sanity check
        assertEquals(3, controller.getNumShards(Math.scalb(600, 20)));
        assertEquals(12, controller.getNumShards(Math.scalb(2400, 20)));
        assertEquals(3, controller.getNumShards(Math.scalb(400, 20)));
        assertEquals(3, controller.getNumShards(Math.scalb(200, 20)));
    }

    private void mockFlushSize(double d)
    {
        TableMetrics metrics = Mockito.mock(TableMetrics.class);
        MovingAverage flushSize = Mockito.mock(MovingAverage.class);
        when(cfs.metrics()).thenReturn(metrics);
        when(metrics.flushSizeOnDisk()).thenReturn(flushSize);
        when(flushSize.get()).thenReturn(Math.scalb(d, 20)); // rounds up to 50MiB
    }

    @Test
    public void testMinSizeAutoAtMostTargetMin()
    {
        Map<String, String> options = new HashMap<>();
        options.put(Controller.BASE_SHARD_COUNT_OPTION, Integer.toString(3));
        options.put(Controller.TARGET_SSTABLE_SIZE_OPTION, "200MiB");
        options.put(Controller.MIN_SSTABLE_SIZE_OPTION, "Auto");
        mockFlushSize(300); // above target min, set to 141MiB
        Controller controller = Controller.fromOptions(cfs, options);

        // Check min size
        assertEquals(2, controller.getNumShards(Math.scalb(400, 20)));
        assertEquals(2, controller.getNumShards(Math.scalb(300, 20)));
        assertEquals(1, controller.getNumShards(Math.scalb(200, 20)));
        assertEquals(1, controller.getNumShards(Math.scalb(100, 20)));
        // sanity check
        assertEquals(3, controller.getNumShards(Math.scalb(600, 20)));
        assertEquals(12, controller.getNumShards(Math.scalb(2400, 20)));
    }

    void testValidateCompactionStrategyOptions(boolean testLogType)
    {
        Map<String, String> options = new HashMap<>();
        options.put(CompactionStrategyOptions.TOMBSTONE_THRESHOLD_OPTION, Float.toString(tombstoneThresholdOption));
        options.put(CompactionStrategyOptions.TOMBSTONE_COMPACTION_INTERVAL_OPTION, Long.toString(tombstoneCompactionIntervalOption));
        options.put(CompactionStrategyOptions.UNCHECKED_TOMBSTONE_COMPACTION_OPTION, Boolean.toString(uncheckedTombstoneCompactionOption));

        if (testLogType)
            options.put(CompactionStrategyOptions.LOG_TYPE_OPTION, logTypeOption);
        else
            options.put(CompactionStrategyOptions.LOG_ALL_OPTION, Boolean.toString(logAllOption));

        options.put(CompactionStrategyOptions.LOG_PERIOD_MINUTES_OPTION, Integer.toString(logPeriodMinutesOption));
        options.put(CompactionStrategyOptions.COMPACTION_ENABLED, Boolean.toString(compactionEnabled));
        options.put(CompactionStrategyOptions.READ_MULTIPLIER_OPTION, Double.toString(readMultiplier));
        options.put(CompactionStrategyOptions.WRITE_MULTIPLIER_OPTION, Double.toString(writeMultiplier));

        CompactionStrategyOptions compactionStrategyOptions = new CompactionStrategyOptions(UnifiedCompactionStrategy.class, options, true);
        assertNotNull(compactionStrategyOptions);
        assertNotNull(compactionStrategyOptions.toString());
        assertEquals(tombstoneThresholdOption, compactionStrategyOptions.getTombstoneThreshold(), epsilon);
        assertEquals(tombstoneCompactionIntervalOption, compactionStrategyOptions.getTombstoneCompactionInterval());
        assertEquals(uncheckedTombstoneCompactionOption, compactionStrategyOptions.isUncheckedTombstoneCompaction());

        if (testLogType)
        {
            assertEquals((logTypeOption.equals("all") || logTypeOption.equals("events_only")), compactionStrategyOptions.isLogEnabled());
            assertEquals(logTypeOption.equals("all"), compactionStrategyOptions.isLogAll());
        }
        else
        {
            assertEquals(logAllOption, compactionStrategyOptions.isLogEnabled());
            assertEquals(logAllOption, compactionStrategyOptions.isLogAll());
        }
        assertEquals(logPeriodMinutesOption, compactionStrategyOptions.getLogPeriodMinutes());
        assertEquals(readMultiplier, compactionStrategyOptions.getReadMultiplier(), epsilon);
        assertEquals(writeMultiplier, compactionStrategyOptions.getWriteMultiplier(), epsilon);

        Map<String, String> uncheckedOptions = CompactionStrategyOptions.validateOptions(options);
        assertNotNull(uncheckedOptions);
    }
}