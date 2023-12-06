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

package org.apache.cassandra.db.compaction.unified;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DiskBoundaries;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.compaction.UnifiedCompactionStrategy;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Overlaps;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

public class ControllerTest
{
    static final double epsilon = 0.00000001;
    static final boolean allowOverlaps = false;
    static final long checkFrequency= 600L;

    @Mock
    ColumnFamilyStore cfs;

    @Mock
    TableMetadata metadata;

    @Mock
    UnifiedCompactionStrategy strategy;

    protected String keyspaceName = "TestKeyspace";
    protected DiskBoundaries diskBoundaries = new DiskBoundaries(cfs, null, null, 0, 0);

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
        when(cfs.getKeyspaceName()).thenAnswer(invocation -> keyspaceName);
        when(cfs.getDiskBoundaries()).thenAnswer(invocation -> diskBoundaries);
    }

    Controller testFromOptions(Map<String, String> options)
    {
        addOptions(false, options);
        Controller.validateOptions(options);

        Controller controller = Controller.fromOptions(cfs, options);
        assertNotNull(controller);
        assertNotNull(controller.toString());

        for (int i = 0; i < 5; i++) // simulate 5 levels
            assertEquals(Controller.DEFAULT_SURVIVAL_FACTOR, controller.getSurvivalFactor(i), epsilon);
        assertEquals(1, controller.getNumShards(0));
        assertEquals(4, controller.getNumShards(16 * 100 << 20));
        assertEquals(Overlaps.InclusionMethod.SINGLE, controller.overlapInclusionMethod());

        return controller;
    }

    @Test
    public void testValidateOptions()
    {
        testValidateOptions(false);
    }

    @Test
    public void testValidateOptionsIntegers()
    {
        testValidateOptions(true);
    }

    void testValidateOptions(boolean useIntegers)
    {
        Map<String, String> options = new HashMap<>();
        addOptions(useIntegers, options);
        options = Controller.validateOptions(options);
        assertTrue(options.toString(), options.isEmpty());
    }

    private static void addOptions(boolean useIntegers, Map<String, String> options)
    {
        String wStr = Arrays.stream(Ws)
                            .mapToObj(useIntegers ? Integer::toString : UnifiedCompactionStrategy::printScalingParameter)
                            .collect(Collectors.joining(","));
        options.putIfAbsent(Controller.SCALING_PARAMETERS_OPTION, wStr);

        options.putIfAbsent(Controller.ALLOW_UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION_OPTION, Boolean.toString(allowOverlaps));
        options.putIfAbsent(Controller.EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS_OPTION, Long.toString(checkFrequency));

        options.putIfAbsent(Controller.BASE_SHARD_COUNT_OPTION, Integer.toString(2));
        options.putIfAbsent(Controller.TARGET_SSTABLE_SIZE_OPTION, FBUtilities.prettyPrintMemory(100 << 20));
        // The below value is based on the value in the above statement. Decreasing the above statement should result in a decrease below.
        options.putIfAbsent(Controller.MIN_SSTABLE_SIZE_OPTION, "70.710MiB");
        options.putIfAbsent(Controller.OVERLAP_INCLUSION_METHOD_OPTION, Overlaps.InclusionMethod.SINGLE.toString().toLowerCase());
        options.putIfAbsent(Controller.SSTABLE_GROWTH_OPTION, "0.5");
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
    public void testGetNumShards()
    {
        Map<String, String> options = new HashMap<>();
        options.putIfAbsent(Controller.BASE_SHARD_COUNT_OPTION, Integer.toString(3));
        options.putIfAbsent(Controller.TARGET_SSTABLE_SIZE_OPTION, FBUtilities.prettyPrintMemory(100 << 20));
        options.put(Controller.MIN_SSTABLE_SIZE_OPTION, "0B");
        options.put(Controller.SSTABLE_GROWTH_OPTION, "0.0");
        Controller.validateOptions(options);
        Controller controller = Controller.fromOptions(cfs, options);

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
        assertEquals(3, controller.getNumShards(Math.scalb(10, 20)));
        assertEquals(3, controller.getNumShards(5));
        assertEquals(3, controller.getNumShards(0));
        // Check upper limit
        assertEquals(3 * (int) Controller.MAX_SHARD_SPLIT, controller.getNumShards(Math.scalb(600, 40)));
        assertEquals(3 * (int) Controller.MAX_SHARD_SPLIT, controller.getNumShards(Math.scalb(10, 60)));
        assertEquals(3 * (int) Controller.MAX_SHARD_SPLIT, controller.getNumShards(Double.POSITIVE_INFINITY));
    }

    @Test
    public void testGetNumShards_growth_0()
    {
        Map<String, String> options = new HashMap<>();
        options.put(Controller.BASE_SHARD_COUNT_OPTION, Integer.toString(3));
        options.put(Controller.TARGET_SSTABLE_SIZE_OPTION, "100MiB");
        options.put(Controller.MIN_SSTABLE_SIZE_OPTION, "10MiB");
        options.put(Controller.SSTABLE_GROWTH_OPTION, "0.0");
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
        assertEquals(1, controller.getNumShards(Math.scalb(29, 20)));
        assertEquals(1, controller.getNumShards(Math.scalb(20, 20)));
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
        assertEquals(1, controller.getNumShards(Math.scalb(29, 20)));
        assertEquals(1, controller.getNumShards(Math.scalb(20, 20)));
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
        assertEquals(1, controller.getNumShards(Math.scalb(29, 20)));
        assertEquals(1, controller.getNumShards(Math.scalb(20, 20)));
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
        assertEquals(1, controller.getNumShards(Math.scalb(29, 20)));
        assertEquals(1, controller.getNumShards(Math.scalb(20, 20)));
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
    public void testGetNumShards_minSize_10MiB_b_3()
    {
        Map<String, String> options = new HashMap<>();
        options.put(Controller.BASE_SHARD_COUNT_OPTION, Integer.toString(3));
        options.put(Controller.TARGET_SSTABLE_SIZE_OPTION, "100MiB");
        options.put(Controller.MIN_SSTABLE_SIZE_OPTION, "10MiB");
        options.put(Controller.SSTABLE_GROWTH_OPTION, "0.333");
        Controller controller = Controller.fromOptions(cfs, options);
        // Check min size
        assertEquals(1, controller.getNumShards(Math.scalb(29, 20)));
        assertEquals(1, controller.getNumShards(Math.scalb(20, 20)));
        assertEquals(1, controller.getNumShards(Math.scalb(19, 20)));
        assertEquals(1, controller.getNumShards(5));
        assertEquals(1, controller.getNumShards(0));
    }

    @Test
    public void testGetNumShards_minSize_10MiB_b_20()
    {
        Map<String, String> options = new HashMap<>();
        options.put(Controller.BASE_SHARD_COUNT_OPTION, Integer.toString(20));
        options.put(Controller.TARGET_SSTABLE_SIZE_OPTION, "100MiB");
        options.put(Controller.MIN_SSTABLE_SIZE_OPTION, "10MiB");
        options.put(Controller.SSTABLE_GROWTH_OPTION, "0.333");
        Controller controller = Controller.fromOptions(cfs, options);
        // Check min size
        assertEquals(2, controller.getNumShards(Math.scalb(29, 20)));
        assertEquals(2, controller.getNumShards(Math.scalb(20, 20)));
        assertEquals(1, controller.getNumShards(Math.scalb(19, 20)));
        assertEquals(1, controller.getNumShards(5));
        assertEquals(1, controller.getNumShards(0));
    }

    @Test
    public void testGetNumShards_minSize_10MiB_b_8()
    {
        Map<String, String> options = new HashMap<>();
        options.put(Controller.BASE_SHARD_COUNT_OPTION, Integer.toString(8));
        options.put(Controller.TARGET_SSTABLE_SIZE_OPTION, "100MiB");
        options.put(Controller.MIN_SSTABLE_SIZE_OPTION, "10MiB");
        options.put(Controller.SSTABLE_GROWTH_OPTION, "0.333");
        Controller controller = Controller.fromOptions(cfs, options);
        // Check min size
        assertEquals(2, controller.getNumShards(Math.scalb(29, 20)));
        assertEquals(2, controller.getNumShards(Math.scalb(20, 20)));
        assertEquals(1, controller.getNumShards(Math.scalb(19, 20)));
        assertEquals(1, controller.getNumShards(5));
        assertEquals(1, controller.getNumShards(0));
    }

    @Test
    public void testGetNumShards_minSize_3MiB_b_20()
    {
        Map<String, String> options = new HashMap<>();
        options.put(Controller.BASE_SHARD_COUNT_OPTION, Integer.toString(20));
        options.put(Controller.TARGET_SSTABLE_SIZE_OPTION, "100MiB");
        options.put(Controller.MIN_SSTABLE_SIZE_OPTION, "3MiB");
        options.put(Controller.SSTABLE_GROWTH_OPTION, "0.333");
        Controller controller = Controller.fromOptions(cfs, options);
        // Check min size
        assertEquals(4, controller.getNumShards(Math.scalb(29, 20)));
        assertEquals(4, controller.getNumShards(Math.scalb(20, 20)));
        assertEquals(4, controller.getNumShards(Math.scalb(19, 20)));
        assertEquals(1, controller.getNumShards(5));
        assertEquals(1, controller.getNumShards(0));
    }

    static final int[] Ws = new int[] { 30, 2, 0, -6};

    @Test
    public void testFromOptions()
    {
        Map<String, String> options = new HashMap<>();
        addOptions(false, options);

        Controller controller = testFromOptions(options);

        for (int i = 0; i < Ws.length; i++)
            assertEquals(Ws[i], controller.getScalingParameter(i));

        assertEquals(Ws[Ws.length-1], controller.getScalingParameter(Ws.length));
    }

    @Test
    public void testFromOptionsIntegers()
    {
        Map<String, String> options = new HashMap<>();
        addOptions(true, options);

        Controller controller = testFromOptions(options);

        for (int i = 0; i < Ws.length; i++)
            assertEquals(Ws[i], controller.getScalingParameter(i));

        assertEquals(Ws[Ws.length-1], controller.getScalingParameter(Ws.length));
    }

    @Test
    public void testMaxSSTablesToCompact()
    {
        Map<String, String> options = new HashMap<>();
        Controller controller = testFromOptions(options);
        assertTrue(controller.maxSSTablesToCompact == Integer.MAX_VALUE);

        options.put(Controller.MAX_SSTABLES_TO_COMPACT_OPTION, "100");
        controller = testFromOptions(options);
        assertEquals(100, controller.maxSSTablesToCompact);
    }

    @Test
    public void testExpiredSSTableCheckFrequency()
    {
        Map<String, String> options = new HashMap<>();

        Controller controller = testFromOptions(options);
        assertEquals(TimeUnit.MILLISECONDS.convert(Controller.DEFAULT_EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS, TimeUnit.SECONDS),
                     controller.getExpiredSSTableCheckFrequency());

        options.put(Controller.EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS_OPTION, "5");
        controller = testFromOptions(options);
        assertEquals(5000L, controller.getExpiredSSTableCheckFrequency());

        try
        {
            options.put(Controller.EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS_OPTION, "0");
            testFromOptions(options);
            fail("Exception should be thrown");
        }
        catch (ConfigurationException e)
        {
            // valid path
        }
    }

    @Test
    public void testAllowOverlaps()
    {
        Map<String, String> options = new HashMap<>();

        Controller controller = testFromOptions(options);
        assertEquals(Controller.DEFAULT_ALLOW_UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION, controller.getIgnoreOverlapsInExpirationCheck());

        options.put(Controller.ALLOW_UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION_OPTION, "true");
        controller = testFromOptions(options);
        assertEquals(Controller.ALLOW_UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION, controller.getIgnoreOverlapsInExpirationCheck());
    }

    @Test
    public void testBaseShardCountDefault()
    {
        Map<String, String> options = new HashMap<>();
        Controller controller = Controller.fromOptions(cfs, options);
        assertEquals(Controller.DEFAULT_BASE_SHARD_COUNT, controller.baseShardCount);

        PartitionPosition min = Util.testPartitioner().getMinimumToken().minKeyBound();
        diskBoundaries = new DiskBoundaries(cfs, null, ImmutableList.of(min, min, min), 0, 0);
        controller = Controller.fromOptions(cfs, options);
        assertEquals(4, controller.baseShardCount);

        diskBoundaries = new DiskBoundaries(cfs, null, ImmutableList.of(min), 0, 0);
        controller = Controller.fromOptions(cfs, options);
        assertEquals(Controller.DEFAULT_BASE_SHARD_COUNT, controller.baseShardCount);
    }

    @Test
    public void testMinSSTableSize()
    {
        Map<String, String> options = new HashMap<>();

        // verify 0 is acceptable
        options.put(Controller.MIN_SSTABLE_SIZE_OPTION, format("%sB", 0));
        Controller.validateOptions(options);

        // test min < 0 failes
        options.put(Controller.MIN_SSTABLE_SIZE_OPTION, "-1B");
        assertThatExceptionOfType(ConfigurationException.class)
        .describedAs("Should have thrown a ConfigurationException when min_sstable_size is less than 0")
        .isThrownBy(() -> Controller.validateOptions(options))
        .withMessageContaining("greater than or equal to 0");

        // test min < default target sstable size * INV_SQRT_2
        int limit = (int) Math.ceil(Controller.DEFAULT_TARGET_SSTABLE_SIZE * Controller.INVERSE_SQRT_2);
        options.put(Controller.MIN_SSTABLE_SIZE_OPTION, format("%sB", limit + 1));
        assertThatExceptionOfType(ConfigurationException.class)
        .describedAs("Should have thrown a ConfigurationException when min_sstable_size is greater than target_sstable_size")
        .isThrownBy(() -> Controller.validateOptions(options))
        .withMessageContaining(format("less than the target size minimum: %s", FBUtilities.prettyPrintMemory(limit)));

        // test min < configured target table size * INV_SQRT_2
        limit = (int) Math.ceil(Controller.MIN_TARGET_SSTABLE_SIZE * 2 * Controller.INVERSE_SQRT_2);
        options.put(Controller.MIN_SSTABLE_SIZE_OPTION, format("%sB", limit + 1));
        options.put(Controller.TARGET_SSTABLE_SIZE_OPTION, format("%sB", Controller.MIN_TARGET_SSTABLE_SIZE * 2));

        assertThatExceptionOfType(ConfigurationException.class)
        .describedAs("Should have thrown a ConfigurationException when min_sstable_size is greater than target_sstable_size")
        .isThrownBy(() -> Controller.validateOptions(options))
        .withMessageContaining(format("less than the target size minimum: %s", FBUtilities.prettyPrintMemory(limit)));
    }
}