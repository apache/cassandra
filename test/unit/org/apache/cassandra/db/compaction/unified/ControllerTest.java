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
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Overlaps;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

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
    protected DiskBoundaries diskBoundaries = new DiskBoundaries(cfs, null, null, Epoch.FIRST, 0);

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
        assertEquals(2, controller.getNumShards(0));
        assertEquals(16, controller.getNumShards(16 * 100 << 20));
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
        options.putIfAbsent(Controller.OVERLAP_INCLUSION_METHOD_OPTION, Overlaps.InclusionMethod.SINGLE.toString().toLowerCase());
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

        String prevKS = keyspaceName;
        try
        {
            keyspaceName = SchemaConstants.SYSTEM_KEYSPACE_NAME;
            controller = controller.fromOptions(cfs, options);
            assertEquals(1, controller.baseShardCount);
        }
        finally
        {
            keyspaceName = prevKS;
        }

        PartitionPosition min = Util.testPartitioner().getMinimumToken().minKeyBound();
        diskBoundaries = new DiskBoundaries(cfs, null, ImmutableList.of(min, min, min), Epoch.FIRST, 0);
        controller = controller.fromOptions(cfs, options);
        assertEquals(1, controller.baseShardCount);

        diskBoundaries = new DiskBoundaries(cfs, null, ImmutableList.of(min), Epoch.FIRST, 0);
        controller = controller.fromOptions(cfs, options);
        assertEquals(Controller.DEFAULT_BASE_SHARD_COUNT, controller.baseShardCount);
    }
}