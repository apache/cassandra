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

import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.UnifiedCompactionStrategy;
import org.apache.cassandra.schema.TableMetadata;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static junit.framework.TestCase.assertNull;
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
    static final int dataSizeGB = 512;
    static final int numShards = 4; // pick it so that dataSizeGB is exactly divisible or tests will break
    static final int sstableSizeMB = 2;
    static final double maxSpaceOverhead = 0.3d;
    static final boolean allowOverlaps = false;
    static final long checkFrequency= 600L;

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

        when(executorService.scheduleAtFixedRate(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class))).thenReturn(fut);

        when(env.flushSize()).thenReturn((double) (sstableSizeMB << 20));
    }

    Controller testFromOptions(boolean adaptive, Map<String, String> options)
    {
        options.putIfAbsent(Controller.ADAPTIVE_OPTION, Boolean.toString(adaptive));
        options.putIfAbsent(Controller.MIN_SSTABLE_SIZE_OPTION_MB, Integer.toString(sstableSizeMB));

        options.putIfAbsent(Controller.DATASET_SIZE_OPTION_GB, Integer.toString(dataSizeGB));
        options.putIfAbsent(Controller.NUM_SHARDS_OPTION, Integer.toString(numShards));
        options.putIfAbsent(Controller.MAX_SPACE_OVERHEAD_OPTION, Double.toString(maxSpaceOverhead));
        options.putIfAbsent(Controller.ALLOW_UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION_OPTION, Boolean.toString(allowOverlaps));
        options.putIfAbsent(Controller.EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS_OPTION, Long.toString(checkFrequency));

        Controller.validateOptions(options);

        Controller controller = Controller.fromOptions(cfs, options);
        assertNotNull(controller);
        assertNotNull(controller.toString());

        assertEquals((long) sstableSizeMB << 20, controller.getMinSstableSizeBytes());
        assertEquals((long) dataSizeGB << 30, controller.getDataSetSizeBytes());
        assertEquals(numShards, controller.getNumShards());
        assertEquals(((long) dataSizeGB << 30) / numShards, controller.getShardSizeBytes());
        assertFalse(controller.isRunning());
        assertEquals(Controller.DEFAULT_SURVIVAL_FACTOR, controller.getSurvivalFactor(), epsilon);
        assertNull(controller.getCalculator());

        return controller;
    }

    void testValidateOptions(Map<String, String> options, boolean adaptive)
    {
        options.putIfAbsent(Controller.ADAPTIVE_OPTION, Boolean.toString(adaptive));
        options.putIfAbsent(Controller.MIN_SSTABLE_SIZE_OPTION_MB, Integer.toString(sstableSizeMB));

        options.putIfAbsent(Controller.DATASET_SIZE_OPTION_GB, Integer.toString(dataSizeGB));
        options.putIfAbsent(Controller.NUM_SHARDS_OPTION, Integer.toString(numShards));
        options.putIfAbsent(Controller.MAX_SPACE_OVERHEAD_OPTION, Double.toString(maxSpaceOverhead));

        options.putIfAbsent(Controller.ALLOW_UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION_OPTION, Boolean.toString(allowOverlaps));
        options.putIfAbsent(Controller.EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS_OPTION, Long.toString(checkFrequency));

        options = Controller.validateOptions(options);
        assertTrue(options.toString(), options.isEmpty());
    }

    void testStartShutdown(Controller controller)
    {
        assertNotNull(controller);

        assertEquals((long) dataSizeGB << 30, controller.getDataSetSizeBytes());
        assertEquals(numShards, controller.getNumShards());
        assertEquals(((long) dataSizeGB << 30) / numShards, controller.getShardSizeBytes());
        assertEquals((long) sstableSizeMB << 20, controller.getMinSstableSizeBytes());
        assertFalse(controller.isRunning());
        assertEquals(Controller.DEFAULT_SURVIVAL_FACTOR, controller.getSurvivalFactor(), epsilon);
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
}