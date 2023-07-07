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
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.schema.CachingParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FixedMonotonicClock;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.when;

public class AdaptiveControllerTest extends ControllerTest
{
    private CostsCalculator calculator;
    private FixedMonotonicClock clock;

    private final int minW = -10;
    private final int maxW = 64;
    private final int[] Ws = {0};
    private final int[] previousWs = {0};
    private final int interval = 60;
    private final int minCost = 5;
    private final int maxAdaptiveCompactions = 2;
    private final double baseCost = minCost * 5;
    private final double threshold = 0.15;

    @Before
    public void setup()
    {
        calculator = Mockito.mock(CostsCalculator.class);
        clock = new FixedMonotonicClock();
    }

    private AdaptiveController makeController()
    {
        return makeController(dataSizeGB, numShards, sstableSizeMB, 0);
    }

    private AdaptiveController makeController(long dataSizeGB, int numShards, long sstableSizeMB, long minSSTableSizeMB)
    {
        return new AdaptiveController(clock,
                                      env,
                                      Ws,
                                      previousWs,
                                      Controller.DEFAULT_SURVIVAL_FACTORS,
                                      dataSizeGB << 30,
                                      minSSTableSizeMB << 20,
                                      0,
                                      0,
                                      Controller.DEFAULT_MAX_SPACE_OVERHEAD,
                                      0,
                                      Controller.DEFAULT_EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS,
                                      Controller.DEFAULT_ALLOW_UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION,
                                      numShards,
                                      sstableSizeMB << 20,
                                      Controller.DEFAULT_SSTABLE_GROWTH,
                                      Controller.DEFAULT_RESERVED_THREADS,
                                      Controller.DEFAULT_RESERVED_THREADS_TYPE,
                                      Controller.DEFAULT_OVERLAP_INCLUSION_METHOD,
                                      interval,
                                      minW,
                                      maxW,
                                      threshold,
                                      minCost,
                                      maxAdaptiveCompactions,
                                      keyspaceName,
                                      tableName);
    }

    @Test
    public void testFromOptions()
    {
        Map<String, String> options = new HashMap<>();
        options.put(AdaptiveController.MIN_SCALING_PARAMETER, "-10");
        options.put(AdaptiveController.MAX_SCALING_PARAMETER, "32");
        options.put(AdaptiveController.INTERVAL_SEC, "120");
        options.put(AdaptiveController.THRESHOLD, "0.15");
        options.put(AdaptiveController.MIN_COST, "5");
        options.put(AdaptiveController.MAX_ADAPTIVE_COMPACTIONS, "-1");
        options.put(Controller.SCALING_PARAMETERS_OPTION, "T5");

        int[] scalingParameters = new int[30];
        Arrays.fill(scalingParameters, 1);
        AdaptiveController.storeOptions(keyspaceName, tableName, scalingParameters, 10 << 20);

        Controller controller = testFromOptions(true, options);
        assertTrue(controller instanceof AdaptiveController);

        for (int i = 0; i < 10; i++)
        {
            assertEquals(1, controller.getScalingParameter(i));
            assertEquals(1, controller.getPreviousScalingParameter(i));
        }
        int[] emptyScalingParameters = {};
        AdaptiveController.storeOptions(keyspaceName, tableName, emptyScalingParameters, 10 << 20);

        Controller controller2 = testFromOptions(true, options);
        assertTrue(controller2 instanceof AdaptiveController);

        for (int i = 0; i < 10; i++)
        {
            assertEquals(3, controller2.getScalingParameter(i));
            assertEquals(3, controller2.getPreviousScalingParameter(i));
        }
        AdaptiveController.getControllerConfigPath(keyspaceName, tableName).delete();

        Map<String, String> options2 = new HashMap<>();
        options2.put(AdaptiveController.MIN_SCALING_PARAMETER, "-10");
        options2.put(AdaptiveController.MAX_SCALING_PARAMETER, "32");
        options2.put(AdaptiveController.INTERVAL_SEC, "120");
        options2.put(AdaptiveController.THRESHOLD, "0.15");
        options2.put(AdaptiveController.MIN_COST, "5");
        options2.put(AdaptiveController.MAX_ADAPTIVE_COMPACTIONS, "-1");
        options2.put(Controller.SCALING_PARAMETERS_OPTION, "L5");
        Controller controller3 = testFromOptions(true, options2);
        assertTrue(controller3 instanceof AdaptiveController);

        for (int i = 0; i < 10; i++)
        {
            assertEquals(-3, controller3.getScalingParameter(i));
            assertEquals(-3, controller3.getPreviousScalingParameter(i));
        }

        Map<String, String> options3 = new HashMap<>();
        options3.put(AdaptiveController.MIN_SCALING_PARAMETER, "-10");
        options3.put(AdaptiveController.MAX_SCALING_PARAMETER, "32");
        options3.put(AdaptiveController.INTERVAL_SEC, "120");
        options3.put(AdaptiveController.THRESHOLD, "0.15");
        options3.put(AdaptiveController.MIN_COST, "5");
        options3.put(AdaptiveController.MAX_ADAPTIVE_COMPACTIONS, "-1");
        options3.put(Controller.STATIC_SCALING_FACTORS_OPTION, "4");
        Controller controller4 = testFromOptions(true, options3);
        assertTrue(controller4 instanceof AdaptiveController);

        for (int i = 0; i < 10; i++)
        {
            assertEquals(4, controller4.getScalingParameter(i));
            assertEquals(4, controller4.getPreviousScalingParameter(i));
        }
    }

    @Test
    public void testValidateOptions()
    {
        Map<String, String> options = new HashMap<>();
        options.put(AdaptiveController.MIN_SCALING_PARAMETER, "-10");
        options.put(AdaptiveController.MAX_SCALING_PARAMETER, "32");
        options.put(AdaptiveController.INTERVAL_SEC, "120");
        options.put(AdaptiveController.THRESHOLD, "0.15");
        options.put(AdaptiveController.MIN_COST, "5");
        options.put(AdaptiveController.MAX_ADAPTIVE_COMPACTIONS, "-1");

        super.testValidateOptions(options, true);

        Map<String, String> options2 = new HashMap<>();
        options2.put(AdaptiveController.MIN_SCALING_PARAMETER, "-10");
        options2.put(AdaptiveController.MAX_SCALING_PARAMETER, "32");
        options2.put(AdaptiveController.INTERVAL_SEC, "120");
        options2.put(AdaptiveController.THRESHOLD, "0.15");
        options2.put(AdaptiveController.MIN_COST, "5");
        options2.put(AdaptiveController.MAX_ADAPTIVE_COMPACTIONS, "-1");
        options2.put(Controller.STATIC_SCALING_FACTORS_OPTION, "1,2,3");

        super.testValidateOptions(options2, true);

        Map<String, String> options3 = new HashMap<>();
        options3.put(AdaptiveController.MIN_SCALING_PARAMETER, "-10");
        options3.put(AdaptiveController.MAX_SCALING_PARAMETER, "32");
        options3.put(AdaptiveController.INTERVAL_SEC, "120");
        options3.put(AdaptiveController.THRESHOLD, "0.15");
        options3.put(AdaptiveController.MIN_COST, "5");
        options3.put(AdaptiveController.MAX_ADAPTIVE_COMPACTIONS, "-1");
        options3.put(Controller.SCALING_PARAMETERS_OPTION, "1,2,3");

        super.testValidateOptions(options3, true);
    }

    @Test
    public void testValidateCompactionStrategyOptions()
    {
        super.testValidateCompactionStrategyOptions(true);
    }

    @Test
    public void testStartShutdown()
    {
        AdaptiveController controller = makeController();
        testStartShutdown(controller);
    }

    @Test
    public void testShutdownNotStarted()
    {
        AdaptiveController controller = makeController();
        testShutdownNotStarted(controller);
    }

    @Test(expected = IllegalStateException.class)
    public void testStartAlreadyStarted()
    {
        AdaptiveController controller = makeController();
        testStartAlreadyStarted(controller);
    }

    @Test
    public void testMinSSTableSizeDynamic()
    {
        // <= 50 MB, round up to 50 MB
        testMinSSTableSizeDynamic(1, 50);
        testMinSSTableSizeDynamic((50 << 20) - 1, 50);
        testMinSSTableSizeDynamic(50 << 20, 50);

        // <= 100 MB, round up to 100 MB
        testMinSSTableSizeDynamic((50 << 20) + 1, 100);
        testMinSSTableSizeDynamic((100 << 20) - 1, 100);
        testMinSSTableSizeDynamic(100 << 20, 100);

        // no flush size, 50 MB, then flush size of 100 MB + 1 returns 150MB
        testMinSSTableSizeDynamic(0, 50, (100 << 20) + 1, 150);
    }

    private void testMinSSTableSizeDynamic(long flushSizeBytes1, int minSSTableSizeMB1)
    {
        // The most common case, the second calculation is skipped so even if the env returns zero the second time, the result won't change
        testMinSSTableSizeDynamic(flushSizeBytes1, minSSTableSizeMB1, 0, minSSTableSizeMB1);
    }

    private void testMinSSTableSizeDynamic(long flushSizeBytes1, int minSSTableSizeMB1, long flushSizeBytes2, int minSSTableSizeMB2)
    {
        // create a controller with minSSTableSizeMB set to zero so that it will calculate the min sstable size from the flush size
        AdaptiveController controller = makeController(dataSizeGB, numShards, Integer.MAX_VALUE, -1);

        when(env.flushSize()).thenReturn(flushSizeBytes1 * 1.0);
        assertEquals(minSSTableSizeMB1 << 20, controller.getMinSstableSizeBytes());

        when(env.flushSize()).thenReturn(flushSizeBytes2 * 1.0);
        assertEquals(minSSTableSizeMB2 << 20, controller.getMinSstableSizeBytes());
    }


    @Test
    public void testUpdateNotEnoughTimeElapsed()
    {
        AdaptiveController controller = makeController();
        controller.startup(strategy, calculator);

        // no update, not enough time elapsed
        controller.onStrategyBackgroundTaskRequest();
        assertEquals(Ws[0], controller.getScalingParameter(0));
    }

    @Test
    public void testUpdateBelowMinCost() throws InterruptedException
    {
        AdaptiveController controller = makeController();
        controller.startup(strategy, calculator);

        // no update, <= min cost
        when(calculator.getReadCostForQueries(anyInt())).thenReturn((double) minCost);
        when(calculator.getReadCostForQueries(anyInt())).thenReturn(0.);
        when(calculator.spaceUsed()).thenReturn(1.0);

        clock.setNowInNanos(clock.now() + TimeUnit.SECONDS.toNanos(interval + 1));
        controller.onStrategyBackgroundTaskRequest();
        assertEquals(Ws[0], controller.getScalingParameter(0));
    }

    @Test
    public void testUpdateWithSize_min() throws InterruptedException
    {
        long totSize = (long) sstableSizeMB << 20;
        testUpdateWithSize(totSize, new double[]{ baseCost, 0, baseCost }, new double[]{ 0, baseCost, baseCost }, new int[]{ 0, 0, 0 });
    }

    @Test
    public void testUpdateWithSize_1GB() throws InterruptedException
    {
        long totSize = 1L << 31;
        testUpdateWithSize(totSize, new double[]{ baseCost, 0, baseCost }, new double[]{ 0, baseCost, baseCost }, new int[]{ -9, 31, 1 });
    }

    @Test
    public void testUpdateWithSize_2GB() throws InterruptedException
    {
        long totSize = 2L << 31;
        testUpdateWithSize(totSize, new double[]{ baseCost, 0, baseCost }, new double[]{ 0, baseCost, baseCost }, new int[]{ -5, 44, 1 } );
    }

    @Test
    public void testUpdateWithSize_128GB() throws InterruptedException
    {
        long totSize = 1L << 37;
        testUpdateWithSize(totSize, new double[] {baseCost, 0, baseCost}, new double[] {0, baseCost, baseCost}, new int[] {-8, 39, 1});
    }

    @Test
    public void testUpdateWithSize_512GB() throws InterruptedException
    {
        long totSize = 1L << 39;
        testUpdateWithSize(totSize, new double[] {baseCost, 0, baseCost}, new double[] {0, baseCost, baseCost}, new int[] {-7, 63, 1});
    }

    @Test
    public void testUpdateWithSize_1TB() throws InterruptedException
    {
        long totSize = 1L << 40;
        testUpdateWithSize(totSize, new double[] {baseCost, 0, baseCost}, new double[] {0, baseCost, baseCost}, new int[] {-7, 25, 1});
    }

    @Test
    public void testUpdateWithSize_5TB() throws InterruptedException
    {
        long totSize = 5 * (1L << 40);
        testUpdateWithSize(totSize, new double[] {baseCost, 0, baseCost}, new double[] {0, baseCost, baseCost}, new int[] {-10, 39, 1});
    }

    @Test
    public void testUpdateWithSize_10TB() throws InterruptedException
    {
        long totSize = 10 * (1L << 40);
        testUpdateWithSize(totSize, new double[] { baseCost, 0, baseCost}, new double[] { 0, baseCost, baseCost}, new int[] { -8, 46, 1});
    }

    @Test
    public void testUpdateWithSize_20TB() throws InterruptedException
    {
        long totSize = 20 * (1L << 49);
        testUpdateWithSize(totSize, new double[] { baseCost, 0, baseCost}, new double[] { 0, baseCost, baseCost}, new int[] { -8, 40, 1});
    }

    private void testUpdateWithSize(long totSize, double[] readCosts, double[] writeCosts, int[] expectedWs) throws InterruptedException
    {
        int shardSizeGB = (int) (totSize >> 30);
        AdaptiveController controller = makeController(shardSizeGB, 1, sstableSizeMB, 0); // one unique shard
        controller.startup(strategy, calculator);

        assertEquals(readCosts.length, writeCosts.length);
        assertEquals(writeCosts.length, expectedWs.length);

        when(calculator.spaceUsed()).thenReturn((double) totSize);

        for (int i = 0; i < readCosts.length; i++)
        {
            final double readCost = readCosts[i];
            final double writeCost = writeCosts[i];

            when(calculator.getReadCostForQueries(anyInt())).thenAnswer(answ -> (int) answ.getArgument(0) * readCost);
            when(calculator.getWriteCostForQueries(anyInt())).thenAnswer(answ -> (int) answ.getArgument(0) * writeCost);

            clock.setNowInNanos(clock.now() + TimeUnit.SECONDS.toNanos(interval + 1));

            controller.onStrategyBackgroundTaskRequest();
            assertEquals(expectedWs[i], controller.getScalingParameter(0));
        }
    }

    @Test
    public void testMetrics()
    {
        TableMetadata metadata = TableMetadata.builder("ks", "table")
                                              .partitioner(Murmur3Partitioner.instance)
                                              .addPartitionKeyColumn("key", UTF8Type.instance)
                                              .addClusteringColumn("col", UTF8Type.instance)
                                              .addRegularColumn("value", UTF8Type.instance)
                                              .caching(CachingParams.CACHE_NOTHING)
                                              .build();
        Controller.Metrics metrics = new Controller.Metrics(metadata);
        AdaptiveController controller = makeController();
        metrics.setController(controller);

        double wa = metrics.getMeasuredWA();
        double readIo = metrics.getReadIOCost();
        double writeIo = metrics.getWriteIOCost();
        double totalIo = metrics.getTotalIOCost();

        assertEquals(0, wa, 0);
        assertEquals(0, readIo, 0);
        assertEquals(0, writeIo, 0);
        assertEquals(0, totalIo, 0);
    }
}
