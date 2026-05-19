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

package org.apache.cassandra.distributed.test.accord.load;

import java.util.Arrays;

import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.distributed.shared.DistributedTestBase;

import static org.apache.cassandra.distributed.test.accord.load.LoadSettings.ycsbZipfian;

public class AccordYcsbLoadTest extends AccordLoadTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(AccordYcsbLoadTest.class);

    private static final int[][] LATENCIES = new int[][] {
        new int[] {  0, 44, 64, 43, 84 },
        new int[] { 44,  0, 30,  3, 45 },
        new int[] { 64, 30,  0, 28, 37 },
        new int[] { 43,  3, 28,  0, 49 },
        new int[] { 84, 45, 37, 49,  0 }
    };

    private static LoadSettings.Builder withArtificialLatencies(LoadSettings.Builder builder)
    {
        return builder.setArtificialLatencies(LATENCIES);
    }

    private static LoadSettings.Builder ycsbA(LoadSettings.Builder builder, int keyCount)
    {
        return builder.setKeySelector(ycsbZipfian(keyCount))
                      .setReadRatio(0.5f);
    }

    private static LoadSettings.Builder ycsbB(LoadSettings.Builder builder, int keyCount)
    {
        return builder.setKeySelector(ycsbZipfian(keyCount))
                      .setReadRatio(0.95f);
    }

    private static LoadSettings.Builder ycsbC(LoadSettings.Builder builder, int keyCount)
    {
        return builder.setKeySelector(ycsbZipfian(keyCount))
                      .setReadRatio(1.0f);

    }

    @Override
    protected Logger logger()
    {
        return logger;
    }

    private static void computeWorstLatencies()
    {
        int[] qs = new int[LATENCIES.length];
        for (int i = 0 ; i < qs.length ; ++i)
        {
            int[] copy = LATENCIES[i].clone();
            Arrays.sort(copy);
            qs[i] = copy[copy.length/2];
        }
        int[] ws = new int[qs.length];
        for (int i = 0 ; i < qs.length ; ++i)
        {
            int iw = Integer.MIN_VALUE;
            for (int j = 0; j < qs.length ; ++j)
                iw = Math.max(iw, qs[i] + 3*qs[j] + LATENCIES[i][j]);
            ws[i] = iw;
        }
        System.out.println(Arrays.toString(ws));
        Arrays.fill(ws, 0);
        for (int i = 0 ; i < qs.length ; ++i)
        {
            for (int j = 0 ; j < qs.length ; ++j)
            {
                if (j == i) continue;
                if (qs[j] > 2*qs[i]) continue;
                int w = qs[i] + 4*qs[j] + LATENCIES[i][j];
                if (w > ws[i])
                    ws[i] = w;
            }
        }
        System.out.println(Arrays.toString(ws));
    }

    @Ignore
    @Test
    public void testLoad() throws Exception
    {
        testLoad(ycsbA(new LoadSettings.Builder(), 100_000)
                 .setRatePerSecond(1600).setMinRatePerSecond(200)
                 .setIncreaseRatePerSecondInterval(5000)
                 .build());
    }

    public static void main(String[] args) throws Throwable
    {
        computeWorstLatencies();

        DistributedTestBase.beforeClass();
        AccordYcsbLoadTest test = new AccordYcsbLoadTest();
        try
        {
            test.setupCluster();
            test.setup();
            test.testLoad(withArtificialLatencies(ycsbA(new LoadSettings.Builder(), 100_000)
                                                  .setRatePerSecond(1600).setMinRatePerSecond(200)
                                                  .setIncreaseRatePerSecondInterval(5000)
            ).build());
        }
        finally
        {
            test.tearDown();
        }
    }
}
