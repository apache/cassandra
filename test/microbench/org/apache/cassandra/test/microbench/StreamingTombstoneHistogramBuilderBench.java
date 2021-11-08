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

package org.apache.cassandra.test.microbench;


import java.util.concurrent.TimeUnit;
import java.util.Random;

import org.apache.cassandra.utils.streamhist.StreamingTombstoneHistogramBuilder;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.profile.*;
import org.openjdk.jmh.runner.*;
import org.openjdk.jmh.runner.options.*;

import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1)
@Threads(1)
@State(Scope.Benchmark)
public class StreamingTombstoneHistogramBuilderBench
{

    static int[] secondInMonth = new int[10000000];
    static int[] secondInDay = new int[10000000];
    static int[] secondIn3Hour = new int[10000000];
    static int[] secondInMin = new int[10000000];

    static
    {
        final int now = (int) (currentTimeMillis() / 1000L);
        Random random = new Random();
        for(int i = 0 ; i < 10000000; i++)
        {
            // Seconds in a month
            secondInMonth[i] = now + random.nextInt(3600 * 24 * 30);
            // Seconds in a day
            secondInDay[i] = now + random.nextInt(3600 * 24);
            // Seconds in 3 hours
            secondIn3Hour[i] = now + random.nextInt(3600 * 3);
            // Seconds in a minute
            secondInMin[i] = now + random.nextInt(60);
        }
    }

    @Param({ "secondInMonth", "secondInDay", "secondIn3Hour", "secondInMin" })
    String a_workLoad;

    @Param({ "0", "1000", "10000", "100000" })
    int b_spoolSize;

    @Benchmark
    public void test()
    {
        StreamingTombstoneHistogramBuilder histogram = new StreamingTombstoneHistogramBuilder(100, b_spoolSize, 1);
        int[] data = selectWorkload(a_workLoad);

        for (int time : data)
        {
            histogram.update(time);
        }

        histogram.flushHistogram();
    }

    private int[] selectWorkload(String workLoad)
    {
        switch (workLoad)
        {
            case "secondInMonth":
                return secondInMonth;
            case "secondInDay":
                return secondInDay;
            case "secondIn3Hour":
                return secondIn3Hour;
            case "secondInMin":
                return secondInMin;
            default:
                throw new IllegalArgumentException("Invalid workload type: " + workLoad);
        }
    }


    public static void main(String[] args) throws Exception
    {
        Options opt = new OptionsBuilder()
                      .include(StreamingTombstoneHistogramBuilderBench.class.getSimpleName())
                      .warmupIterations(3)
                      .measurementIterations(10)
                      .addProfiler(GCProfiler.class)
                      .threads(1)
                      .forks(1)
                      .build();
        new Runner(opt).run();
    }
}
