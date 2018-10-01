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

package org.apache.cassandra.metrics;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ExponentialMovingAverageTest
{
    protected static final Logger logger = LoggerFactory.getLogger(ExponentialMovingAverageTest.class);

    @Test
    public void simpleEMATest() throws Exception
    {
        ExponentialMovingAverage avg = new ExponentialMovingAverage(0.5, 0);
        avg.update(1.0);
        assertEquals(0.5, avg.getAvg(), 0.01);
        avg.update(1.0);
        assertEquals(0.75, avg.getAvg(), 0.01);
        avg.update(1.0);
        assertEquals(0.875, avg.getAvg(), 0.01);

        avg.updateParameter(0.1, avg.getAvg());
    }

    @Test
    public void updateParameterTest() throws Exception
    {
        ExponentialMovingAverage avg = new ExponentialMovingAverage(0.25, 0);
        avg.update(0.1);
        avg.update(0.15);
        avg.update(0.45);
        avg.update(0.25);
        avg.update(0.1);
        assertEquals(avg.getAvg(), 0.158, 0.01);

        avg.updateParameter(0.5, avg.getAvg());
        avg.update(20);
        assertEquals(avg.getAvg(), 10.07, 0.01);
        avg.update(30);
        assertEquals(avg.getAvg(), 20.039, 0.01);
    }

    @Test
    public void testReset() throws Exception
    {
        ExponentialMovingAverage avg = new ExponentialMovingAverage(0.1, 0.0);
        for (int i = 0; i < 100; i++)
        {
            avg.update((double) i);
        }
        assertEquals(90, avg.getAvg(), 0.1);
        avg.reset();
        assertEquals(avg.getAvg(), 0.0, 0.0);
    }

    @Test
    public void testMultiThreading() throws Exception
    {
        ExecutorService executor = Executors.newFixedThreadPool(20);
        final ExponentialMovingAverage avg = new ExponentialMovingAverage(0.1, 0.0);

        assertEquals(avg.getAvg(), 0.0, 0.01);

        executor.submit(() -> {
            double value = (double) ThreadLocalRandom.current().nextInt(50, 100);
            for(int i = 0; i < 20000; i ++)
            {
                avg.update(value);
            }
        });
        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.SECONDS);
        logger.info("Multi threading upgrades lead to {}", avg.getAvg());

        assertTrue(avg.getAvg() >= 50.0 && avg.getAvg() <= 100.0);
    }
}