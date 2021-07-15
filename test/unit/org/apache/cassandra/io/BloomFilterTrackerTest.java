package org.apache.cassandra.io;
/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */


import java.util.concurrent.TimeUnit;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.sstable.BloomFilterTracker;

import static org.junit.Assert.assertEquals;

public class BloomFilterTrackerTest
{
    @BeforeClass
    public static void setUp()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testAddingFalsePositives() throws InterruptedException
    {
        BloomFilterTracker bft = BloomFilterTracker.createMeterTracker();
        assertEquals(0L, bft.getFalsePositiveCount());
        assertEquals(0d, bft.getRecentFalsePositiveRate(), 0.0);
        bft.addFalsePositive();
        bft.addFalsePositive();
        Thread.sleep(TimeUnit.SECONDS.toMillis(5L)); // wait for tick that updates rates
        assertEquals(2L, bft.getFalsePositiveCount());
        assertEquals(0.4d, bft.getRecentFalsePositiveRate(), 0.0);
        assertEquals(0.4d, bft.getRecentFalsePositiveRate(), 0.0);
        assertEquals(2L, bft.getFalsePositiveCount()); // sanity check
    }

    @Test
    public void testAddingTruePositives()  throws InterruptedException
    {
        BloomFilterTracker bft = BloomFilterTracker.createMeterTracker();
        assertEquals(0L, bft.getTruePositiveCount());
        assertEquals(0d, bft.getRecentTruePositiveRate(), 0.0);
        bft.addTruePositive();
        bft.addTruePositive();
        Thread.sleep(TimeUnit.SECONDS.toMillis(5L)); // wait for tick that updates rates
        assertEquals(2L, bft.getTruePositiveCount());
        assertEquals(0.4d, bft.getRecentTruePositiveRate(), 0.0);
        assertEquals(0.4d, bft.getRecentTruePositiveRate(), 0.0);
        assertEquals(2L, bft.getTruePositiveCount()); // sanity check
    }

    @Test
    public void testAddingToOneLeavesTheOtherAlone() throws InterruptedException
    {
        BloomFilterTracker bft = BloomFilterTracker.createMeterTracker();
        bft.addFalsePositive();
        assertEquals(0L, bft.getTruePositiveCount());
        assertEquals(0d, bft.getRecentTruePositiveRate(), 0.0);
        bft.addTruePositive();
        Thread.sleep(TimeUnit.SECONDS.toMillis(5L)); // wait for tick that updates rates
        assertEquals(1L, bft.getFalsePositiveCount());
        assertEquals(0.2d, bft.getRecentFalsePositiveRate(), 0.0);
    }
}
