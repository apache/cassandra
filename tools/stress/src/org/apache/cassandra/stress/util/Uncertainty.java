package org.apache.cassandra.stress.util;
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


import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

// TODO: do not assume normal distribution of measurements.
public class Uncertainty
{

    private int measurements;
    private double sumsquares;
    private double sum;
    private double stdev;
    private double mean;
    private double uncertainty;

    private CopyOnWriteArrayList<WaitForTargetUncertainty> waiting = new CopyOnWriteArrayList<>();

    private static final class WaitForTargetUncertainty
    {
        final double targetUncertainty;
        final int minMeasurements;
        final int maxMeasurements;
        final CountDownLatch latch = new CountDownLatch(1);

        private WaitForTargetUncertainty(double targetUncertainty, int minMeasurements, int maxMeasurements)
        {
            this.targetUncertainty = targetUncertainty;
            this.minMeasurements = minMeasurements;
            this.maxMeasurements = maxMeasurements;
        }

        void await() throws InterruptedException
        {
            latch.await();
        }

    }

    public void update(double value)
    {
        measurements++;
        sumsquares += value * value;
        sum += value;
        mean = sum / measurements;
        stdev = Math.sqrt((sumsquares / measurements) - (mean * mean));
        uncertainty = (stdev / Math.sqrt(measurements)) / mean;

        for (WaitForTargetUncertainty waiter : waiting)
        {
            if ((uncertainty < waiter.targetUncertainty && measurements >= waiter.minMeasurements) || (measurements >= waiter.maxMeasurements))
            {
                waiter.latch.countDown();
                // can safely remove as working over snapshot with COWArrayList
                waiting.remove(waiter);
            }
        }
    }

    public void await(double targetUncertainty, int minMeasurements, int maxMeasurements) throws InterruptedException
    {
        final WaitForTargetUncertainty wait = new WaitForTargetUncertainty(targetUncertainty, minMeasurements, maxMeasurements);
        waiting.add(wait);
        wait.await();
    }

    public double getUncertainty()
    {
        return uncertainty;
    }

    public void wakeAll()
    {
        for (WaitForTargetUncertainty waiting : this.waiting)
        {
            waiting.latch.countDown();
            this.waiting.remove(waiting);
        }
    }

}
