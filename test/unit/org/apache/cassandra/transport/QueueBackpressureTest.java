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

package org.apache.cassandra.transport;

import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

public class QueueBackpressureTest
{
    @Test
    public void testQueueBackpressure()
    {
        QueueBackpressure.Impl backpressure = new QueueBackpressure.Impl(() -> TimeUnit.MILLISECONDS.toNanos(10),
                                                                         () -> TimeUnit.MILLISECONDS.toNanos(100),
                                                                         -1, 0, 0);

        Assert.assertEquals(0, backpressure.delay(TimeUnit.NANOSECONDS));
        backpressure = backpressure.mark(backpressure.appliedAt() + TimeUnit.MILLISECONDS.toNanos(100));
        Assert.assertEquals(backpressure.minDelayNanos(), backpressure.delay(TimeUnit.NANOSECONDS));
        // up to 9 times
        for (int i = 1; i < 10; i++)
            backpressure = backpressure.mark(backpressure.appliedAt() + TimeUnit.MILLISECONDS.toNanos(100));
        Assert.assertEquals(backpressure.minDelayNanos(), backpressure.delay(TimeUnit.NANOSECONDS));

        // 10th time
        backpressure = backpressure.mark(backpressure.appliedAt() + TimeUnit.MILLISECONDS.toNanos(100));
        Assert.assertEquals(backpressure.minDelayNanos() * 2, backpressure.delay(TimeUnit.NANOSECONDS));

        for (int i = 0; i < 40; i++)
            backpressure = backpressure.mark(backpressure.appliedAt() + TimeUnit.MILLISECONDS.toNanos(100));

        Assert.assertEquals(backpressure.minDelayNanos() * 6, backpressure.delay(TimeUnit.NANOSECONDS));

        for (int i = 0; i < 1000; i++)
            backpressure = backpressure.mark(backpressure.appliedAt() + TimeUnit.MILLISECONDS.toNanos(100));

        Assert.assertEquals(backpressure.maxDelayNanos(), backpressure.delay(TimeUnit.NANOSECONDS));

        backpressure = backpressure.mark(backpressure.appliedAt() + TimeUnit.MILLISECONDS.toNanos(1001));
        Assert.assertEquals(backpressure.minDelayNanos(), backpressure.delay(TimeUnit.NANOSECONDS));
    }
}
