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

package org.apache.cassandra.utils.concurrent;

import java.util.function.Supplier;

import org.junit.Assert;
import org.junit.Test;

public class CountDownLatchTest extends AbstractTestAwaitable<CountDownLatch>
{
    @Test
    public void testAsync()
    {
        testOne(() -> new CountDownLatch.Async(1));
        testNone(() -> new CountDownLatch.Async(0));
    }

    @Test
    public void testSync()
    {
        testOne(() -> new CountDownLatch.Sync(1));
        testNone(() -> new CountDownLatch.Sync(0));
    }

    void testOne(Supplier<CountDownLatch> cs)
    {
        CountDownLatch c = cs.get();
        testOneTimeout(c);
        Assert.assertEquals(1, c.count());

        testOneInterrupt(c);
        Assert.assertEquals(1, c.count());

        testOneSuccess(c, CountDownLatch::decrement);
        Assert.assertEquals(0, c.count());
    }

    void testNone(Supplier<CountDownLatch> cs)
    {
        CountDownLatch c = cs.get();
        Assert.assertEquals(0, c.count());
        testOneSuccess(c, ignore -> {});
    }
}
