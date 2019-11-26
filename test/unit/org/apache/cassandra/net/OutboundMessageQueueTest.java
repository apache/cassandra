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

package org.apache.cassandra.net;

import java.util.concurrent.CountDownLatch;

import com.google.common.util.concurrent.Uninterruptibles;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.OutboundMessageQueue;
import org.apache.cassandra.net.Verb;

import static org.apache.cassandra.net.NoPayload.noPayload;

// TODO: incomplete
public class OutboundMessageQueueTest
{

    @BeforeClass
    public static void init()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testRemove() throws InterruptedException
    {
        final Message<?> m1 = Message.out(Verb._TEST_1, noPayload);
        final Message<?> m2 = Message.out(Verb._TEST_1, noPayload);
        final Message<?> m3 = Message.out(Verb._TEST_1, noPayload);

        final OutboundMessageQueue queue = new OutboundMessageQueue(message -> true);
        queue.add(m1);
        queue.add(m2);
        queue.add(m3);

        Assert.assertTrue(queue.remove(m1));
        Assert.assertFalse(queue.remove(m1));

        CountDownLatch locked = new CountDownLatch(1);
        CountDownLatch lockUntil = new CountDownLatch(1);
        new Thread(() -> {
            try (OutboundMessageQueue.WithLock lock = queue.lockOrCallback(0, () -> {}))
            {
                locked.countDown();
                Uninterruptibles.awaitUninterruptibly(lockUntil);
            }
        }).start();
        Uninterruptibles.awaitUninterruptibly(locked);

        CountDownLatch start = new CountDownLatch(2);
        CountDownLatch finish = new CountDownLatch(2);
        new Thread(() -> {
            start.countDown();
            Assert.assertTrue(queue.remove(m2));
            finish.countDown();
        }).start();
        new Thread(() -> {
            start.countDown();
            Assert.assertTrue(queue.remove(m3));
            finish.countDown();
        }).start();
        Uninterruptibles.awaitUninterruptibly(start);
        lockUntil.countDown();
        Uninterruptibles.awaitUninterruptibly(finish);

        try (OutboundMessageQueue.WithLock lock = queue.lockOrCallback(0, () -> {}))
        {
            Assert.assertNull(lock.peek());
        }
    }

}
