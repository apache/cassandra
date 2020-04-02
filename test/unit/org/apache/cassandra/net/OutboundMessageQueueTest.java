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

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;

import static org.apache.cassandra.net.NoPayload.noPayload;
import static org.apache.cassandra.utils.MonotonicClock.approxTime;

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

    @Test
    public void testExpiration() throws InterruptedException
    {
        List<Message> expiredMessages = new LinkedList<>();
        long startTime = approxTime.now();

        Message<?> m1 = Message.out(Verb._TEST_1, noPayload, startTime + TimeUnit.SECONDS.toNanos(7));
        Message<?> m2 = Message.out(Verb._TEST_1, noPayload, startTime + TimeUnit.SECONDS.toNanos(3));

        OutboundMessageQueue queue = new OutboundMessageQueue(m -> expiredMessages.add(m));
        queue.add(m1);
        queue.add(m2);

        try(OutboundMessageQueue.WithLock l = queue.lockOrCallback(approxTime.now(), () -> {}))
        {
            // Do nothing
        }
        // Check next expiry time is equal to m2, and we haven't expired anything yet:
        Assert.assertEquals(3, queue.nextExpirationIn(startTime, TimeUnit.SECONDS));
        Assert.assertTrue(expiredMessages.isEmpty());

        // Wait for m2 expiry time:
        Thread.sleep(TimeUnit.SECONDS.toMillis(3));

        try(OutboundMessageQueue.WithLock l = queue.lockOrCallback(approxTime.now(), () -> {}))
        {
            // Add a new message while we're iterating the queue:
            Message<?> m3 = Message.out(Verb._TEST_1, noPayload, startTime + TimeUnit.SECONDS.toNanos(60));
            queue.add(m3);
        }
        // After expiration runs following the WithLock#close(), check the expiration time is updated to m1:
        Assert.assertEquals(7, queue.nextExpirationIn(startTime, TimeUnit.SECONDS));
        // Also, m2 was expired and collected:
        Assert.assertEquals(m2, expiredMessages.remove(0));

        // Wait for m1 expiry time:
        Thread.sleep(TimeUnit.SECONDS.toMillis(4));

        try(OutboundMessageQueue.WithLock l = queue.lockOrCallback(approxTime.now(), () -> {}))
        {
            // Do nothing
        }
        // Check m1 was expired and collected:
        Assert.assertEquals(m1, expiredMessages.remove(0));
        // Check next expiry time is m3:
        Assert.assertEquals(60, queue.nextExpirationIn(startTime, TimeUnit.SECONDS));
    }
}
