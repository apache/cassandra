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
import org.apache.cassandra.utils.FreeRunningClock;

import static org.apache.cassandra.net.NoPayload.noPayload;
import static org.apache.cassandra.utils.MonotonicClock.Global.approxTime;

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

        final OutboundMessageQueue queue = new OutboundMessageQueue(approxTime, message -> true);
        queue.add(m1);
        queue.add(m2);
        queue.add(m3);

        Assert.assertTrue(queue.remove(m1));
        Assert.assertFalse(queue.remove(m1));

        CountDownLatch locked = new CountDownLatch(1);
        CountDownLatch lockUntil = new CountDownLatch(1);
        CountDownLatch lockReleased = new CountDownLatch(1);
        new Thread(() -> {
            try (OutboundMessageQueue.WithLock lock = queue.lockOrCallback(0, () -> {}))
            {
                locked.countDown();
                Uninterruptibles.awaitUninterruptibly(lockUntil);
            }
            lockReleased.countDown();
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

        Uninterruptibles.awaitUninterruptibly(lockReleased);
        try (OutboundMessageQueue.WithLock lock = queue.lockOrCallback(0, () -> {}))
        {
            Assert.assertNull(lock.peek());
        }
    }

    @Test
    public void testExpirationOnIteration()
    {
        FreeRunningClock clock = new FreeRunningClock(approxTime.now());

        List<Message> expiredMessages = new LinkedList<>();
        long startTime = clock.now();

        Message<?> m1 = Message.out(Verb._TEST_1, noPayload, startTime + TimeUnit.SECONDS.toNanos(7));
        Message<?> m2 = Message.out(Verb._TEST_1, noPayload, startTime + TimeUnit.SECONDS.toNanos(3));
        Message<?> m3;
        Message<?> m4;

        OutboundMessageQueue queue = new OutboundMessageQueue(clock, m -> expiredMessages.add(m));
        queue.add(m1);
        queue.add(m2);

        try(OutboundMessageQueue.WithLock l = queue.lockOrCallback(clock.now(), () -> {}))
        {
            // Do nothing
        }
        // Check next expiry time is equal to m2, and we haven't expired anything yet:
        Assert.assertEquals(3, queue.nextExpirationIn(startTime, TimeUnit.SECONDS));
        Assert.assertTrue(expiredMessages.isEmpty());

        // Wait for m2 expiry time:
        clock.advance(4, TimeUnit.SECONDS);

        try(OutboundMessageQueue.WithLock l = queue.lockOrCallback(clock.now(), () -> {}))
        {
            // Add a new message while we're iterating the queue: this will expire later than any existing message.
            m3 = Message.out(Verb._TEST_1, noPayload, startTime + TimeUnit.SECONDS.toNanos(60));
            queue.add(m3);
        }
        // After expiration runs following the WithLock#close(), check the expiration time is updated to m1 (not m3):
        Assert.assertEquals(7, queue.nextExpirationIn(startTime, TimeUnit.SECONDS));
        // Also, m2 was expired and collected:
        Assert.assertEquals(m2, expiredMessages.remove(0));

        // Wait for m1 expiry time:
        clock.advance(4, TimeUnit.SECONDS);

        try(OutboundMessageQueue.WithLock l = queue.lockOrCallback(clock.now(), () -> {}))
        {
            // Add a new message while we're iterating the queue: this will expire sooner than the already existing message.
            m4 = Message.out(Verb._TEST_1, noPayload, startTime + TimeUnit.SECONDS.toNanos(10));
            queue.add(m4);
        }
        // Check m1 was expired and collected:
        Assert.assertEquals(m1, expiredMessages.remove(0));
        // Check next expiry time is m4 (not m3):
        Assert.assertEquals(10, queue.nextExpirationIn(startTime, TimeUnit.SECONDS));

        // Consume all messages before expiration:
        try(OutboundMessageQueue.WithLock l = queue.lockOrCallback(clock.now(), () -> {}))
        {
            Assert.assertEquals(m3, l.poll());
            Assert.assertEquals(m4, l.poll());
        }
        // Check next expiry time is still m4 as the deadline hasn't passed yet:
        Assert.assertEquals(10, queue.nextExpirationIn(startTime, TimeUnit.SECONDS));

        // Go past the deadline:
        clock.advance(4, TimeUnit.SECONDS);

        try(OutboundMessageQueue.WithLock l = queue.lockOrCallback(clock.now(), () -> {}))
        {
            // Do nothing, just trigger expiration on close
        }
        // Check nothing is expired:
        Assert.assertTrue(expiredMessages.isEmpty());
        // Check next expiry time is now Long.MAX_VALUE as nothing was in the queue:
        Assert.assertEquals(Long.MAX_VALUE, queue.nextExpirationIn(0, TimeUnit.NANOSECONDS));
    }

    @Test
    public void testExpirationOnAdd()
    {
        FreeRunningClock clock = new FreeRunningClock(approxTime.now());

        List<Message> expiredMessages = new LinkedList<>();
        long startTime = clock.now();

        OutboundMessageQueue queue = new OutboundMessageQueue(clock, m -> expiredMessages.add(m));

        Message<?> m1 = Message.out(Verb._TEST_1, noPayload, startTime + TimeUnit.SECONDS.toNanos(7));
        Message<?> m2 = Message.out(Verb._TEST_1, noPayload, startTime + TimeUnit.SECONDS.toNanos(3));
        queue.add(m1);
        queue.add(m2);

        // Check next expiry time is equal to m2, and we haven't expired anything yet:
        Assert.assertEquals(3, queue.nextExpirationIn(startTime, TimeUnit.SECONDS));
        Assert.assertTrue(expiredMessages.isEmpty());

        // Go past m1 expiry time:
        clock.advance(8, TimeUnit.SECONDS);

        // Add a new message and verify both m1 and m2 have been expired:
        Message<?> m3 = Message.out(Verb._TEST_1, noPayload, startTime + TimeUnit.SECONDS.toNanos(10));
        queue.add(m3);
        Assert.assertEquals(m2, expiredMessages.remove(0));
        Assert.assertEquals(m1, expiredMessages.remove(0));

        // New expiration deadline is m3:
        Assert.assertEquals(10, queue.nextExpirationIn(startTime, TimeUnit.SECONDS));

        // Go past m3 expiry time:
        clock.advance(4, TimeUnit.SECONDS);

        try(OutboundMessageQueue.WithLock l = queue.lockOrCallback(clock.now(), () -> {}))
        {
            // Add a new message and verify nothing is expired because the lock is held by this iteration:
            Message<?> m4 = Message.out(Verb._TEST_1, noPayload, startTime + TimeUnit.SECONDS.toNanos(15));
            queue.add(m4);
            Assert.assertTrue(expiredMessages.isEmpty());

            // Also the deadline didn't change, even though we're past the m3 expiry time: this way we're sure the
            // pruner will run promptly even if falling behind during iteration.
            Assert.assertEquals(10, queue.nextExpirationIn(startTime, TimeUnit.SECONDS));
        }

        // Check post iteration m3 has expired:
        Assert.assertEquals(m3, expiredMessages.remove(0));
        // And deadline is now m4:
        Assert.assertEquals(15, queue.nextExpirationIn(startTime, TimeUnit.SECONDS));
    }
}
