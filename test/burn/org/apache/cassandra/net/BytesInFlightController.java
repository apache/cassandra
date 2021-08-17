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

import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.locks.LockSupport;
import java.util.function.IntConsumer;

import org.apache.cassandra.utils.Pair;

public class BytesInFlightController
{
    private static final AtomicLongFieldUpdater<BytesInFlightController> sentBytesUpdater = AtomicLongFieldUpdater.newUpdater(BytesInFlightController.class, "sentBytes");
    private static final AtomicLongFieldUpdater<BytesInFlightController> receivedBytesUpdater = AtomicLongFieldUpdater.newUpdater(BytesInFlightController.class, "receivedBytes");

    private volatile long minimumInFlightBytes, maximumInFlightBytes;
    private volatile long sentBytes;
    private volatile long receivedBytes;
    private final ConcurrentLinkedQueue<Pair<Integer, IntConsumer>> deferredBytes = new ConcurrentLinkedQueue<>();
    private final ConcurrentSkipListMap<Long, Thread> waitingToSend = new ConcurrentSkipListMap<>();

    BytesInFlightController(long maximumInFlightBytes)
    {
        this.maximumInFlightBytes = maximumInFlightBytes;
    }

    public void send(long bytes) throws InterruptedException
    {
        long sentBytes = sentBytesUpdater.getAndAdd(this, bytes);
        maybeProcessDeferred();
        if ((sentBytes - receivedBytes) >= maximumInFlightBytes)
        {
            long waitUntilReceived = sentBytes - maximumInFlightBytes;
            // overlap shouldn't occur, but cannot guarantee it when we modify maximumInFlightBytes
            Thread prev = waitingToSend.putIfAbsent(waitUntilReceived, Thread.currentThread());
            while (prev != null)
                prev = waitingToSend.putIfAbsent(++waitUntilReceived, Thread.currentThread());

            boolean isInterrupted;
            while (!(isInterrupted = Thread.currentThread().isInterrupted())
                   && waitUntilReceived - receivedBytes >= 0
                   && waitingToSend.get(waitUntilReceived) != null)
            {
                LockSupport.park();
            }
            waitingToSend.remove(waitUntilReceived);

            if (isInterrupted)
                throw new InterruptedException();
        }
    }

    public long minimumInFlightBytes() { return minimumInFlightBytes; }
    public long maximumInFlightBytes() { return maximumInFlightBytes; }

    void adjust(int predictedSentBytes, int actualSentBytes)
    {
        receivedBytesUpdater.addAndGet(this, predictedSentBytes - actualSentBytes);
        if (predictedSentBytes > actualSentBytes) wakeupSenders();
        else maybeProcessDeferred();
    }

    public long inFlight()
    {
        return sentBytes - receivedBytes;
    }

    public void fail(int bytes)
    {
        receivedBytesUpdater.addAndGet(this, bytes);
        wakeupSenders();
    }

    public void process(int bytes, IntConsumer releaseBytes)
    {
        while (true)
        {
            long sent = sentBytes;
            long received = receivedBytes;
            long newReceived = received + bytes;
            if (sent - newReceived <= minimumInFlightBytes)
            {
                deferredBytes.add(Pair.create(bytes, releaseBytes));
                break;
            }
            if (receivedBytesUpdater.compareAndSet(this, received, newReceived))
            {
                releaseBytes.accept(bytes);
                break;
            }
        }
        maybeProcessDeferred();
        wakeupSenders();
    }

    void setInFlightByteBounds(long minimumInFlightBytes, long maximumInFlightBytes)
    {
        this.minimumInFlightBytes = minimumInFlightBytes;
        this.maximumInFlightBytes = maximumInFlightBytes;
        maybeProcessDeferred();
    }

    // unlike the rest of the class, this method does not handle wrap-around of sent/received;
    // since this shouldn't happen it's no big deal, but maybe for absurdly long runs it might.
    // if so, fix it.
    private void wakeupSenders()
    {
        Map.Entry<Long, Thread> next;
        while (null != (next = waitingToSend.firstEntry()))
        {
            if (next.getKey() - receivedBytes >= 0)
                break;

            if (waitingToSend.remove(next.getKey(), next.getValue()))
                LockSupport.unpark(next.getValue());
        }
    }

    private void maybeProcessDeferred()
    {
        while (true)
        {
            long sent = sentBytes;
            long received = receivedBytes;
            if (sent - received <= minimumInFlightBytes)
                break;

            Pair<Integer, IntConsumer> next = deferredBytes.poll();
            if (next == null)
                break;

            int receive = next.left;
            IntConsumer callbacks = next.right;
            while (true)
            {
                long newReceived = received + receive;
                if (receivedBytesUpdater.compareAndSet(this, received, newReceived))
                {
                    callbacks.accept(receive);
                    wakeupSenders();
                    break;
                }

                sent = sentBytes;
                received = receivedBytes;
                if (sent - received <= minimumInFlightBytes)
                {
                    deferredBytes.add(next);
                    break; // continues with outer loop to maybe process it if minimumInFlightBytes has changed meanwhile
                }
            }
        }
    }

}
