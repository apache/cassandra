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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.cassandra.utils.concurrent.BlockingQueues.newBlockingQueue;

/**
 * Allows inspecting the behavior of mocked messaging by observing {@link MatcherResponse}.
 */
public class MockMessagingSpy
{
    private static final Logger logger = LoggerFactory.getLogger(MockMessagingSpy.class);

    private final AtomicInteger messagesIntercepted = new AtomicInteger();
    private final AtomicInteger mockedMessageResponses = new AtomicInteger();

    private final BlockingQueue<Message<?>> interceptedMessages = newBlockingQueue();
    private final BlockingQueue<Message<?>> deliveredResponses = newBlockingQueue();

    private static final Executor executor = Executors.newSingleThreadExecutor();

    /**
     * Returns a future with the first mocked incoming message that has been created and delivered.
     */
    public ListenableFuture<Message<?>> captureMockedMessage()
    {
        return Futures.transform(captureMockedMessageN(1), (List<Message<?>> result) -> result.isEmpty() ? null : result.get(0), MoreExecutors.directExecutor());
    }

    /**
     * Returns a future with the specified number mocked incoming messages that have been created and delivered.
     */
    public ListenableFuture<List<Message<?>>> captureMockedMessageN(int noOfMessages)
    {
        CapturedResultsFuture<Message<?>> ret = new CapturedResultsFuture<>(noOfMessages, deliveredResponses);
        executor.execute(ret);
        return ret;
    }

    /**
     * Returns a future that will indicate if a mocked incoming message has been created and delivered.
     */
    public ListenableFuture<Boolean> expectMockedMessage()
    {
        return expectMockedMessage(1);
    }

    /**
     * Returns a future that will indicate if the specified number of mocked incoming message have been created and delivered.
     */
    public ListenableFuture<Boolean> expectMockedMessage(int noOfMessages)
    {
        ResultsCompletionFuture<Message<?>> ret = new ResultsCompletionFuture<>(noOfMessages, deliveredResponses);
        executor.execute(ret);
        return ret;
    }

    /**
     * Returns a future with the first intercepted outbound message that would have been send.
     */
    public ListenableFuture<Message<?>> captureMessageOut()
    {
        return Futures.transform(captureMessageOut(1), (List<Message<?>> result) -> result.isEmpty() ? null : result.get(0), MoreExecutors.directExecutor());
    }

    /**
     * Returns a future with the specified number of intercepted outbound messages that would have been send.
     */
    public ListenableFuture<List<Message<?>>> captureMessageOut(int noOfMessages)
    {
        CapturedResultsFuture<Message<?>> ret = new CapturedResultsFuture<>(noOfMessages, interceptedMessages);
        executor.execute(ret);
        return ret;
    }

    /**
     * Returns a future that will indicate if an intercepted outbound messages would have been send.
     */
    public ListenableFuture<Boolean> interceptMessageOut()
    {
        return interceptMessageOut(1);
    }

    /**
     * Returns a future that will indicate if the specified number of intercepted outbound messages would have been send.
     */
    public ListenableFuture<Boolean> interceptMessageOut(int noOfMessages)
    {
        ResultsCompletionFuture<Message<?>> ret = new ResultsCompletionFuture<>(noOfMessages, interceptedMessages);
        executor.execute(ret);
        return ret;
    }

    /**
     * Returns a future that will indicate the absence of any intercepted outbound messages with the specifed period.
     */
    public ListenableFuture<Boolean> interceptNoMsg(long time, TimeUnit unit)
    {
        ResultAbsenceFuture<Message<?>> ret = new ResultAbsenceFuture<>(interceptedMessages, time, unit);
        executor.execute(ret);
        return ret;
    }

    public int messagesIntercepted()
    {
        return messagesIntercepted.get();
    }

    public int mockedMessageResponses()
    {
        return mockedMessageResponses.get();
    }

    void matchingMessage(Message<?> message)
    {
        messagesIntercepted.incrementAndGet();
        logger.trace("Received matching message: {}", message);
        interceptedMessages.add(message);
    }

    void matchingResponse(Message<?> response)
    {
        mockedMessageResponses.incrementAndGet();
        logger.trace("Responding to intercepted message: {}", response);
        deliveredResponses.add(response);
    }

    private static class CapturedResultsFuture<T> extends AbstractFuture<List<T>> implements Runnable
    {
        private final int waitForResults;
        private final List<T> results;
        private final BlockingQueue<T> queue;

        CapturedResultsFuture(int waitForResponses, BlockingQueue<T> queue)
        {
            this.waitForResults = waitForResponses;
            results = new ArrayList<T>(waitForResponses);
            this.queue = queue;
        }

        public void run()
        {
            try
            {
                while (results.size() < waitForResults)
                    results.add(queue.take());

                set(results);
            }
            catch (InterruptedException e)
            {
                throw new AssertionError();
            }
        }
    }

    private static class ResultsCompletionFuture<T> extends AbstractFuture<Boolean> implements Runnable
    {
        private final int waitForResults;
        private final BlockingQueue<T> queue;

        ResultsCompletionFuture(int waitForResponses, BlockingQueue<T> queue)
        {
            this.waitForResults = waitForResponses;
            this.queue = queue;
        }

        public void run()
        {
            try
            {
                for (int i = 0; i < waitForResults; i++)
                {
                    queue.take();
                }
                set(true);
            }
            catch (InterruptedException e)
            {
                throw new AssertionError();
            }
        }
    }

    private static class ResultAbsenceFuture<T> extends AbstractFuture<Boolean> implements Runnable
    {
        private final BlockingQueue<T> queue;
        private final long time;
        private final TimeUnit unit;

        ResultAbsenceFuture(BlockingQueue<T> queue, long time, TimeUnit unit)
        {
            this.queue = queue;
            this.time = time;
            this.unit = unit;
        }

        public void run()
        {
            try
            {
                T result = queue.poll(time, unit);
                if (result != null)
                    setException(new AssertionError("Received unexpected message: " + result));
                else
                    set(true);
            }
            catch (InterruptedException e)
            {
                throw new AssertionError();
            }
        }
    }
}
