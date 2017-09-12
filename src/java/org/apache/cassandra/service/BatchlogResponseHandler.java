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

package org.apache.cassandra.service;

import java.net.InetAddress;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.exceptions.WriteFailureException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessagingService;

public class BatchlogResponseHandler<T> extends AbstractWriteResponseHandler<T>
{
    AbstractWriteResponseHandler<T> wrapped;
    BatchlogCleanup cleanup;
    protected volatile int requiredBeforeFinish;
    private static final AtomicIntegerFieldUpdater<BatchlogResponseHandler> requiredBeforeFinishUpdater
            = AtomicIntegerFieldUpdater.newUpdater(BatchlogResponseHandler.class, "requiredBeforeFinish");

    public BatchlogResponseHandler(AbstractWriteResponseHandler<T> wrapped, int requiredBeforeFinish, BatchlogCleanup cleanup, long queryStartNanoTime)
    {
        super(wrapped.keyspace, wrapped.naturalEndpoints, wrapped.pendingEndpoints, wrapped.consistencyLevel, wrapped.callback, wrapped.writeType, queryStartNanoTime);
        this.wrapped = wrapped;
        this.requiredBeforeFinish = requiredBeforeFinish;
        this.cleanup = cleanup;
    }

    protected int ackCount()
    {
        return wrapped.ackCount();
    }

    public void response(MessageIn<T> msg)
    {
        wrapped.response(msg);
        if (requiredBeforeFinishUpdater.decrementAndGet(this) == 0)
            cleanup.ackMutation();
    }

    public boolean isLatencyForSnitch()
    {
        return wrapped.isLatencyForSnitch();
    }

    public void onFailure(InetAddress from, RequestFailureReason failureReason)
    {
        wrapped.onFailure(from, failureReason);
    }

    public void assureSufficientLiveNodes()
    {
        wrapped.assureSufficientLiveNodes();
    }

    public void get() throws WriteTimeoutException, WriteFailureException
    {
        wrapped.get();
    }

    protected int totalBlockFor()
    {
        return wrapped.totalBlockFor();
    }

    protected int totalEndpoints()
    {
        return wrapped.totalEndpoints();
    }

    protected boolean waitingFor(InetAddress from)
    {
        return wrapped.waitingFor(from);
    }

    protected void signal()
    {
        wrapped.signal();
    }

    public static class BatchlogCleanup
    {
        private final BatchlogCleanupCallback callback;

        protected volatile int mutationsWaitingFor;
        private static final AtomicIntegerFieldUpdater<BatchlogCleanup> mutationsWaitingForUpdater
            = AtomicIntegerFieldUpdater.newUpdater(BatchlogCleanup.class, "mutationsWaitingFor");

        public BatchlogCleanup(int mutationsWaitingFor, BatchlogCleanupCallback callback)
        {
            this.mutationsWaitingFor = mutationsWaitingFor;
            this.callback = callback;
        }

        public void ackMutation()
        {
            if (mutationsWaitingForUpdater.decrementAndGet(this) == 0)
                callback.invoke();
        }
    }

    public interface BatchlogCleanupCallback
    {
        void invoke();
    }
}
