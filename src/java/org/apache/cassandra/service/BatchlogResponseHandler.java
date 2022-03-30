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

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.exceptions.WriteFailureException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;

public class BatchlogResponseHandler<T> extends AbstractWriteResponseHandler<T>
{
    AbstractWriteResponseHandler<T> wrapped;
    BatchlogCleanup cleanup;
    protected volatile int requiredBeforeFinish;
    private static final AtomicIntegerFieldUpdater<BatchlogResponseHandler> requiredBeforeFinishUpdater
            = AtomicIntegerFieldUpdater.newUpdater(BatchlogResponseHandler.class, "requiredBeforeFinish");

    public BatchlogResponseHandler(AbstractWriteResponseHandler<T> wrapped, int requiredBeforeFinish, BatchlogCleanup cleanup, long queryStartNanoTime)
    {
        super(wrapped.replicaPlan, wrapped.callback, wrapped.writeType, null, queryStartNanoTime);
        this.wrapped = wrapped;
        this.requiredBeforeFinish = requiredBeforeFinish;
        this.cleanup = cleanup;
    }

    protected int ackCount()
    {
        return wrapped.ackCount();
    }

    public void onResponse(Message<T> msg)
    {
        wrapped.onResponse(msg);
        if (requiredBeforeFinishUpdater.decrementAndGet(this) == 0)
            cleanup.ackMutation();
    }

    public void onFailure(InetAddressAndPort from, RequestFailureReason failureReason)
    {
        wrapped.onFailure(from, failureReason);
    }

    public boolean invokeOnFailure()
    {
        return wrapped.invokeOnFailure();
    }

    public void get() throws WriteTimeoutException, WriteFailureException
    {
        wrapped.get();
    }

    protected int blockFor()
    {
        return wrapped.blockFor();
    }

    protected int candidateReplicaCount()
    {
        return wrapped.candidateReplicaCount();
    }

    protected boolean waitingFor(InetAddressAndPort from)
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

        public int decrement()
        {
            return mutationsWaitingForUpdater.decrementAndGet(this);
        }

        public void ackMutation()
        {
            if (decrement() == 0)
                callback.invoke();
        }
    }

    public interface BatchlogCleanupCallback
    {
        void invoke();
    }
}
