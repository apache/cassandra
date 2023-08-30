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
package org.apache.cassandra.repair.state;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Throwables;

import org.apache.cassandra.utils.Clock;

public abstract class AbstractCompletable<I> implements Completable<I>
{
    public enum Status { INIT, ACCEPTED, COMPLETED }

    private final long creationTimeMillis; // used to convert from nanos to millis
    private final long creationTimeNanos;
    protected final Clock clock;
    private final AtomicReference<Result> result = new AtomicReference<>(null);
    public final I id;
    protected volatile long lastUpdatedAtNs;

    public AbstractCompletable(Clock clock, I id)
    {
        this.creationTimeMillis = clock.currentTimeMillis();
        this.creationTimeNanos = clock.nanoTime();
        this.clock = clock;
        this.id = id;
    }

    public abstract boolean isAccepted();

    public Status getCompletionStatus()
    {
        Result result = getResult();
        if (result != null)
            return Status.COMPLETED;
        return isAccepted() ? Status.ACCEPTED : Status.INIT;
    }

    @Override
    public I getId()
    {
        return id;
    }

    @Override
    public long getInitializedAtMillis()
    {
        return nanosToMillis(creationTimeNanos);
    }

    @Override
    public long getInitializedAtNanos()
    {
        return creationTimeNanos;
    }

    @Override
    public long getLastUpdatedAtMillis()
    {
        return nanosToMillis(lastUpdatedAtNs);
    }

    @Override
    public long getLastUpdatedAtNanos()
    {
        return lastUpdatedAtNs;
    }

    @Override
    public Result getResult()
    {
        return result.get();
    }

    public void updated()
    {
        lastUpdatedAtNs = clock.nanoTime();
    }

    protected boolean tryResult(Result result)
    {
        if (!this.result.compareAndSet(null, result))
            return false;
        onComplete();
        lastUpdatedAtNs = clock.nanoTime();
        return true;
    }

    protected void onComplete() {}

    protected long nanosToMillis(long nanos)
    {
        // nanos - creationTimeNanos = delta since init
        return creationTimeMillis + TimeUnit.NANOSECONDS.toMillis(nanos - creationTimeNanos);
    }

    protected class BaseSkipPhase extends BasePhase
    {
        public void skip(String msg)
        {
            tryResult(Result.skip(msg));
        }
    }

    protected class BasePhase
    {
        public void success()
        {
            tryResult(Result.success());
        }

        public void success(String msg)
        {
            tryResult(Result.success(msg));
        }

        public void fail(Throwable e)
        {
            fail(e == null ? null : Throwables.getStackTraceAsString(e));
        }

        public void fail(String failureCause)
        {
            tryResult(Result.fail(failureCause));
        }
    }
}
