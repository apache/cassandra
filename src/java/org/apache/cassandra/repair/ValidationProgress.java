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

package org.apache.cassandra.repair;

/**
 * Used for tracking the progress a single validation is making.  The expected usage is to have a single object per
 * validation and must be created <b>before</b> submitting on a stage or thread pool (to track queue time). When ready
 * to start a call to {@link #start(long, long)} will set the estimates used for progress tracking. Periodic calls to
 * {@link #processed(int)} are needed to update estimates for partitions processed. When the validation is
 * successful, should call {@link #complete()} and if it fails should call {@link #fail(Throwable)}.
 *
 * This class is designed to be accessed (read) by multiple threads, but written with single threaded access. In order
 * for this class to be thread-safe, a few rules must be followed:
 *
 * <ul>
 *     <li>no concurrent writers (callers to {@link #processed(int)}, {@link #complete()}), or {@link #fail(Throwable)};
 *     only one thread should mutate at any point in time</li>
 *     <li>getters are allowed to return stale data</li>
 *     <li>for latest data, call {@link #getLastUpdatedAtNs()} before calling other getters</li>
 * </ul>
 */
public class ValidationProgress implements Progress
{
    public enum State {INIT, RUNNING, SUCCESS, FAILURE}

    private final long creationtTimeNs = System.nanoTime();
    private long startTimeNs;
    private State state = State.INIT;
    private long estimatedPartitions;
    private long estimatedTotalBytes;
    private long partitionsProcessed;
    private Throwable failureCause;
    private volatile long lastUpdatedAtNs;

    ValidationProgress()
    {
        lastUpdatedAtNs = creationtTimeNs;
    }

    public void start(long estimatedPartitions, long estimatedTotalBytes)
    {
        checkState(State.INIT);
        this.startTimeNs = System.nanoTime();
        this.estimatedPartitions = estimatedPartitions;
        this.estimatedTotalBytes = estimatedTotalBytes;
        this.state = State.RUNNING;
        this.lastUpdatedAtNs = startTimeNs;
    }

    /**
     * What time this object was created.
     *
     * Uses {@link System#nanoTime()} so does not reflect unix epoch.
     */
    @Override
    public long getCreationtTimeNs()
    {
        return creationtTimeNs;
    }

    /**
     * What time the validation started (call to {@link #start(long, long)}.
     *
     * Uses {@link System#nanoTime()} so does not reflect unix epoch.
     */
    @Override
    public long getStartTimeNs()
    {
        return startTimeNs;
    }

    public State getState()
    {
        return state;
    }

    public long getEstimatedPartitions()
    {
        return estimatedPartitions;
    }

    public long getEstimatedTotalBytes()
    {
        return estimatedTotalBytes;
    }

    public long getPartitionsProcessed()
    {
        return partitionsProcessed;
    }

    @Override
    public Throwable getFailureCause()
    {
        return failureCause;
    }

    /**
     * The time the last mutation was made.
     *
     * Uses {@link System#nanoTime()} so does not reflect unix epoch.
     */
    @Override
    public long getLastUpdatedAtNs()
    {
        return lastUpdatedAtNs;
    }

    @Override
    public boolean isComplete()
    {
        switch (state)
        {
            case SUCCESS:
            case FAILURE:
                return true;
            default:
                return false;
        }
    }

    /**
     * @return 0.0 to 1.0 to represent estimate on validation progress
     */
    @Override
    public float getProgress()
    {
        switch (state)
        {
            case INIT:
                return 0.0F;
            case SUCCESS:
            case FAILURE:
                return 1.0F;
        }
        if (estimatedPartitions == 0) // mostly to avoid / 0
            return 1.0f;
        return Math.min(1.0f, partitionsProcessed / (float) estimatedPartitions);
    }

    /**
     * Updates {@link #getPartitionsProcessed()}; this may only be called while state is {@link State#RUNNING}.
     */
    public void processed(int processed)
    {
        checkState(State.RUNNING);
        partitionsProcessed += processed;
        lastUpdatedAtNs = System.nanoTime();
    }

    /**
     * Marks state as {@link State#SUCCESS}; this may only be called while state is {@link State#RUNNING}.
     */
    public void complete()
    {
        checkState(State.RUNNING);
        state = State.SUCCESS;
        lastUpdatedAtNs = System.nanoTime();
    }

    /**
     * Marks state as {@link State#FAILURE}; this may only be called while state is {@link State#RUNNING}.
     */
    public void fail(Throwable failureCause)
    {
        checkState(State.RUNNING);
        this.failureCause = failureCause;
        this.state = State.FAILURE;
        this.lastUpdatedAtNs = System.nanoTime();
    }

    private void checkState(State expected)
    {
        State state = this.state;
        assert state == expected : "Expected state " + expected + " but actually " + state;
    }
}
