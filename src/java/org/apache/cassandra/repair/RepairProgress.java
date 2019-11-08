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

import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Throwables;

import org.apache.cassandra.repair.consistent.SyncStatSummary;

public class RepairProgress implements Progress
{
    public enum State {
        INIT, START,
        PREPARE_SUBMIT, PREPARE_COMPLETE,
        SESSIONS_SUBMIT, SESSIONS_COMPLETE,
        SKIPPED, COMPLETE,
        FAILURE
    }

    private final long creationTimeMillis = System.currentTimeMillis();
    private final long[] stateTimes = new long[State.values().length];
    private int currentState;
    private int files;
    private long bytes;
    private int ranges;
    private String failureCause;
    private volatile long lastUpdatedAtNs;

    RepairProgress()
    {
        updateState(State.INIT);
    }

    public void start()
    {
        updateState(State.START);
    }

    public void prepareStart()
    {
        updateState(State.PREPARE_SUBMIT);
    }

    public void prepareComplete()
    {
        updateState(State.PREPARE_COMPLETE);
    }

    public void sessionStart()
    {
        updateState(State.SESSIONS_SUBMIT);
    }

    public void sessionComplete(List<RepairSessionResult> results)
    {
        SyncStatSummary summary = new SyncStatSummary(true);
        summary.consumeSessionResults(results);
        this.files = summary.getFiles();
        this.bytes = summary.getBytes();
        this.ranges = summary.getRanges();
        updateState(State.SESSIONS_COMPLETE);
    }

    public void skip(String reason)
    {
        failureCause = reason;
        updateState(State.SKIPPED);
    }

    public void complete()
    {
        updateState(State.COMPLETE);
    }

    public void fail(String failureCause)
    {
        this.failureCause = failureCause;
        updateState(State.FAILURE);
    }

    public void fail(Throwable failureCause)
    {
        fail(Throwables.getStackTraceAsString(failureCause));
    }

    public State getState()
    {
        return State.values()[currentState];
    }

    private void updateState(State state)
    {
        long now = System.nanoTime();
        stateTimes[currentState = state.ordinal()] = now;
        lastUpdatedAtNs = now;
    }

    public int getFiles()
    {
        return files;
    }

    public long getBytes()
    {
        return bytes;
    }

    public int getRanges()
    {
        return ranges;
    }

    @Override
    public long getCreationtTimeNs()
    {
        return stateTimes[State.INIT.ordinal()];
    }

    @Override
    public long getStartTimeNs()
    {
        return stateTimes[State.START.ordinal()];
    }

    @Override
    public String getFailureCause()
    {
        return failureCause;
    }

    @Override
    public long getLastUpdatedAtNs()
    {
        return lastUpdatedAtNs;
    }

    public long getLastUpdatedAtMicro()
    {
        // this is not acurate, but close enough to target human readability
        long durationNanos = lastUpdatedAtNs - stateTimes[0];
        return TimeUnit.MILLISECONDS.toMicros(creationTimeMillis) + TimeUnit.NANOSECONDS.toMicros(durationNanos);
    }

    public boolean isComplete()
    {
        switch (getState())
        {
            case SKIPPED:
            case COMPLETE:
            case FAILURE:
                return true;
            default:
                return false;
        }
    }

    public float getProgress()
    {
        //TODO - since this currently doesn't track participent state progress can only be based off the current state
        // some states are more costly than others, so this doesn't do a good job showing progress
        if (isComplete())
            return 1.0f;
        return (float) currentState / (float) State.values().length;
    }
}
