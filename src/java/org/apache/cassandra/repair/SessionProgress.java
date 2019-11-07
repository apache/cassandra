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

import com.google.common.base.Throwables;

public class SessionProgress implements Progress
{
    public enum State {
        INIT, START,
        JOBS_SUBMIT, JOBS_COMPLETE,
        SKIPPED, FAILURE
    }

    private final long[] stateTimes = new long[State.values().length];
    private int currentState;
    private String failureCause;
    private volatile long lastUpdatedAtNs;

    SessionProgress()
    {
        updateState(State.INIT);
    }

    public void start()
    {
        updateState(State.START);
    }

    public void jobsSubmitted()
    {
        updateState(State.JOBS_SUBMIT);
    }

    public void complete()
    {
        updateState(State.JOBS_COMPLETE);
    }

    public void skip(String reason)
    {
        failureCause = reason;
        updateState(State.SKIPPED);
    }

    public void fail(String cause)
    {
        failureCause = cause;
        updateState(State.FAILURE);
    }

    public void fail(Throwable cause)
    {
        fail(Throwables.getStackTraceAsString(cause));
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

    public long getCreationtTimeNs()
    {
        return stateTimes[State.INIT.ordinal()];
    }

    public long getStartTimeNs()
    {
        return stateTimes[State.START.ordinal()];
    }

    public String getFailureCause()
    {
        return failureCause;
    }

    public long getLastUpdatedAtNs()
    {
        return lastUpdatedAtNs;
    }

    public boolean isComplete()
    {
        switch (getState())
        {
            case SKIPPED:
            case JOBS_COMPLETE:
            case FAILURE:
                return true;
            default:
                return false;
        }
    }

    public float getProgress()
    {
        if (isComplete())
            return 1.0f;
        return (float) currentState / (float) JobProgress.State.values().length;
    }
}
