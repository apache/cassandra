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

public class JobProgress implements Progress
{
    public enum State {
        INIT, START,
        SNAPSHOT_REQUEST, SNAPSHOT_COMPLETE,
        VALIDATION_REQUEST, VALIDATON_COMPLETE,
        SYNC_REQUEST, SYNC_COMPLETE,
        FAILURE
    }

    private final long[] stateTimes = new long[State.values().length];
    private int currentState;
    private Throwable failureCause;
    private volatile long lastUpdatedAtNs;

    JobProgress()
    {
        updateState(State.INIT);
    }

    public void start()
    {
        updateState(State.START);
    }

    public void snapshotRequest()
    {
        updateState(State.SNAPSHOT_REQUEST);
    }

    public void snapshotComplete()
    {
        updateState(State.SNAPSHOT_COMPLETE);
    }

    public void validationRequested()
    {
        updateState(State.VALIDATION_REQUEST);
    }

    public void validationComplete()
    {
        updateState(State.VALIDATON_COMPLETE);
    }

    public void syncRequest()
    {
        updateState(State.SYNC_REQUEST);
    }

    public void complete()
    {
        updateState(State.SYNC_COMPLETE);
    }

    public void fail(Throwable failureCause)
    {
        this.failureCause = failureCause;
        updateState(State.FAILURE);
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
    public Throwable getFailureCause()
    {
        return failureCause;
    }

    @Override
    public long getLastUpdatedAtNs()
    {
        return lastUpdatedAtNs;
    }

    @Override
    public boolean isComplete()
    {
        switch (getState())
        {
            case SYNC_COMPLETE:
            case FAILURE:
                return true;
            default:
                return false;
        }
    }

    @Override
    public float getProgress()
    {
        //TODO - since this currently doesn't track participent state progress can only be based off the current state
        // some states are more costly than others, so this doesn't do a good job showing progress
        if (isComplete())
            return 1.0f;
        return (float) currentState / (float) State.values().length;
    }
}
