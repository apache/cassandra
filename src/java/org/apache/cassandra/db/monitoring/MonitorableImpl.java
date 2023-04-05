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

package org.apache.cassandra.db.monitoring;

import static org.apache.cassandra.utils.MonotonicClock.Global.approxTime;

public abstract class MonitorableImpl implements Monitorable
{
    private MonitoringState state;
    private boolean isSlow;
    private long approxCreationTimeNanos = -1;
    private long timeoutNanos;
    private long slowTimeoutNanos;
    private boolean isCrossNode;

    protected MonitorableImpl()
    {
        this.state = MonitoringState.IN_PROGRESS;
        this.isSlow = false;
    }

    /**
     * This setter is ugly but the construction chain to ReadCommand
     * is too complex, it would require passing new parameters to all serializers
     * or specializing the serializers to accept these message properties.
     */
    public void setMonitoringTime(long approxCreationTimeNanos, boolean isCrossNode, long timeoutNanos, long slowTimeoutNanos)
    {
        assert approxCreationTimeNanos >= 0;
        this.approxCreationTimeNanos = approxCreationTimeNanos;
        this.isCrossNode = isCrossNode;
        this.timeoutNanos = timeoutNanos;
        this.slowTimeoutNanos = slowTimeoutNanos;
    }

    public long creationTimeNanos()
    {
        return approxCreationTimeNanos;
    }

    public long timeoutNanos()
    {
        return timeoutNanos;
    }

    public boolean isCrossNode()
    {
        return isCrossNode;
    }

    public long slowTimeoutNanos()
    {
        return slowTimeoutNanos;
    }

    public boolean isInProgress()
    {
        check();
        return state == MonitoringState.IN_PROGRESS;
    }

    public boolean isAborted()
    {
        check();
        return state == MonitoringState.ABORTED;
    }

    public boolean isCompleted()
    {
        check();
        return state == MonitoringState.COMPLETED;
    }

    public boolean isSlow()
    {
        check();
        return isSlow;
    }

    public boolean abort()
    {
        if (state == MonitoringState.IN_PROGRESS)
        {
            if (approxCreationTimeNanos >= 0)
                MonitoringTask.addFailedOperation(this, approxTime.now());

            state = MonitoringState.ABORTED;
            return true;
        }

        return state == MonitoringState.ABORTED;
    }

    public boolean complete()
    {
        if (state == MonitoringState.IN_PROGRESS)
        {
            if (isSlow && slowTimeoutNanos > 0 && approxCreationTimeNanos >= 0)
                MonitoringTask.addSlowOperation(this, approxTime.now());

            state = MonitoringState.COMPLETED;
            return true;
        }

        return state == MonitoringState.COMPLETED;
    }

    private void check()
    {
        if (approxCreationTimeNanos < 0 || state != MonitoringState.IN_PROGRESS)
            return;

        long minElapsedNanos = (approxTime.now() - approxCreationTimeNanos) - approxTime.error();

        if (minElapsedNanos >= slowTimeoutNanos && !isSlow)
            isSlow = true;

        if (minElapsedNanos >= timeoutNanos)
            abort();
    }
}
