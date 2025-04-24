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

package org.apache.cassandra.service.accord;

import org.apache.cassandra.metrics.LatencyMetrics;
import static org.apache.cassandra.service.accord.RequestBookkeeping.ThrowsExceptionType.READ;

public abstract class TimeOnlyRequestBookkeeping extends RequestBookkeeping
{
    public static class LatencyRequestBookkeeping extends TimeOnlyRequestBookkeeping
    {
        final LatencyMetrics latency;

        public LatencyRequestBookkeeping(LatencyMetrics latency)
        {
            this.latency = latency;
        }

        public final void markElapsedNanos(long nanos)
        {
            if (latency != null)
                latency.addNano(nanos);
        }
    }

    TimeOnlyRequestBookkeeping()
    {
        super(READ);
    }

    @Override
    final void markTimeout()
    {
    }

    @Override
    final void markPreempted()
    {
    }

    final void markFailure()
    {
    }

    @Override
    final void markRetryDifferentSystem()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    void markTopologyMismatch()
    {
        throw new UnsupportedOperationException();
    }
}
