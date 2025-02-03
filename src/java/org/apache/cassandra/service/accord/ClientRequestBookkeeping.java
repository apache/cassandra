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

import java.util.function.Function;

import com.codahale.metrics.Meter;
import org.apache.cassandra.metrics.AccordClientRequestMetrics;
import org.apache.cassandra.metrics.ClientRequestMetrics;

import static org.apache.cassandra.service.accord.RequestBookkeeping.ThrowsExceptionType.READ;
import static org.apache.cassandra.service.accord.RequestBookkeeping.ThrowsExceptionType.WRITE;

// TODO (expected): merge with AccordClientRequestMetrics instead
public class ClientRequestBookkeeping extends RequestBookkeeping
{
    final AccordClientRequestMetrics metrics;

    public ClientRequestBookkeeping(boolean isWrite, AccordClientRequestMetrics metrics)
    {
        super(isWrite ? WRITE : READ);
        this.metrics = metrics;
    }

    @Override
    final void markTimeout()
    {
        mark(metrics -> metrics.timeouts);
    }

    @Override
    final void markPreempted()
    {
        metrics.preempted.mark();
    }

    final void markFailure()
    {
        mark(metrics -> metrics.failures);
    }

    @Override
    final void markRetryDifferentSystem()
    {
        metrics.retryDifferentSystem.mark();
    }

    @Override
    final void markTopologyMismatch()
    {
        metrics.topologyMismatches.mark();
    }

    private void mark(Function<ClientRequestMetrics, Meter> get)
    {
        get.apply(metrics).mark();
        if (metrics.shared != null)
            get.apply(metrics.shared).mark();
    }

    public final void markElapsedNanos(long nanos)
    {
        metrics.addNano(nanos);
        if (metrics.shared != null)
            metrics.shared.addNano(nanos);
    }
}
