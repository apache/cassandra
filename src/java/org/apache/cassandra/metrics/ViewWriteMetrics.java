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

package org.apache.cassandra.metrics;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Gauge;

public class ViewWriteMetrics extends ClientRequestMetrics
{
    public final Counter viewReplicasAttempted;
    public final Counter viewReplicasSuccess;
    // time between when mutation is applied to local memtable to when CL.ONE is achieved on MV
    public final Timer viewWriteLatency;

    public ViewWriteMetrics(String scope)
    {
        super(scope);
        viewReplicasAttempted = Metrics.counter(factory.createMetricName("ViewReplicasAttempted"));
        viewReplicasSuccess = Metrics.counter(factory.createMetricName("ViewReplicasSuccess"));
        viewWriteLatency = Metrics.timer(factory.createMetricName("ViewWriteLatency"));
        Metrics.register(factory.createMetricName("ViewPendingMutations"), new Gauge<Long>()
                {
                    public Long getValue()
                    {
                        return viewReplicasAttempted.getCount() - viewReplicasSuccess.getCount();
                    }
                });
    }

    public void release()
    {
        super.release();
        Metrics.remove(factory.createMetricName("ViewReplicasAttempted"));
        Metrics.remove(factory.createMetricName("ViewReplicasSuccess"));
        Metrics.remove(factory.createMetricName("ViewWriteLatency"));
        Metrics.remove(factory.createMetricName("ViewPendingMutations"));
    }
}
