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

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.util.RatioGauge;


public class CqlStatementMetrics
{
    private final MetricNameFactory factory = new DefaultNameFactory("CqlStatement");
    public final Counter activePreparedStatements = Metrics.newCounter(factory.createMetricName("ActivePreparedStatements"));
    public final Counter executedPrepared = Metrics.newCounter(factory.createMetricName("ExecutedPrepared"));
    public final Counter executedUnprepared = Metrics.newCounter(factory.createMetricName("ExecutedUnPrepared"));

    public final Gauge<Double> preparedRatio = Metrics.newGauge(factory.createMetricName("PreparedUnpreparedRatio"), new RatioGauge()
    {
        protected double getNumerator()
        {
            long num = executedPrepared.count();
            return num == 0 ? 1 : num;
        }

        protected double getDenominator()
        {
            long den = executedUnprepared.count();
            return den == 0 ? 1 : den;
        }
    });

    public void reset()
    {
        executedPrepared.clear();
        executedUnprepared.clear();
    }
}
