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

import org.apache.cassandra.cql3.QueryProcessor;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.util.RatioGauge;

public class CQLMetrics
{
    private static final MetricNameFactory factory = new DefaultNameFactory("CQL");

    public final Counter regularStatementsExecuted;
    public final Counter preparedStatementsExecuted;
    public final Counter preparedStatementsEvicted;

    public final Gauge<Integer> preparedStatementsCount;
    public final Gauge<Double> preparedStatementsRatio;

    public CQLMetrics()
    {
        regularStatementsExecuted = Metrics.newCounter(factory.createMetricName("RegularStatementsExecuted"));
        preparedStatementsExecuted = Metrics.newCounter(factory.createMetricName("PreparedStatementsExecuted"));
        preparedStatementsEvicted = Metrics.newCounter(factory.createMetricName("PreparedStatementsEvicted"));

        preparedStatementsCount = Metrics.newGauge(factory.createMetricName("PreparedStatementsCount"), new Gauge<Integer>()
        {
            public Integer value()
            {
                return QueryProcessor.preparedStatementsCount();
            }
        });
        preparedStatementsRatio = Metrics.newGauge(factory.createMetricName("PreparedStatementsRatio"), new RatioGauge()
        {
            public double getNumerator()
            {
                return preparedStatementsExecuted.count();
            }

            public double getDenominator()
            {
                return regularStatementsExecuted.count() + preparedStatementsExecuted.count();
            }
        });
    }
}
