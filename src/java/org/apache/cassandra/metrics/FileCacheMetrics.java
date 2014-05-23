/**
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

import java.util.concurrent.TimeUnit;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.util.RatioGauge;
import org.apache.cassandra.service.FileCacheService;

public class FileCacheMetrics
{
    private static final MetricNameFactory factory = new DefaultNameFactory("FileCache");

    /** Total number of hits */
    public final Meter hits;
    /** Total number of requests */
    public final Meter requests;
    /** hit rate */
    public final Gauge<Double> hitRate;
    /** Total size of file cache, in bytes */
    public final Gauge<Long> size;

    public FileCacheMetrics()
    {
        hits = Metrics.newMeter(factory.createMetricName("Hits"), "hits", TimeUnit.SECONDS);
        requests = Metrics.newMeter(factory.createMetricName("Requests"), "requests", TimeUnit.SECONDS);
        hitRate = Metrics.newGauge(factory.createMetricName("HitRate"), new RatioGauge()
        {
            protected double getNumerator()
            {
                return hits.count();
            }

            protected double getDenominator()
            {
                return requests.count();
            }
        });
        size = Metrics.newGauge(factory.createMetricName("Size"), new Gauge<Long>()
        {
            public Long value()
            {
                return FileCacheService.instance.sizeInBytes();
            }
        });
    }
}
