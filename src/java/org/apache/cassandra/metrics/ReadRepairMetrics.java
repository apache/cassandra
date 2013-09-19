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

import java.util.concurrent.TimeUnit;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;

/**
 * Metrics related to Read Repair.
 */
public class ReadRepairMetrics
{
    private static final MetricNameFactory factory = new DefaultNameFactory("ReadRepair");

    public static final Meter repairedBlocking =
            Metrics.newMeter(factory.createMetricName("RepairedBlocking"), "RepairedBlocking", TimeUnit.SECONDS);
    public static final Meter repairedBackground =
            Metrics.newMeter(factory.createMetricName("RepairedBackground"), "RepairedBackground", TimeUnit.SECONDS);
    public static final Meter attempted = 
            Metrics.newMeter(factory.createMetricName("Attempted"), "Attempted", TimeUnit.SECONDS);
}
