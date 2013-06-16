package org.apache.cassandra.metrics;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */


import java.util.concurrent.TimeUnit;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;

/**
 * Metrics related to Read Repair.
 */
public class ReadRepairMetrics {
    public static final String GROUP_NAME = "org.apache.cassandra.metrics";
    public static final String TYPE_NAME = "ReadRepair";
    
    public static final Meter repairedBlocking =
            Metrics.newMeter(new MetricName(GROUP_NAME, TYPE_NAME, "RepairedBlocking"), "RepairedBlocking", TimeUnit.SECONDS);
    public static final Meter repairedBackground =
            Metrics.newMeter(new MetricName(GROUP_NAME, TYPE_NAME, "RepairedBackground"), "RepairedBackground", TimeUnit.SECONDS);
    public static final Meter attempted = 
            Metrics.newMeter(new MetricName(GROUP_NAME, TYPE_NAME, "Attempted"), "Attempted", TimeUnit.SECONDS);
}
