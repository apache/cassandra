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

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;

public class CassandraMetricRegistry 
{
    private static MetricRegistry defaultRegistry = new MetricRegistry();
    
    private CassandraMetricRegistry()
    {  
    }
    
    public static MetricRegistry get()
    {
        return defaultRegistry;
    }
    
    public static <T extends Metric> T register(String name, T metric) 
    {
        defaultRegistry.remove(name);
        return defaultRegistry.register(name, metric);
    }
    
    public static void unregister(String name)
    {
        defaultRegistry.remove(name);
    }
}
