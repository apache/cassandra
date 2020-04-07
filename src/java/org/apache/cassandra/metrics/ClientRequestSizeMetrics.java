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
import com.codahale.metrics.Histogram;

/**
 * Metrics to track the size of incoming and outgoing bytes at Cassandra server.
 */
public class ClientRequestSizeMetrics
{
    private static final String TYPE = "ClientRequestSize";
    public static final Counter totalBytesRead = Metrics.counter(DefaultNameFactory.createMetricName(TYPE, "IncomingBytes", null));
    public static final Counter totalBytesWritten = Metrics.counter(DefaultNameFactory.createMetricName(TYPE, "OutgoingBytes", null));
    public static final Histogram bytesRecievedPerFrame = Metrics.histogram(DefaultNameFactory.createMetricName(TYPE, "BytesRecievedPerFrame", null), true);
    public static final Histogram bytesTransmittedPerFrame = Metrics.histogram(DefaultNameFactory.createMetricName(TYPE, "BytesTransmittedPerFrame", null), true);
}
