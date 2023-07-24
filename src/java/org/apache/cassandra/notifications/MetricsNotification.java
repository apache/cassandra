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

package org.apache.cassandra.notifications;

public class MetricsNotification implements INotification
{
    private final long bytesInserted;
    private final long partitionsRead;
    private final double flushSize;
    private final double sstablePartitionReadLatencyNanos;
    private final double flushTimePerKbNanos;

    public MetricsNotification(long bytesInserted, long partitionsRead, double flushSize, double sstablePartitionReadLatencyNanos, double flushTimePerKbNanos)
    {
        this.bytesInserted = bytesInserted;
        this.partitionsRead = partitionsRead;
        this.flushSize = flushSize;
        this.sstablePartitionReadLatencyNanos = sstablePartitionReadLatencyNanos;
        this.flushTimePerKbNanos = flushTimePerKbNanos;
    }

    public long getBytesInserted()
    {
        return bytesInserted;
    }
    public long getPartitionsRead()
    {
        return partitionsRead;
    }
    public double getFlushSize()
    {
        return flushSize;
    }
    public double getSstablePartitionReadLatencyNanos()
    {
        return sstablePartitionReadLatencyNanos;
    }
    public double getFlushTimePerKbNanos()
    {
        return flushTimePerKbNanos;
    }
}
