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

package org.apache.cassandra.io.sstable;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.DiskOptimizationStrategy;
import org.apache.cassandra.io.util.SequentialWriterOption;

public class IOOptions
{
    public static IOOptions fromDatabaseDescriptor()
    {
        return new IOOptions(DatabaseDescriptor.getDiskOptimizationStrategy(),
                             DatabaseDescriptor.getDiskAccessMode(),
                             DatabaseDescriptor.getIndexAccessMode(),
                             DatabaseDescriptor.getDiskOptimizationEstimatePercentile(),
                             SequentialWriterOption.newBuilder()
                                                   .trickleFsync(DatabaseDescriptor.getTrickleFsync())
                                                   .trickleFsyncByteInterval(DatabaseDescriptor.getTrickleFsyncIntervalInKiB() * 1024)
                                                   .build(),
                             DatabaseDescriptor.getFlushCompression());
    }

    public final DiskOptimizationStrategy diskOptimizationStrategy;
    public final Config.DiskAccessMode defaultDiskAccessMode;
    public final Config.DiskAccessMode indexDiskAccessMode;
    public final double diskOptimizationEstimatePercentile;
    public final SequentialWriterOption writerOptions;
    public final Config.FlushCompression flushCompression;

    public IOOptions(DiskOptimizationStrategy diskOptimizationStrategy,
                     Config.DiskAccessMode defaultDiskAccessMode,
                     Config.DiskAccessMode indexDiskAccessMode,
                     double diskOptimizationEstimatePercentile,
                     SequentialWriterOption writerOptions,
                     Config.FlushCompression flushCompression)
    {
        this.diskOptimizationStrategy = diskOptimizationStrategy;
        this.defaultDiskAccessMode = defaultDiskAccessMode;
        this.indexDiskAccessMode = indexDiskAccessMode;
        this.diskOptimizationEstimatePercentile = diskOptimizationEstimatePercentile;
        this.writerOptions = writerOptions;
        this.flushCompression = flushCompression;
    }
}