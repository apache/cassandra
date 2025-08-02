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

import java.util.function.Supplier;

import com.codahale.metrics.Snapshot;
import org.apache.cassandra.metrics.LogLinearHistogram.LogLinearSnapshot;

import static org.apache.cassandra.metrics.CassandraReservoir.BucketStrategy.log_linear;

public class OnDemandHistogram extends OverrideHistogram
{
    final Supplier<LogLinearSnapshot> snapshot;
    protected OnDemandHistogram(Supplier<LogLinearSnapshot> snapshot)
    {
        this.snapshot = snapshot;
    }

    @Override
    public synchronized long getCount()
    {
        return snapshot.get().totalCount;
    }

    @Override
    public Snapshot getSnapshot()
    {
        return snapshot.get();
    }

    @Override
    public CassandraReservoir.BucketStrategy bucketStrategy()
    {
        return log_linear;
    }

    @Override
    public long[] bucketStarts(int length)
    {
        return LogLinearHistogram.bucketsWithLength(length);
    }
}
