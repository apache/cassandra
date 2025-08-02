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

import com.codahale.metrics.Snapshot;
import org.agrona.UnsafeAccess;

public abstract class OverrideHistogram extends CassandraHistogram
{
    private static final CassandraReservoir NO_RESERVOIR = new CassandraReservoir() {
        @Override public Snapshot getPercentileSnapshot() { return null; }
        @Override public long[] buckets(int length) { return new long[0]; }
        @Override public BucketStrategy bucketStrategy() { return BucketStrategy.none; }
        @Override public int size() { return 0; }
        @Override public void update(long value) {}
        @Override public Snapshot getSnapshot() { return null; }
    };

    protected OverrideHistogram()
    {
        super(NO_RESERVOIR);
        try
        {
            UnsafeAccess.UNSAFE.putObject(this, UnsafeAccess.UNSAFE.objectFieldOffset(CassandraHistogram.class.getDeclaredField("count")), null);
        }
        catch (Throwable t)
        {
            // best effort, don't worry if this fails
        }
    }

    public abstract CassandraReservoir.BucketStrategy bucketStrategy();
    public abstract long[] bucketStarts(int length);
}
