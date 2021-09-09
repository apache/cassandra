/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.db.compaction.unified;

import java.util.concurrent.TimeUnit;

import org.apache.cassandra.cache.ChunkCache;
import org.apache.cassandra.db.compaction.CompactionRealm;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.ExpMovingAverage;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MovingAverage;
import org.apache.cassandra.utils.PageAware;

/**
 * An implementation of {@link Environment} that returns
 * real values.
 */
class RealEnvironment implements Environment
{
    private final CompactionRealm realm;

    RealEnvironment(CompactionRealm realm)
    {
        this.realm = realm;
    }

    private TableMetrics metrics()
    {
        return realm.metrics();
    }

    @Override
    public MovingAverage makeExpMovAverage()
    {
        return ExpMovingAverage.decayBy100();
    }

    @Override
    public double cacheMissRatio()
    {
        double hitRate = ChunkCache.instance.metrics.hitRate();
        if (Double.isNaN(hitRate))
            return 1; // if the cache is not yet initialized then assume all requests are a cache miss

        return 1 - Math.min(1, hitRate); // hit rate should never be > 1 but just in case put a check
    }

    @Override
    public double bloomFilterFpRatio()
    {
        return metrics().bloomFilterFalseRatio.getValue();
    }

    @Override
    public int chunkSize()
    {
        CompressionParams compressionParams = realm.metadata().params.compression;
        if (compressionParams.isEnabled())
            return compressionParams.chunkLength();

        return PageAware.PAGE_SIZE;
    }

    @Override
    public long partitionsRead()
    {
        return metrics().readRequests.getCount();
    }

    @Override
    public double sstablePartitionReadLatencyNanos()
    {
        return metrics().sstablePartitionReadLatency.get();
    }

    @Override
    public double compactionTimePerKbInNanos()
    {
        return metrics().compactionTimePerKb.get();
    }

    @Override
    public double flushTimePerKbInNanos()
    {
        return metrics().flushTimePerKb.get();
    }

    @Override
    public long bytesInserted()
    {
        return metrics().bytesInserted.getCount();
    }

    @Override
    public double WA()
    {
        return realm.getWA();
    }

    @Override
    public double flushSize()
    {
        return metrics().flushSizeOnDisk.get();
    }

    @Override
    public String toString()
    {
        return String.format("Read latency: %d us / partition, flush latency: %d us / KiB, compaction latency: %d us / KiB, bfpr: %f, measured WA: %.2f, flush size %s",
                             TimeUnit.NANOSECONDS.toMicros((long) sstablePartitionReadLatencyNanos()),
                             TimeUnit.NANOSECONDS.toMicros((long) flushTimePerKbInNanos()),
                             TimeUnit.NANOSECONDS.toMicros((long) compactionTimePerKbInNanos()),
                             bloomFilterFpRatio(),
                             WA(),
                             FBUtilities.prettyPrintMemory((long)flushSize()));
    }
}
