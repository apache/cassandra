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
package org.apache.cassandra.io.util;

import java.io.*;

import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.utils.JVMStabilityInspector;

public class MmappedSegmentedFile extends SegmentedFile
{
    private static final Logger logger = LoggerFactory.getLogger(MmappedSegmentedFile.class);

    private final MmappedRegions regions;

    public MmappedSegmentedFile(ChannelProxy channel, int bufferSize, long length, MmappedRegions regions)
    {
        super(new Cleanup(channel, regions), channel, bufferSize, length);
        this.regions = regions;
    }

    private MmappedSegmentedFile(MmappedSegmentedFile copy)
    {
        super(copy);
        this.regions = copy.regions;
    }

    public MmappedSegmentedFile sharedCopy()
    {
        return new MmappedSegmentedFile(this);
    }

    public RandomAccessReader createReader()
    {
        return new RandomAccessReader.Builder(channel)
               .overrideLength(length)
               .regions(regions)
               .build();
    }

    public RandomAccessReader createReader(RateLimiter limiter)
    {
        return new RandomAccessReader.Builder(channel)
               .overrideLength(length)
               .bufferSize(bufferSize)
               .regions(regions)
               .limiter(limiter)
               .build();
    }

    private static final class Cleanup extends SegmentedFile.Cleanup
    {
        private final MmappedRegions regions;

        Cleanup(ChannelProxy channel, MmappedRegions regions)
        {
            super(channel);
            this.regions = regions;
        }

        public void tidy()
        {
            Throwable err = regions.close(null);
            if (err != null)
            {
                JVMStabilityInspector.inspectThrowable(err);

                // This is not supposed to happen
                logger.error("Error while closing mmapped regions", err);
            }

            super.tidy();
        }
    }

    /**
     * Overrides the default behaviour to create segments of a maximum size.
     */
    static class Builder extends SegmentedFile.Builder
    {
        private MmappedRegions regions;

        Builder()
        {
            super();
        }

        public SegmentedFile complete(ChannelProxy channel, int bufferSize, long overrideLength)
        {
            long length = overrideLength > 0 ? overrideLength : channel.size();
            updateRegions(channel, length);

            return new MmappedSegmentedFile(channel, bufferSize, length, regions.sharedCopy());
        }

        private void updateRegions(ChannelProxy channel, long length)
        {
            if (regions != null && !regions.isValid(channel))
            {
                Throwable err = regions.close(null);
                if (err != null)
                    logger.error("Failed to close mapped regions", err);

                regions = null;
            }

            if (regions == null)
                regions = MmappedRegions.map(channel, length);
            else
                regions.extend(length);
        }

        @Override
        public void serializeBounds(DataOutput out, Version version) throws IOException
        {
            if (!version.hasBoundaries())
                return;

            super.serializeBounds(out, version);
            out.writeInt(0);
        }

        @Override
        public void deserializeBounds(DataInput in, Version version) throws IOException
        {
            if (!version.hasBoundaries())
                return;

            super.deserializeBounds(in, version);
            in.skipBytes(in.readInt() * TypeSizes.sizeof(0L));
        }

        @Override
        public Throwable close(Throwable accumulate)
        {
            return super.close(regions == null
                               ? accumulate
                               : regions.close(accumulate));
        }
    }
}
