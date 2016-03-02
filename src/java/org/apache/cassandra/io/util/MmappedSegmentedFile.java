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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.sstable.format.Version;

public class MmappedSegmentedFile extends SegmentedFile
{
    private static final Logger logger = LoggerFactory.getLogger(MmappedSegmentedFile.class);

    public MmappedSegmentedFile(ChannelProxy channel, long length, MmappedRegions regions)
    {
        this(channel, new MmapRebufferer(channel, length, regions), length);
    }

    public MmappedSegmentedFile(ChannelProxy channel, RebuffererFactory rebufferer, long length)
    {
        super(new Cleanup(channel, rebufferer), channel, rebufferer, length);
    }

    private MmappedSegmentedFile(MmappedSegmentedFile copy)
    {
        super(copy);
    }

    public MmappedSegmentedFile sharedCopy()
    {
        return new MmappedSegmentedFile(this);
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

            return new MmappedSegmentedFile(channel, length, regions.sharedCopy());
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
