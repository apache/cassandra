/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/

package org.apache.cassandra.io.sstable;

import java.io.BufferedInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.commitlog.ReplayPosition;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.EstimatedHistogram;

/**
 * Metadata for a SSTable.
 * Metadata includes:
 *  - estimated row size histogram
 *  - estimated column count histogram
 *  - replay position
 *  - max column timestamp
 *
 * An SSTableMetadata should be instantiated via the Collector, openFromDescriptor()
 * or createDefaultInstance()
 */
public class SSTableMetadata
{
    private static Logger logger = LoggerFactory.getLogger(SSTableMetadata.class);
    protected final EstimatedHistogram estimatedRowSize;
    protected final EstimatedHistogram estimatedColumnCount;
    protected final ReplayPosition replayPosition;
    protected final long maxTimestamp;
    public static final SSTableMetadataSerializer serializer = new SSTableMetadataSerializer();

    private SSTableMetadata()
    {
        this(defaultRowSizeHistogram(), defaultColumnCountHistogram(), ReplayPosition.NONE);
    }

    // when there is no max timestamp recorded, default to max long
    private SSTableMetadata(EstimatedHistogram rowSizes, EstimatedHistogram columnCounts, ReplayPosition replayPosition)
    {
        this(rowSizes, columnCounts, replayPosition, Long.MAX_VALUE);
    }

    private SSTableMetadata(EstimatedHistogram rowSizes, EstimatedHistogram columnCounts, ReplayPosition replayPosition, long maxTimestamp)
    {
        this.estimatedRowSize = rowSizes;
        this.estimatedColumnCount = columnCounts;
        this.replayPosition = replayPosition;
        this.maxTimestamp = maxTimestamp;
    }

    public static SSTableMetadata createDefaultInstance()
    {
        return new SSTableMetadata();
    }

    public static Collector createCollector()
    {
        return new Collector();
    }

    public EstimatedHistogram getEstimatedRowSize()
    {
        return estimatedRowSize;
    }

    public EstimatedHistogram getEstimatedColumnCount()
    {
        return estimatedColumnCount;
    }

    public ReplayPosition getReplayPosition()
    {
        return replayPosition;
    }

    public long getMaxTimestamp()
    {
        return maxTimestamp;
    }

    static EstimatedHistogram defaultColumnCountHistogram()
    {
        // EH of 114 can track a max value of 2395318855, i.e., > 2B columns
        return new EstimatedHistogram(114);
    }

    static EstimatedHistogram defaultRowSizeHistogram()
    {
        // EH of 150 can track a max value of 1697806495183, i.e., > 1.5PB
        return new EstimatedHistogram(150);
    }

    public static class Collector
    {
        protected EstimatedHistogram estimatedRowSize;
        protected EstimatedHistogram estimatedColumnCount;
        protected ReplayPosition replayPosition;
        protected long maxTimestamp;

        private Collector()
        {
            this.estimatedRowSize = defaultRowSizeHistogram();
            this.estimatedColumnCount = defaultColumnCountHistogram();
            this.replayPosition = ReplayPosition.NONE;
            this.maxTimestamp = Long.MIN_VALUE;
        }

        public void addRowSize(long rowSize)
        {
            estimatedRowSize.add(rowSize);
        }

        public void addColumnCount(long columnCount)
        {
            estimatedColumnCount.add(columnCount);
        }

        public void updateMaxTimestamp(long potentialMax)
        {
            maxTimestamp = Math.max(maxTimestamp, potentialMax);
        }

        public SSTableMetadata finalizeMetadata()
        {
            return new SSTableMetadata(estimatedRowSize, estimatedColumnCount, replayPosition, maxTimestamp);
        }

        public Collector estimatedRowSize(EstimatedHistogram estimatedRowSize)
        {
            this.estimatedRowSize = estimatedRowSize;
            return this;
        }

        public Collector estimatedColumnCount(EstimatedHistogram estimatedColumnCount)
        {
            this.estimatedColumnCount = estimatedColumnCount;
            return this;
        }

        public Collector replayPosition(ReplayPosition replayPosition)
        {
            this.replayPosition = replayPosition;
            return this;
        }
    }

    public static class SSTableMetadataSerializer implements ISerializer<SSTableMetadata>
    {
        private static final Logger logger = LoggerFactory.getLogger(SSTableMetadataSerializer.class);

        public void serialize(SSTableMetadata sstableStats, DataOutput dos) throws IOException
        {
            EstimatedHistogram.serializer.serialize(sstableStats.getEstimatedRowSize(), dos);
            EstimatedHistogram.serializer.serialize(sstableStats.getEstimatedColumnCount(), dos);
            ReplayPosition.serializer.serialize(sstableStats.getReplayPosition(), dos);
            dos.writeLong(sstableStats.getMaxTimestamp());
        }

        public SSTableMetadata deserialize(Descriptor descriptor) throws IOException
        {
            File statsFile = new File(descriptor.filenameFor(SSTable.COMPONENT_STATS));
            if (!statsFile.exists())
            {
                logger.debug("No sstable stats for {}", descriptor);
                return new SSTableMetadata();
            }

            DataInputStream dis = null;
            try
            {
                logger.debug("Load metadata for {}", descriptor);
                dis = new DataInputStream(new BufferedInputStream(new FileInputStream(statsFile)));

                if (!descriptor.usesHistogramAndReplayPositionStatsFile)
                  return deserialize(dis);

                EstimatedHistogram rowSizes = EstimatedHistogram.serializer.deserialize(dis);
                EstimatedHistogram columnCounts = EstimatedHistogram.serializer.deserialize(dis);
                ReplayPosition replayPosition = descriptor.hasReplayPosition()
                                              ? ReplayPosition.serializer.deserialize(dis)
                                              : ReplayPosition.NONE;

                return new SSTableMetadata(rowSizes, columnCounts, replayPosition);
            }
            finally
            {
                FileUtils.closeQuietly(dis);
            }
        }

        public SSTableMetadata deserialize(DataInput dis) throws IOException
        {
            EstimatedHistogram rowSizes = EstimatedHistogram.serializer.deserialize(dis);
            EstimatedHistogram columnCounts = EstimatedHistogram.serializer.deserialize(dis);
            ReplayPosition replayPosition = ReplayPosition.serializer.deserialize(dis);
            long maxTimestamp = dis.readLong();
            return new SSTableMetadata(rowSizes, columnCounts, replayPosition, maxTimestamp);
        }

        public long serializedSize(SSTableMetadata object)
        {
            throw new UnsupportedOperationException();
        }
    }
}
