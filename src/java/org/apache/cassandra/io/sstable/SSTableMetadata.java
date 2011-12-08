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
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.commitlog.ReplayPosition;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.EstimatedHistogram;

/**
 * Metadata for a SSTable.
 * Metadata includes:
 *  - estimated row size histogram
 *  - estimated column count histogram
 *  - replay position
 *  - max column timestamp
 *  - compression ratio
 *  - partitioner
 *
 * An SSTableMetadata should be instantiated via the Collector, openFromDescriptor()
 * or createDefaultInstance()
 */
public class SSTableMetadata
{
    private static Logger logger = LoggerFactory.getLogger(SSTableMetadata.class);

    public static final SSTableMetadataSerializer serializer = new SSTableMetadataSerializer();

    public final EstimatedHistogram estimatedRowSize;
    public final EstimatedHistogram estimatedColumnCount;
    public final ReplayPosition replayPosition;
    public final long maxTimestamp;
    public final double compressionRatio;
    public final String partitioner;

    private SSTableMetadata()
    {
        this(defaultRowSizeHistogram(),
             defaultColumnCountHistogram(),
             ReplayPosition.NONE,
             Long.MIN_VALUE,
             Double.MIN_VALUE,
             null);
    }

    private SSTableMetadata(EstimatedHistogram rowSizes, EstimatedHistogram columnCounts, ReplayPosition replayPosition, long maxTimestamp, double cr, String partitioner)
    {
        this.estimatedRowSize = rowSizes;
        this.estimatedColumnCount = columnCounts;
        this.replayPosition = replayPosition;
        this.maxTimestamp = maxTimestamp;
        this.compressionRatio = cr;
        this.partitioner = partitioner;
    }

    public static SSTableMetadata createDefaultInstance()
    {
        return new SSTableMetadata();
    }

    public static Collector createCollector()
    {
        return new Collector();
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
        protected EstimatedHistogram estimatedRowSize = defaultRowSizeHistogram();
        protected EstimatedHistogram estimatedColumnCount = defaultColumnCountHistogram();
        protected ReplayPosition replayPosition = ReplayPosition.NONE;
        protected long maxTimestamp = Long.MIN_VALUE;
        protected double compressionRatio = Double.MIN_VALUE;

        public void addRowSize(long rowSize)
        {
            estimatedRowSize.add(rowSize);
        }

        public void addColumnCount(long columnCount)
        {
            estimatedColumnCount.add(columnCount);
        }

        /**
         * Ratio is compressed/uncompressed and it is
         * if you have 1.x then compression isn't helping 
         */
        public void addCompressionRatio(long compressed, long uncompressed)
        {
            compressionRatio = (double) compressed/uncompressed;
        }
        
        public void updateMaxTimestamp(long potentialMax)
        {
            maxTimestamp = Math.max(maxTimestamp, potentialMax);
        }

        public SSTableMetadata finalizeMetadata(String partitioner)
        {
            return new SSTableMetadata(estimatedRowSize,
                                       estimatedColumnCount,
                                       replayPosition,
                                       maxTimestamp,
                                       compressionRatio,
                                       partitioner);
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

    public static class SSTableMetadataSerializer
    {
        private static final Logger logger = LoggerFactory.getLogger(SSTableMetadataSerializer.class);

        public void serialize(SSTableMetadata sstableStats, DataOutput dos) throws IOException
        {
            assert sstableStats.partitioner != null;

            EstimatedHistogram.serializer.serialize(sstableStats.estimatedRowSize, dos);
            EstimatedHistogram.serializer.serialize(sstableStats.estimatedColumnCount, dos);
            ReplayPosition.serializer.serialize(sstableStats.replayPosition, dos);
            dos.writeLong(sstableStats.maxTimestamp);
            dos.writeDouble(sstableStats.compressionRatio);
            dos.writeUTF(sstableStats.partitioner);
        }

        public SSTableMetadata deserialize(Descriptor descriptor) throws IOException
        {
            logger.debug("Load metadata for {}", descriptor);
            File statsFile = new File(descriptor.filenameFor(SSTable.COMPONENT_STATS));
            if (!statsFile.exists())
            {
                logger.debug("No sstable stats for {}", descriptor);
                return new SSTableMetadata();
            }

            DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(statsFile)));
            try
            {
                return deserialize(dis, descriptor);
            }
            finally
            {
                FileUtils.closeQuietly(dis);
            }
        }

        public SSTableMetadata deserialize(DataInputStream dis, Descriptor desc) throws IOException
        {
            EstimatedHistogram rowSizes = EstimatedHistogram.serializer.deserialize(dis);
            EstimatedHistogram columnCounts = EstimatedHistogram.serializer.deserialize(dis);
            ReplayPosition replayPosition = desc.metadataIncludesReplayPosition
                                          ? ReplayPosition.serializer.deserialize(dis)
                                          : ReplayPosition.NONE;
            long maxTimestamp = desc.tracksMaxTimestamp ? dis.readLong() : Long.MIN_VALUE;
            double compressionRatio = desc.hasCompressionRatio
                                    ? dis.readDouble()
                                    : Double.MIN_VALUE;
            String partitioner = desc.hasPartitioner ? dis.readUTF() : null;
            return new SSTableMetadata(rowSizes, columnCounts, replayPosition, maxTimestamp, compressionRatio, partitioner);
        }
    }
}
