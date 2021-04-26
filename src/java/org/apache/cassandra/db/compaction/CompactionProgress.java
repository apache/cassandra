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

package org.apache.cassandra.db.compaction;

import java.util.Collection;

import javax.annotation.Nullable;

import org.apache.cassandra.io.sstable.format.SSTableReader;

/**
 * The progress information for a compaction operation. This adds compaction
 * specific information to {@link TableOperation.Progress}.
 */
public interface CompactionProgress extends TableOperation.Progress
{
    /**
     * The compaction strategy if available, otherwise null.
     * <p/>
     * The compaction strategy may not be available for some operations that use compaction task such
     * as GC or sstable splitting.
     *
     * @return the compaction strategy when available or null.
     */
    @Nullable AbstractCompactionStrategy strategy();

    /**
     * @return true if the compaction was requested to interrupt
     */
    boolean isStopRequested();

    /**
     * @return input sstables
     */
    Collection<SSTableReader> inSSTables();

    /**
     * @return output sstables
     */
    Collection<SSTableReader> outSSTables();

    /**
     * @return Size on disk (compressed) of the input sstables.
     */
    long inputDiskSize();

    /**
     * @return The uncompressed size of the input sstables.
     */
    long inputUncompressedSize();

    /** Same as {@link this#inputDiskSize()} except for LCS where it estimates
     * the compressed size for number of keys that will be read from the input sstables,
     * see {@link org.apache.cassandra.db.compaction.LeveledCompactionStrategy}. */
    long adjustedInputDiskSize();

    /**
     * @return Size on disk (compressed) of the output sstables.
     */
    long outputDiskSize();

    /**
     * @return the number of bytes processed by the compaction iterator. For compressed or encrypted sstables,
     *         this is the number of bytes processed by the iterator after decompression, so this is the current
     *         position in the uncompressed sstable files.
     */
    long uncompressedBytesRead();

    /**
     * @return the number of bytes processed by the compaction iterator for sstables on the specified level.
     *         For compressed or encrypted sstables, this is the number of bytes processed by the iterator after decompression,
     *         so this is the current position in the uncompressed sstable files.
     */
    long uncompressedBytesRead(int level);

    /**
     * @return the number of bytes that were written before compression is applied (uncompressed size).
     */
    long uncompressedBytesWritten();

    /**
     * @return the duration so far in nanoseconds.
     */
    long durationInNanos();

    /**
     * @return total number of partitions read
     */
    long partitionsRead();

    /**
     * @return otal number of rows read
     */
    long rowsRead();

    /**
     * The partitions histogram maps the number of sstables to the number of partitions that were merged with that number of input sstables.
     *
     * @return the partitions histogram
     */
    long[] partitionsHistogram();

    /**
     * The rows histogram maps the number of sstables to the number of rows that were merged with that number of input sstables.
     *
     * @return the rows histogram
     */
    long[] rowsHistogram();

    /**
     * @return the ratio of bytes before and after compaction, using the adjusted input and output disk sizes (uncompressed values).
     */
    double sizeRatio();
}
