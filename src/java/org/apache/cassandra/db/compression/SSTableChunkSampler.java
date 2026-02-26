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

package org.apache.cassandra.db.compression;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.compression.ICompressionDictionaryTrainer.TrainingStatus;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.ChannelProxy;
import org.apache.cassandra.utils.ChecksumType;

/**
 * Samples uncompressed chunks from existing SSTables for dictionary training.
 * Uses random sampling to locate the chunk offsets to avoid sequential scanning while ensuring representative samples.
 * Supports both compressed and uncompressed SSTables.
 */
public class SSTableChunkSampler
{
    private static final Logger logger = LoggerFactory.getLogger(SSTableChunkSampler.class);

    /**
     * Information about an SSTable and its chunks for sampling.
     */
    static class SSTableChunkInfo
    {
        final SSTableReader sstable;
        final CompressionMetadata metadata; // null for uncompressed
        final long chunkCount;
        final long dataLength;
        final int chunkSize;
        final boolean isCompressed;

        SSTableChunkInfo(SSTableReader sstable, CompressionDictionaryTrainingConfig config)
        {
            this.sstable = sstable;
            this.isCompressed = sstable.compression;

            if (isCompressed)
            {
                this.metadata = sstable.getCompressionMetadata();
                this.dataLength = metadata.dataLength;
                this.chunkSize = metadata.chunkLength();
                // Use the logical chunk count from metadata (each offset is 8 bytes)
                this.chunkCount = metadata.chunkOffsetsSize >> 3;
            }
            else
            {
                this.metadata = null;
                this.dataLength = sstable.uncompressedLength();
                this.chunkSize = config.chunkSize;
                // Calculate number of chunks for uncompressed: dataLength divided by chunkSize, rounded up
                this.chunkCount = (dataLength + chunkSize - 1) / chunkSize;
            }
        }
    }

    /**
     * Samples chunks from existing SSTables and adds them to the trainer.
     * Uses two-level sampling to avoid memory issues with large datasets:
     * 1. Select SSTables (potentially all, weighted by size)
     * 2. For each SSTable, randomly select specific chunks to sample
     *
     * @param sstables the set of SSTables to sample from
     * @param trainer  the trainer to add samples to
     * @param config   the training configuration with sample size limits
     */
    public static void sampleFromSSTables(List<SSTableReader> sstables,
                                          ICompressionDictionaryTrainer trainer,
                                          CompressionDictionaryTrainingConfig config) throws IOException
    {
        if (sstables.isEmpty())
        {
            throw new IllegalArgumentException("No SSTables provided for sampling");
        }

        TrainingStatus status = trainer.getTrainingState().status;
        if (status != TrainingStatus.SAMPLING)
        {
            throw new IllegalStateException("Trainer is not ready to accept samples. Current status: " + status);
        }

        // Build metadata for all SSTables
        List<SSTableChunkInfo> sstableInfos = buildSSTableInfos(sstables, config);
        long totalChunks = sstableInfos.stream().mapToLong(info -> info.chunkCount).sum();

        // Calculate how many chunks to sample
        long targetChunkCount = calculateTargetChunkCount(sstableInfos, totalChunks, config);

        logger.debug("Target chunk count for sampling: {} (max sample size: {} bytes)",
                     targetChunkCount, config.maxTotalSampleSize);

        // Sample chunks from each SSTable
        SamplingStats stats = sampleChunksFromSSTables(sstableInfos, totalChunks, targetChunkCount, trainer, config);

        logger.info("Completed sampling: {} chunks, total size: {} bytes", stats.sampleCount, stats.totalSampleSize);
    }

    /**
     * Builds SSTableChunkInfo objects for all SSTables and logs statistics.
     */
    static List<SSTableChunkInfo> buildSSTableInfos(List<SSTableReader> sstables,
                                                    CompressionDictionaryTrainingConfig config)
    {
        List<SSTableChunkInfo> sstableInfos = new ArrayList<>();
        long totalChunks = 0;
        int compressedCount = 0;
        int uncompressedCount = 0;

        for (SSTableReader sstable : sstables)
        {
            SSTableChunkInfo info = new SSTableChunkInfo(sstable, config);
            sstableInfos.add(info);
            totalChunks += info.chunkCount;

            if (info.isCompressed)
                compressedCount++;
            else
                uncompressedCount++;
        }

        logger.info("Sampling from {} SSTables ({} compressed, {} uncompressed) with {} total chunks",
                    sstableInfos.size(), compressedCount, uncompressedCount, totalChunks);

        return sstableInfos;
    }

    /**
     * Calculates the target number of chunks to sample based on available data and constraints.
     */
    static long calculateTargetChunkCount(List<SSTableChunkInfo> sstableInfos,
                                          long totalChunks,
                                          CompressionDictionaryTrainingConfig config)
    {
        long totalDataSize = sstableInfos.stream().mapToLong(info -> info.dataLength).sum();
        int averageChunkSize = totalDataSize > 0 ? (int) (totalDataSize / totalChunks) : config.chunkSize;
        return config.maxTotalSampleSize / averageChunkSize;
    }

    /**
     * Result of sampling operation containing statistics.
     */
    static class SamplingStats
    {
        final long sampleCount;
        final long totalSampleSize;

        SamplingStats(long sampleCount, long totalSampleSize)
        {
            this.sampleCount = sampleCount;
            this.totalSampleSize = totalSampleSize;
        }
    }

    /**
     * Samples chunks from all SSTables proportionally to their size.
     * Each SSTable contributes samples in proportion to its chunk count relative to the total.
     * Stops early if either the target chunk count or max total sample size limit is reached.
     * <p>
     * For example,
     * <pre>
     * Given:
     *   - SSTable A: 40 chunks, chunkSize=64KB (40% of total)
     *   - SSTable B: 60 chunks, chunkSize=64KB (60% of total)
     *   - Target chunk count: 100
     *   - Max total sample size: 5MB
     *
     * Result:
     *   - Sample 32 chunks from A (5MiB / 64KiB * 0.4 = 32 chunks)
     *   - Sample 48 chunks from B (5MiB / 64KiB * 0.6 = 48 chunks)
     *   - Total sampled: 80 chunks (stopped due to size limit, not target)
     * </pre>
     */
    static SamplingStats sampleChunksFromSSTables(List<SSTableChunkInfo> sstableInfos,
                                                  long totalChunks,
                                                  long targetChunkCount,
                                                  ICompressionDictionaryTrainer trainer,
                                                  CompressionDictionaryTrainingConfig config) throws IOException
    {
        long totalSampleSize = 0;
        long sampleCount = 0;

        for (SSTableChunkInfo info : sstableInfos)
        {
            if (sampleCount >= targetChunkCount || totalSampleSize >= config.maxTotalSampleSize)
            {
                break;
            }

            // Calculate how many chunks to sample from this SSTable (proportional to its size)
            long remainingTarget = Math.min(targetChunkCount - sampleCount, (config.maxTotalSampleSize - totalSampleSize) / info.chunkSize);
            long chunksFromThisSSTable = Math.min((targetChunkCount * info.chunkCount) / totalChunks, remainingTarget);

            if (chunksFromThisSSTable <= 0)
            {
                continue;
            }

            // Sample chunks from this SSTable
            SamplingStats sstableStats = sampleChunksFromSSTable(info, chunksFromThisSSTable, trainer, config);
            totalSampleSize += sstableStats.totalSampleSize;
            sampleCount += sstableStats.sampleCount;

            if (sampleCount % 100 == 0)
            {
                logger.debug("Sampled {} chunks, total size: {} bytes", sampleCount, totalSampleSize);
            }
        }

        return new SamplingStats(sampleCount, totalSampleSize);
    }

    /**
     * Samples a specified number of chunks from a single SSTable.
     */
    static SamplingStats sampleChunksFromSSTable(SSTableChunkInfo info,
                                                 long chunksToSample,
                                                 ICompressionDictionaryTrainer trainer,
                                                 CompressionDictionaryTrainingConfig config) throws IOException
    {
        long totalSampleSize = 0;
        long sampleCount = 0;

        // Generate random chunk indices for this SSTable (without building full list)
        Set<Long> selectedIndices = selectRandomChunkIndices(info.chunkCount, chunksToSample);

        // Sample the selected chunks
        for (long chunkIndex : selectedIndices)
        {
            if (totalSampleSize >= config.maxTotalSampleSize)
            {
                logger.debug("Reached max total sample size limit");
                break;
            }

            long position = chunkIndex * info.chunkSize;
            ByteBuffer chunk = readChunk(info, position);

            // Check if adding this sample would exceed the max total sample size
            if (totalSampleSize + chunk.remaining() > config.maxTotalSampleSize)
            {
                logger.debug("Next chunk would exceed max total sample size limit");
                break;
            }

            trainer.addSample(chunk);
            totalSampleSize += chunk.remaining();
            sampleCount++;
        }

        return new SamplingStats(sampleCount, totalSampleSize);
    }

    /**
     * Selects random chunk indices.
     *
     * @param totalChunks the total number of chunks available
     * @param count       the number of chunks to select
     * @return set of randomly selected chunk indices
     */
    static Set<Long> selectRandomChunkIndices(long totalChunks, long count)
    {
        // If we need to sample more than half, it's more efficient to select what to exclude
        if (count > totalChunks / 2)
        {
            long excludeCount = totalChunks - count;
            Set<Long> toExclude = floydRandomSampling(totalChunks, excludeCount);
            Set<Long> selected = new HashSet<>();
            // Add all indices except those in toExclude
            for (long i = 0; i < totalChunks; i++)
            {
                if (!toExclude.contains(i))
                {
                    selected.add(i);
                }
            }
            return selected;
        }
        else
        {
            return floydRandomSampling(totalChunks, count);
        }
    }

    /**
     * Floyd's algorithm for random sampling without replacement.
     * Efficiently selects a random subset by iterating only through the sample size, not the total population.
     * Guarantees no duplication.
     *
     * @param total   the total number of items available
     * @param samples the number of items to select
     * @return set of randomly selected indices
     * @see <a href="https://fermatslibrary.com/s/a-sample-of-brilliance">Floyd's Sampling Algorithm</a>
     */
    static Set<Long> floydRandomSampling(long total, long samples)
    {
        Set<Long> set = new HashSet<>();
        long requested = Math.min(total, samples);
        for (long i = total - requested; i < total; i++)
        {
            long randomIndex = ThreadLocalRandom.current().nextLong(i + 1);
            if (!set.add(randomIndex))
            {
                set.add(i);
            }
        }
        return set;
    }

    /**
     * Reads a chunk from an SSTable at the given position.
     * Handles both compressed and uncompressed SSTables.
     *
     * @param sstableInfo the SSTable info
     * @param position    the position to read from
     * @return the chunk data (uncompressed if source was compressed)
     * @throws IOException if reading or decompression fails
     */
    static ByteBuffer readChunk(SSTableChunkInfo sstableInfo, long position) throws IOException
    {
        if (sstableInfo.isCompressed)
        {
            return readAndDecompressChunk(sstableInfo, position);
        }
        else
        {
            return readUncompressedChunk(sstableInfo, position);
        }
    }

    /**
     * Reads and decompresses a single chunk from a compressed SSTable.
     *
     * @param sstableInfo the SSTable info
     * @param position    the uncompressed position (will be mapped to chunk)
     * @return the uncompressed chunk data
     * @throws IOException if reading or decompression fails
     */
    static ByteBuffer readAndDecompressChunk(SSTableChunkInfo sstableInfo, long position) throws IOException
    {
        CompressionMetadata metadata = sstableInfo.metadata;
        CompressionMetadata.Chunk chunk = metadata.chunkFor(position);

        // Read the compressed chunk from disk
        ChannelProxy channel = sstableInfo.sstable.getDataChannel();

        // Allocate buffer for compressed data + checksum
        int compressedLength = chunk.length;
        ByteBuffer compressed = ByteBuffer.allocateDirect(compressedLength + Integer.BYTES);

        int read = channel.read(compressed, chunk.offset);
        if (read != compressedLength + Integer.BYTES)
        {
            throw new IOException(String.format("Expected to read %d bytes but got %d",
                                                compressedLength + Integer.BYTES, read));
        }

        compressed.flip();
        compressed.limit(compressedLength);

        // Verify checksum
        int expectedChecksum = (int) ChecksumType.CRC32.of(compressed);
        compressed.limit(compressedLength + Integer.BYTES);
        int actualChecksum = compressed.getInt(compressedLength);

        if (expectedChecksum != actualChecksum)
        {
            throw new IOException(String.format("Checksum mismatch for chunk at position %d in SSTable %s (expected: %d, actual: %d)",
                                                position, sstableInfo.sstable, expectedChecksum, actualChecksum));
        }

        // Reset for decompression
        compressed.position(0).limit(compressedLength);

        // Decompress the chunk
        ICompressor compressor = metadata.compressor();
        ByteBuffer uncompressed = ByteBuffer.allocateDirect(metadata.chunkLength());

        compressor.uncompress(compressed, uncompressed);
        uncompressed.flip();
        return uncompressed;
    }

    /**
     * Reads a chunk directly from an uncompressed SSTable.
     *
     * @param sstableInfo the SSTable info
     * @param position    the position to read from
     * @return the chunk data
     * @throws IOException if reading fails
     */
    static ByteBuffer readUncompressedChunk(SSTableChunkInfo sstableInfo, long position) throws IOException
    {
        ChannelProxy channel = sstableInfo.sstable.getDataChannel();

        // Calculate how much to read (might be less than chunkSize at end of file)
        long remainingData = sstableInfo.dataLength - position;
        int readSize = (int) Math.min(sstableInfo.chunkSize, remainingData);

        if (readSize <= 0)
        {
            throw new IOException(String.format("Invalid read size %d at position %d (dataLength: %d) for SSTable %s",
                                                readSize, position, sstableInfo.dataLength, sstableInfo.sstable));
        }

        ByteBuffer buffer = ByteBuffer.allocateDirect(readSize);
        int read = channel.read(buffer, position);

        if (read != readSize)
        {
            throw new IOException(String.format("Expected to read %d bytes but got %d", readSize, read));
        }

        buffer.flip();
        return buffer;
    }
}
