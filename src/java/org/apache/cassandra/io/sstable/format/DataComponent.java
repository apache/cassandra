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

package org.apache.cassandra.io.sstable.format;

import java.util.EnumMap;

import org.apache.cassandra.config.Config.DiskAccessMode;
import org.apache.cassandra.config.Config.FlushCompression;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.compression.CompressionDictionaryManager;
import org.apache.cassandra.io.DirectIoSupport;
import org.apache.cassandra.io.compress.CompressedSequentialWriter;
import org.apache.cassandra.io.compress.DirectCompressedSequentialWriter;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableFormat.Components;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.util.ChecksummedSequentialWriter;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.io.util.SequentialWriterOption;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.schema.TableMetadata;

import static org.apache.cassandra.io.DirectIoSupport.SUPPORTED;
import static org.apache.cassandra.io.DirectIoSupport.UNSUPPORTED_CORRECTNESS;
import static org.apache.cassandra.io.DirectIoSupport.UNSUPPORTED_POLICY;

public class DataComponent
{

    private static final EnumMap<OperationType, DirectIoSupport> DIRECT_WRITE_SUPPORT = buildDirectWriteSupport();

    private static EnumMap<OperationType, DirectIoSupport> buildDirectWriteSupport()
    {
        EnumMap<OperationType, DirectIoSupport> m = new EnumMap<>(OperationType.class);

        // append()-only writers; safe under O_DIRECT.
        m.put(OperationType.CLEANUP, SUPPORTED);
        m.put(OperationType.UPGRADE_SSTABLES, SUPPORTED);
        m.put(OperationType.MAJOR_COMPACTION, SUPPORTED);
        m.put(OperationType.GARBAGE_COLLECT, SUPPORTED);
        m.put(OperationType.WRITE, SUPPORTED);
        m.put(OperationType.ANTICOMPACTION, SUPPORTED);
        m.put(OperationType.COMPACTION, SUPPORTED);
        m.put(OperationType.TOMBSTONE_COMPACTION, SUPPORTED);
        m.put(OperationType.STREAM, SUPPORTED);

        // tryAppend() needs mark()/resetAndTruncate(), unsupported under O_DIRECT.
        m.put(OperationType.SCRUB, UNSUPPORTED_CORRECTNESS);

        // Flushed data is hot; keep it in the page cache.
        m.put(OperationType.FLUSH, UNSUPPORTED_POLICY);

        for (OperationType op : OperationType.values())
            if (op.writesData && !m.containsKey(op))
                throw new AssertionError("Missing direct-write classification for " + op);
        return m;
    }

    public static SequentialWriter buildWriter(Descriptor descriptor,
                                               TableMetadata metadata,
                                               SequentialWriterOption options,
                                               MetadataCollector metadataCollector,
                                               OperationType operationType,
                                               FlushCompression flushCompression,
                                               CompressionDictionaryManager compressionDictionaryManager)
    {
        if (metadata.params.compression.isEnabled())
        {
            CompressionParams compressionParams = buildCompressionParams(metadata, operationType, flushCompression);

            if (DatabaseDescriptor.getBackgroundWriteDiskAccessMode() == DiskAccessMode.direct
                && isDirectWriteSupported(operationType))
            {
                return new DirectCompressedSequentialWriter(descriptor.fileFor(Components.DATA),
                                                            descriptor.fileFor(Components.COMPRESSION_INFO),
                                                            descriptor.fileFor(Components.DIGEST),
                                                            options,
                                                            compressionParams,
                                                            metadataCollector,
                                                            compressionDictionaryManager);
            }
            else
            {
                return new CompressedSequentialWriter(descriptor.fileFor(Components.DATA),
                                                      descriptor.fileFor(Components.COMPRESSION_INFO),
                                                      descriptor.fileFor(Components.DIGEST),
                                                      options,
                                                      compressionParams,
                                                      metadataCollector,
                                                      compressionDictionaryManager);
            }
        }
        else
        {
            return new ChecksummedSequentialWriter(descriptor.fileFor(Components.DATA),
                                                   descriptor.fileFor(Components.CRC),
                                                   descriptor.fileFor(Components.DIGEST),
                                                   options);
        }
    }

    /**
     * Given an OpType, determine the correct Compression Parameters
     *
     * @return {@link CompressionParams}
     */
    private static CompressionParams buildCompressionParams(TableMetadata metadata, OperationType operationType, FlushCompression flushCompression)
    {
        CompressionParams compressionParams = metadata.params.compression;
        final ICompressor compressor = compressionParams.getSstableCompressor();

        if (null != compressor && operationType == OperationType.FLUSH)
        {
            // When we are flushing out of the memtable throughput of the compressor is critical as flushes,
            // especially of large tables, can queue up and potentially block writes.
            // This optimization allows us to fall back to a faster compressor if a particular
            // compression algorithm indicates we should. See CASSANDRA-15379 for more details.
            switch (flushCompression)
            {
                // It is relatively easier to insert a Noop compressor than to disable compressed writing
                // entirely as the "compression" member field is provided outside the scope of this class.
                // It may make sense in the future to refactor the ownership of the compression flag so that
                // We can bypass the CompressedSequentialWriter in this case entirely.
                case none:
                    compressionParams = CompressionParams.NOOP;
                    break;
                case fast:
                    if (!compressor.recommendedUses().contains(ICompressor.Uses.FAST_COMPRESSION))
                    {
                        // The default compressor is generally fast (LZ4 with 16KiB block size)
                        compressionParams = CompressionParams.DEFAULT;
                        break;
                    }
                    // else fall through
                case table:
                default:
                    break;
            }
        }
        return compressionParams;
    }

    private static boolean isDirectWriteSupported(OperationType operationType)
    {
        DirectIoSupport support = DIRECT_WRITE_SUPPORT.get(operationType);
        if (support == null)
            throw new IllegalArgumentException("OperationType " + operationType
                                               + " has no direct-write classification — likely a read-only operation routed through buildWriter()");
        return support.isSupported();
    }

}
