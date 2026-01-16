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

import accord.utils.Gen;
import accord.utils.Gens;

import org.apache.cassandra.schema.CompressionParams;

public abstract class CompressedChunkReaderTestBase
{

    static Gen<SequentialWriterOption> writerOptions()
    {
        Gen<Integer> bufferSizes = Gens.constant(1 << 10);
        return rs -> writerOption(bufferSizes.next(rs));
    }

    static SequentialWriterOption writerOption(int bufferSize)
    {
        return SequentialWriterOption.newBuilder()
                                     .finishOnClose(false)
                                     .bufferSize(bufferSize)
                                     .build();
    }

    private enum CompressionKind { Noop, Snappy, Deflate, Lz4, Zstd }

    static Gen<CompressionParams> compressionParams(Gen<Integer> chunkLengths)
    {
        Gen<Double> compressionRatio = Gens.pick(1.1D);
        return rs -> {
            CompressionKind kind = rs.pick(CompressionKind.values());
            switch (kind)
            {
                case Noop: return CompressionParams.noop();
                case Snappy: return CompressionParams.snappy(chunkLengths.next(rs), compressionRatio.next(rs));
                case Deflate: return CompressionParams.deflate(chunkLengths.next(rs));
                case Lz4: return CompressionParams.lz4(chunkLengths.next(rs));
                case Zstd: return CompressionParams.zstd(chunkLengths.next(rs));
                default: throw new UnsupportedOperationException(kind.name());
            }
        };
    }
}
