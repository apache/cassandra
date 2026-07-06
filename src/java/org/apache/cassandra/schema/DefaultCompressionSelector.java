/*
 * Copyright IBM Corp.
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
package org.apache.cassandra.schema;

import java.util.Optional;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.compress.ICompressor;

/**
 * The built-in, cluster-wide {@link CompressionParams.Selector} used when no custom selector class is configured.
 *
 * <p>It applies the following policy:
 * <ul>
 *   <li><b>Compressor for newly created tables</b> – always {@link CompressionParams#FAST} (LZ4).</li>
 *   <li><b>Flush compression</b> – derives a write-optimised scheme from the
 *       {@link org.apache.cassandra.config.Config.FlushCompression flush_compression} cassandra.yaml setting:
 *       <ul>
 *         <li>{@code none} – disables compression on flush SSTables ({@link CompressionParams#NOOP}).</li>
 *         <li>{@code fast} – uses {@link CompressionParams#FAST} (LZ4) unless the table's own compressor
 *             already supports fast compression, in which case the table params are preserved.</li>
 *         <li>{@code adaptive} – uses {@link CompressionParams#FAST_ADAPTIVE} (adaptive LZ4) unless the
 *             table's own compressor already supports fast compression.</li>
 *         <li>{@code table} (default) – honours the table's configured compression params, preferring
 *             any fast-compression variant they expose.</li>
 *       </ul>
 *   </li>
 *   <li><b>Compaction compression</b> – always defers to the table's own {@link CompressionParams}
 *       unchanged.</li>
 * </ul>
 *
 * <p>A different implementation can be substituted at startup by setting the
 * {@code cassandra.sstable.compression.selector.class} system property (see
 * {@link CompressionParams.Selector#fromProperty()}).
 */
public class DefaultCompressionSelector implements CompressionParams.Selector
{
    @Override
    public CompressionParams newTableCompression(String keyspace)
    {
        return CompressionParams.FAST;
    }

    @Override
    public CompressionParams flushCompression(String keyspace, CompressionParams tableParams)
    {
        final ICompressor compressor = tableParams.getSstableCompressor();
        if (compressor == null)
            return tableParams;

        switch (DatabaseDescriptor.getFlushCompression())
        {
            case none:
                return CompressionParams.NOOP;
            case fast:
                if (!compressor.recommendedUses().contains(ICompressor.Uses.FAST_COMPRESSION))
                    return CompressionParams.FAST;
                // else fall through
            case adaptive:
                if (!compressor.recommendedUses().contains(ICompressor.Uses.FAST_COMPRESSION))
                    return CompressionParams.FAST_ADAPTIVE;
                // else fall through
            case table:
            default:
                return Optional.ofNullable(tableParams.forUse(ICompressor.Uses.FAST_COMPRESSION))
                               .orElse(tableParams);
        }
    }

    @Override
    public CompressionParams compactionCompression(String keyspace, CompressionParams tableParams)
    {
        return tableParams;
    }
}
