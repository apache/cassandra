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

package org.apache.cassandra.index.sai.disk.v1.sortedterms;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.index.sai.disk.v1.MetadataWriter;
import org.apache.cassandra.index.sai.disk.v1.SAICodecUtils;
import org.apache.cassandra.index.sai.disk.v1.bitpack.NumericValuesWriter;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.StringHelper;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;
import java.io.Closeable;
import java.io.IOException;

/**
 * Writes an ordered sequence of terms for use with {@link SortedTermsReader}.
 * <p>
 * Terms must be added in lexicographical ascending order.
 * Terms can be of varying lengths.
 * <p>
 * For documentation of the underlying on-disk data structures, see the package documentation.
 * <p>
 * The {@code cassandra.sai.sorted_terms_block_shift} property is used to quickly determine the id of the current block
 * based on a point id or to check if we are exactly at the beginning of the block.
 * Terms data are organized in blocks of (2 ^ {@link #blockShift}) terms.
 * The blocks should not be too small because they allow prefix compression of
 * the terms except the first term in a block.
 * The blocks should not be too large because we can't just randomly jump to the term inside the block,
 * but we have to iterate through all the terms from the start of the block.
 *
 * @see SortedTermsReader
 * @see org.apache.cassandra.index.sai.disk.v1.sortedterms
 */
@NotThreadSafe
public class SortedTermsWriter implements Closeable
{
    private final int blockShift;
    private final int blockMask;
    private final IndexOutput termsOutput;
    private final NumericValuesWriter offsetsWriter;
    private final String componentName;
    private final MetadataWriter metadataWriter;

    private BytesRefBuilder prevTerm = new BytesRefBuilder();
    private BytesRefBuilder tempTerm = new BytesRefBuilder();

    private final long bytesStartFP;

    private int maxTermLength = -1;
    private long pointId = 0;

    /**
     * Creates a new writer.
     * <p>
     * It does not own the components, so you must close the components by yourself
     * after you're done with the writer.
     *
     * @param componentName the component name for the {@link SortedTermsMeta}
     * @param metadataWriter the {@link MetadataWriter} for storing the {@link SortedTermsMeta}
     * @param termsOutput where to write the prefix-compressed terms data
     * @param termsDataBlockOffsets  where to write the offsets of each block of terms data
     */
    public SortedTermsWriter(String componentName,
                             MetadataWriter metadataWriter,
                             IndexOutput termsOutput,
                             NumericValuesWriter termsDataBlockOffsets) throws IOException
    {
        this.componentName = componentName;
        this.metadataWriter = metadataWriter;
        SAICodecUtils.writeHeader(termsOutput);
        this.blockShift = CassandraRelevantProperties.SAI_SORTED_TERMS_BLOCK_SHIFT.getInt();
        this.blockMask = (1 << this.blockShift) - 1;
        this.termsOutput = termsOutput;
        this.termsOutput.writeVInt(blockShift);
        this.bytesStartFP = termsOutput.getFilePointer();
        this.offsetsWriter = termsDataBlockOffsets;
    }

    /**
     * Appends a term at the end of the sequence.
     *
     * @throws IOException if write to disk fails
     * @throws IllegalArgumentException if the term is not greater than the previous added term
     */
    public void add(final @Nonnull ByteComparable term) throws IOException
    {
        tempTerm.clear();
        copyBytes(term, tempTerm);

        BytesRef termRef = tempTerm.get();

        writeTermData(termRef);

        maxTermLength = Math.max(maxTermLength, termRef.length);

        BytesRefBuilder temp = this.tempTerm;
        this.tempTerm = this.prevTerm;
        this.prevTerm = temp;

        pointId++;
    }

    private void writeTermData(BytesRef term) throws IOException
    {
        if ((pointId & blockMask) == 0)
        {
            offsetsWriter.add(termsOutput.getFilePointer() - bytesStartFP);

            termsOutput.writeVInt(term.length);
            termsOutput.writeBytes(term.bytes, term.offset, term.length);
        }
        else
        {
            int prefixLength = StringHelper.bytesDifference(prevTerm.get(), term);
            int suffixLength = term.length - prefixLength;
            assert suffixLength > 0: "terms must be unique";

            // The prefix and suffix lengths are written as a byte followed by up to 2 vints. An attempt is
            // made to compress the lengths into the byte (if prefix length < 15 and/or suffix length < 16).
            // If either length exceeds the compressed byte maximum, it is written as a vint following the byte.
            termsOutput.writeByte((byte) (Math.min(prefixLength, 15) | (Math.min(15, suffixLength - 1) << 4)));
            if (prefixLength >= 15)
                termsOutput.writeVInt(prefixLength - 15);
            if (suffixLength >= 16)
                termsOutput.writeVInt(suffixLength - 16);

            termsOutput.writeBytes(term.bytes, term.offset + prefixLength, term.length - prefixLength);
        }
    }

    /**
     * Flushes any in-memory buffers to the output streams.
     * Does not close the output streams.
     * No more writes are allowed.
     */
    @Override
    public void close() throws IOException
    {
        try (IndexOutput output = metadataWriter.builder(componentName))
        {
            SAICodecUtils.writeFooter(termsOutput);
            SortedTermsMeta.write(output, pointId, maxTermLength);
            // Don't close the offsets writer quietly because of the work it does
            // during its close. We need to propagate the error.
            offsetsWriter.close();
        }
        finally
        {
            FileUtils.closeQuietly(termsOutput);
        }
    }

    private void copyBytes(ByteComparable source, BytesRefBuilder dest)
    {
        ByteSource byteSource = source.asComparableBytes(ByteComparable.Version.OSS50);
        int val;
        while ((val = byteSource.next()) != ByteSource.END_OF_STREAM)
            dest.append((byte) val);
    }
}
