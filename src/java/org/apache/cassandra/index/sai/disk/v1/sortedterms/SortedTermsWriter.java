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

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;


import org.apache.cassandra.db.memtable.TrieMemtable;
import org.apache.cassandra.db.tries.InMemoryTrie;
import org.apache.cassandra.index.sai.disk.io.IndexOutputWriter;
import org.apache.cassandra.index.sai.disk.v1.MetadataWriter;
import org.apache.cassandra.index.sai.disk.v1.bitpack.NumericValuesWriter;
import org.apache.cassandra.index.sai.disk.v1.SAICodecUtils;
import org.apache.cassandra.index.sai.utils.SegmentMemoryLimiter;
import org.apache.cassandra.io.tries.IncrementalDeepTrieWriterPageAware;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.StringHelper;

import static org.apache.cassandra.index.sai.disk.v1.trie.TrieTermsDictionaryReader.trieSerializer;

/**
 * Writes an ordered sequence of terms for use with {@link SortedTermsReader}.
 * <p>
 * Terms must be added in lexicographical ascending order.
 * Terms can be of varying lengths.
 * <p>
 * For documentation of the underlying on-disk data structures, see the package documentation.
 * <p>
 * The TERMS_DICT_ constants allow for quickly determining the id of the current block based on a point id
 * or to check if we are exactly at the beginning of the block.
 * Terms data are organized in blocks of (2 ^ {@link #TERMS_DICT_BLOCK_SHIFT}) terms.
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
    static final int TERMS_DICT_BLOCK_SHIFT = 4;
    static final int TERMS_DICT_BLOCK_SIZE = 1 << TERMS_DICT_BLOCK_SHIFT;
    static final int TERMS_DICT_BLOCK_MASK = TERMS_DICT_BLOCK_SIZE - 1;

    private final IndexOutputWriter trieOutputWriter;
    private final IndexOutput termsOutput;
    private final NumericValuesWriter offsetsWriter;
    private final String componentName;
    private final MetadataWriter metadataWriter;
    private final List<SortedTermsMeta.SortedTermsSegmentMeta> segments = new ArrayList<>();

    private TrieSegment trieSegment;
    private BytesRefBuilder tempTerm = new BytesRefBuilder();
    private BytesRefBuilder prevTerm = new BytesRefBuilder();

    private final long bytesStartFP;

    private int maxTermLength = -1;
    private long rowId = 0;

    /**
     * Creates a new writer.
     * <p>
     * It does not own the components, so you must close the components by yourself
     * after you're done with the writer.
     *
     * @param componentName the component name for the {@link SortedTermsMeta}
     * @param metadataWriter the {@link MetadataWriter} for storing the {@link SortedTermsMeta}
     * @param termsData where to write the prefix-compressed terms data
     * @param termsDataBlockOffsets  where to write the offsets of each block of terms data
     * @param trieOutput where to write the trie that maps the terms to point ids
     */
    public SortedTermsWriter(String componentName,
                             MetadataWriter metadataWriter,
                             IndexOutput termsData,
                             NumericValuesWriter termsDataBlockOffsets,
                             IndexOutputWriter trieOutput) throws IOException
    {
        this.componentName = componentName;
        this.metadataWriter = metadataWriter;
        this.trieOutputWriter = trieOutput;
        SAICodecUtils.writeHeader(this.trieOutputWriter);
        SAICodecUtils.writeHeader(termsData);
        this.termsOutput = termsData;
        this.bytesStartFP = termsData.getFilePointer();
        this.offsetsWriter = termsDataBlockOffsets;
    }

    public SortedTermsWriter(String componentName,
                             MetadataWriter metadataWriter,
                             IndexOutput termsData,
                             NumericValuesWriter termsDataBlockOffsets) throws IOException
    {
        this.componentName = componentName;
        this.metadataWriter = metadataWriter;
        this.trieOutputWriter = null;
        SAICodecUtils.writeHeader(termsData);
        this.termsOutput = termsData;
        this.bytesStartFP = termsData.getFilePointer();
        this.offsetsWriter = termsDataBlockOffsets;
    }


    /**
     * Appends a term at the end of the sequence.
     * Terms must be added in lexicographic order.
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

        if (trieOutputWriter != null)
        {
            if (trieSegment == null)
                trieSegment = new TrieSegment();
            else if (SegmentMemoryLimiter.usageExceedsLimit(trieSegment.totalBytesAllocated))
            {
                segments.add(trieSegment.flush());
                trieSegment = new TrieSegment();
            }
            trieSegment.add(term);
        }
        rowId++;
    }

    /**
     * Flushes any in-memory buffers to the output streams.
     * Does not close the output streams.
     * No more writes are allowed.
     */
    @Override
    public void close() throws IOException
    {
        if (trieOutputWriter != null)
        {
            // The trieSegment can be null if we haven't had any terms added
            if (trieSegment != null)
                segments.add(trieSegment.flush());
            SAICodecUtils.writeFooter(trieOutputWriter);
        }
        try (IndexOutput output = metadataWriter.builder(componentName))
        {
            SAICodecUtils.writeFooter(termsOutput);
            SortedTermsMeta.write(output, rowId, maxTermLength, segments);
            // Don't close the offsets writer quietly because of the work it does
            // during its close. We need to propagate the error.
            offsetsWriter.close();
        }
        finally
        {
            FileUtils.closeQuietly(Arrays.asList(trieOutputWriter, termsOutput));
        }
    }

    private void writeTermData(BytesRef term) throws IOException
    {
        if ((rowId & TERMS_DICT_BLOCK_MASK) == 0)
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

    private void copyBytes(ByteComparable source, BytesRefBuilder dest)
    {
        ByteSource byteSource = source.asComparableBytes(ByteComparable.Version.OSS50);
        int val;
        while ((val = byteSource.next()) != ByteSource.END_OF_STREAM)
            dest.append((byte) val);
    }

    private class TrieSegment
    {
        private final InMemoryTrie<Long> trie;
        private final BytesRefBuilder prevTerm = new BytesRefBuilder();
        private final BytesRefBuilder tempTerm = new BytesRefBuilder();

        private BytesRef minTerm;
        private long totalBytesAllocated;
        private boolean flushed = false;
        private boolean active = true;

        TrieSegment()
        {
            trie = new InMemoryTrie<>(TrieMemtable.BUFFER_TYPE);
            SegmentMemoryLimiter.registerBuilder();
        }

        void add(ByteComparable term)
        {
            final long initialSizeOnHeap = trie.sizeOnHeap();


            try
            {
                trie.putRecursive(term, rowId, (existing, update) -> update);
            }
            catch (InMemoryTrie.SpaceExhaustedException e)
            {
                throw Throwables.unchecked(e);
            }

            long bytesAllocated = trie.sizeOnHeap() - initialSizeOnHeap;
            totalBytesAllocated += bytesAllocated;
            SegmentMemoryLimiter.increment(bytesAllocated);
        }

        SortedTermsMeta.SortedTermsSegmentMeta flush() throws IOException
        {
            assert !flushed : "Cannot flush a trie segment that has already been flushed";

            flushed = true;

            long trieFilePointer;

            try (IncrementalDeepTrieWriterPageAware<Long> trieWriter = new IncrementalDeepTrieWriterPageAware<>(trieSerializer,
                                                                                                                trieOutputWriter.asSequentialWriter()))
            {
                Iterator<Map.Entry<ByteComparable, Long>> iterator = trie.entryIterator();

                while (iterator.hasNext())
                {
                    Map.Entry<ByteComparable, Long> next = iterator.next();
                    tempTerm.clear();
                    copyBytes(next.getKey(), tempTerm);

                    BytesRef termRef = tempTerm.get();

                    if (minTerm == null)
                        minTerm = new BytesRef(Arrays.copyOf(termRef.bytes, termRef.length));

                    trieWriter.add(next.getKey(), next.getValue());
                    copyBytes(next.getKey(), prevTerm);
                }

                trieFilePointer = trieWriter.complete();
            }
            finally
            {
                release();
            }

            return new SortedTermsMeta.SortedTermsSegmentMeta(trieFilePointer, minTerm, tempTerm.get().clone());
        }

        void release()
        {
            if (active)
            {
                SegmentMemoryLimiter.unregisterBuilder();
                SegmentMemoryLimiter.decrement(totalBytesAllocated);
                active = false;
            }
        }

    }
}
