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

package org.apache.cassandra.index.sai.disk.v1.keystore;

import java.io.Closeable;
import java.io.IOException;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

import org.apache.cassandra.index.sai.disk.v1.MetadataWriter;
import org.apache.cassandra.index.sai.disk.v1.SAICodecUtils;
import org.apache.cassandra.index.sai.disk.v1.bitpack.NumericValuesWriter;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.FastByteOperations;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.StringHelper;

/**
 * Writes a sequence of partition keys or clustering keys for use with {@link KeyLookup}.
 * <p>
 * Partition keys are written unordered and clustering keys are written in ordered partitions determined by calls to
 * {@link #startPartition()}. In either case keys can be of varying lengths.
 * <p>
 * The {@link #blockShift} field is used to quickly determine the id of the current block
 * based on a point id or to check if we are exactly at the beginning of the block.
 * <p>
 * Keys are organized in blocks of (2 ^ {@link #blockShift}) keys.
 * <p>
 * The blocks should not be too small because they allow prefix compression of the keys except the first key in a block.
 * <p>
 * The blocks should not be too large because we can't just randomly jump to the key inside the block, but we have to
 * iterate through all the keys from the start of the block.
 *
 * @see KeyLookup
 */
@NotThreadSafe
public class KeyStoreWriter implements Closeable
{
    private final int blockShift;
    private final int blockMask;
    private final boolean clustering;
    private final IndexOutput keysOutput;
    private final NumericValuesWriter offsetsWriter;
    private final String componentName;
    private final MetadataWriter metadataWriter;

    private BytesRefBuilder prevKey = new BytesRefBuilder();
    private BytesRefBuilder tempKey = new BytesRefBuilder();

    private final long bytesStartFP;

    private boolean inPartition = false;
    private int maxKeyLength = -1;
    private long pointId = 0;

    /**
     * Creates a new writer.
     * <p>
     * It does not own the components, so you must close the components by yourself
     * after you're done with the writer.
     *
     * @param componentName the component name for the {@link KeyLookupMeta}
     * @param metadataWriter the {@link MetadataWriter} for storing the {@link KeyLookupMeta}
     * @param keysOutput where to write the prefix-compressed keys
     * @param keysBlockOffsets  where to write the offsets of each block of keys
     * @param blockShift the block shift that is used to determine the block size
     * @param clustering determines whether the keys will be written as ordered partitions
     */
    public KeyStoreWriter(String componentName,
                          MetadataWriter metadataWriter,
                          IndexOutput keysOutput,
                          NumericValuesWriter keysBlockOffsets,
                          int blockShift,
                          boolean clustering) throws IOException
    {
        this.componentName = componentName;
        this.metadataWriter = metadataWriter;
        SAICodecUtils.writeHeader(keysOutput);
        this.blockShift = blockShift;
        this.blockMask = (1 << this.blockShift) - 1;
        this.clustering = clustering;
        this.keysOutput = keysOutput;
        this.keysOutput.writeVInt(blockShift);
        this.keysOutput.writeByte((byte ) (clustering ? 1 : 0));
        this.bytesStartFP = keysOutput.getFilePointer();
        this.offsetsWriter = keysBlockOffsets;
    }

    public void startPartition()
    {
        assert clustering : "Cannot start a partition on a non-clustering key store";

        inPartition = false;
    }

    /**
     * Appends a key at the end of the sequence.
     *
     * @throws IOException if write to disk fails
     * @throws IllegalArgumentException if the key is not greater than the previous added key
     */
    public void add(final @Nonnull ByteComparable key) throws IOException
    {
        tempKey.clear();
        copyBytes(key, tempKey);

        BytesRef keyRef = tempKey.get();

        if (clustering && inPartition)
        {
            if (compareKeys(keyRef, prevKey.get()) <= 0)
                throw new IllegalArgumentException("Clustering keys must be in ascending lexographical order");
        }

        inPartition = true;

        writeKey(keyRef);

        maxKeyLength = Math.max(maxKeyLength, keyRef.length);

        BytesRefBuilder temp = this.tempKey;
        this.tempKey = this.prevKey;
        this.prevKey = temp;

        pointId++;
    }

    private void writeKey(BytesRef key) throws IOException
    {
        if ((pointId & blockMask) == 0)
        {
            offsetsWriter.add(keysOutput.getFilePointer() - bytesStartFP);

            keysOutput.writeVInt(key.length);
            keysOutput.writeBytes(key.bytes, key.offset, key.length);
        }
        else
        {
            int prefixLength = 0;
            int suffixLength = 0;

            // If the key is the same as the previous key then we use prefix and suffix lengths of 0.
            // This means that we store a byte of 0 and don't write any data for the key.
            if (compareKeys(prevKey.get(), key) != 0)
            {
                prefixLength = StringHelper.bytesDifference(prevKey.get(), key);
                suffixLength = key.length - prefixLength;
            }
            // The prefix and suffix lengths are written as a byte followed by up to 2 vints. An attempt is
            // made to compress the lengths into the byte (if prefix length < 15 and/or suffix length < 15).
            // If either length exceeds the compressed byte maximum, it is written as a vint following the byte.
            keysOutput.writeByte((byte) (Math.min(prefixLength, 15) | (Math.min(15, suffixLength) << 4)));

            if (prefixLength + suffixLength > 0)
            {
                if (prefixLength >= 15)
                    keysOutput.writeVInt(prefixLength - 15);
                if (suffixLength >= 15)
                    keysOutput.writeVInt(suffixLength - 15);

                keysOutput.writeBytes(key.bytes, key.offset + prefixLength, key.length - prefixLength);
            }
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
            SAICodecUtils.writeFooter(keysOutput);
            KeyLookupMeta.write(output, pointId, maxKeyLength);
        }
        finally
        {
            FileUtils.close(offsetsWriter, keysOutput);
        }
    }

    private int compareKeys(BytesRef left, BytesRef right)
    {
        return FastByteOperations.compareUnsigned(left.bytes, left.offset, left.offset + left.length,
                                                  right.bytes, right.offset, right.offset + right.length);
    }

    private void copyBytes(ByteComparable source, BytesRefBuilder dest)
    {
        ByteSource byteSource = source.asComparableBytes(ByteComparable.Version.OSS50);
        int val;
        while ((val = byteSource.next()) != ByteSource.END_OF_STREAM)
            dest.append((byte) val);
    }
}
