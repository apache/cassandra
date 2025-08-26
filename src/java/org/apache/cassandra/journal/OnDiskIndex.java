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
package org.apache.cassandra.journal;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.NavigableMap;
import java.util.zip.CRC32;
import javax.annotation.Nullable;

import accord.utils.Invariants;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.Crc;
import org.apache.cassandra.utils.memory.MemoryUtil;

import static org.apache.cassandra.journal.Journal.validateCRC;
import static org.apache.cassandra.utils.FBUtilities.updateChecksumInt;
import static org.apache.cassandra.utils.FBUtilities.updateChecksumLong;

/**
 * An on-disk (memory-mapped) index for a completed flushed segment.
 * <p/>
 * TODO (expected): block-level CRC
 */
final class OnDiskIndex<K> extends Index<K>
{
    private static final long[] EMPTY = new long[0];

    private static final int FILE_PREFIX_SIZE = 4 + 4; // count of entries, CRC
    private static final int VALUE_SIZE = Long.BYTES;   // int offset + int size

    private final int KEY_SIZE;
    private final int ENTRY_SIZE;

    private final Descriptor descriptor;

    private final FileChannel channel;
    private volatile MappedByteBuffer buffer;
    private final int entryCount;

    private volatile K firstId, lastId;

    private OnDiskIndex(
        Descriptor descriptor, KeySupport<K> keySupport, FileChannel channel, MappedByteBuffer buffer, int entryCount)
    {
        super(keySupport);

        this.descriptor = descriptor;
        this.channel = channel;
        this.buffer = buffer;
        this.entryCount = entryCount;

        KEY_SIZE = keySupport.serializedSize(descriptor.userVersion);
        ENTRY_SIZE = KEY_SIZE + VALUE_SIZE;
    }

    /**
     * Open the index for reading, validate CRC
     */
    @SuppressWarnings({ "resource", "RedundantSuppression" })
    static <K> OnDiskIndex<K> open(Descriptor descriptor, KeySupport<K> keySupport)
    {
        File file = descriptor.fileFor(Component.INDEX);
        FileChannel channel = null;
        MappedByteBuffer buffer = null;
        try
        {
            channel = FileChannel.open(file.toPath(), StandardOpenOption.READ);
            buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());

            int entryCount = buffer.getInt(0);
            OnDiskIndex<K> index = new OnDiskIndex<>(descriptor, keySupport, channel, buffer, entryCount);
            index.validate();
            index.init();
            return index;
        }
        catch (Throwable e)
        {
            MemoryUtil.clean(buffer);
            FileUtils.closeQuietly(channel);
            throw new JournalReadError(descriptor, file, e);
        }
    }

    private void init()
    {
        if (entryCount > 0)
        {
            firstId = keyAtIndex(0);
             lastId = keyAtIndex(entryCount - 1);
        }
    }

    @Override
    public void close()
    {
        try
        {
            MemoryUtil.clean(buffer);
            buffer = null;
            channel.close();
        }
        catch (IOException e)
        {
            throw new JournalWriteError(descriptor, Component.INDEX, e);
        }
    }

    void validate() throws IOException
    {
        CRC32 crc = Crc.crc32();

        try (DataInputBuffer in = new DataInputBuffer(buffer, true))
        {
            int entryCount = in.readInt();
            updateChecksumInt(crc, entryCount);
            validateCRC(crc, in.readInt());

            Crc.updateCrc32(crc, buffer, FILE_PREFIX_SIZE, FILE_PREFIX_SIZE + entryCount * ENTRY_SIZE);
            in.skipBytesFully(entryCount * ENTRY_SIZE);
            validateCRC(crc, in.readInt());

            if (in.available() != 0)
                throw new IOException("Trailing data encountered in segment index " + descriptor.fileFor(Component.INDEX));
        }
    }

    static <K> void write(
        NavigableMap<K, long[]> entries, KeySupport<K> keySupport, DataOutputPlus out, int userVersion) throws IOException
    {
        CRC32 crc = Crc.crc32();

        int size = entries.values()
                          .stream()
                          .mapToInt(offsets -> offsets.length)
                          .sum();
        out.writeInt(size);
        updateChecksumInt(crc, size);
        out.writeInt((int) crc.getValue());

        for (Map.Entry<K, long[]> entry : entries.entrySet())
        {
            long prev = -1;
            for (long offsetAndSize : entry.getValue())
            {
                K key = entry.getKey();
                keySupport.serialize(key, out, userVersion);
                keySupport.updateChecksum(crc, key, userVersion);

                if (prev != -1)
                {
                    long tmp = prev;
                    Invariants.require(readOffset(offsetAndSize) < readOffset(prev),
                                          () -> String.format("Offsets should be strictly reverse monotonic, but found %d following %d",
                                                              readOffset(offsetAndSize), readOffset(tmp)));
                }
                out.writeLong(offsetAndSize);
                updateChecksumLong(crc, offsetAndSize);
                prev = offsetAndSize;
            }
        }

        out.writeInt((int) crc.getValue());
    }

    @Override
    @Nullable
    public K firstId()
    {
        return firstId;
    }

    @Override
    @Nullable
    public K lastId()
    {
        return lastId;
    }

    public int entryCount()
    {
        return entryCount;
    }

    @Override
    public long[] lookUp(K id)
    {
        return lookUpAll(id);
    }

    @Override
    public long lookUpLast(K id)
    {
        if (!mayContainId(id))
            return -1L;

        int keyIndex = binarySearch(id);
        return keyIndex < 0 ? -1 : recordAtIndex(keyIndex);
    }

    @Override
    public long[] lookUpAll(K id)
    {
        if (!mayContainId(id))
            return EMPTY;

        int someIndex = binarySearch(id);
        if (someIndex < 0)
            return EMPTY;

        int firstKeyIndex = someIndex;
        while (firstKeyIndex > 0 && id.equals(keyAtIndex(firstKeyIndex - 1)))
            --firstKeyIndex;

        int lastKeyIndex = someIndex;
        while (lastKeyIndex + 1 < entryCount && id.equals(keyAtIndex(lastKeyIndex + 1)))
            ++lastKeyIndex;

        long[] all = new long[lastKeyIndex - firstKeyIndex + 1];
        int idx = firstKeyIndex;
        for (int i = 0; i < all.length; i++)
            all[i] = recordAtIndex(idx++);
        return all;
    }

    IndexReader reader()
    {
        return new IndexReader();
    }

    public class IndexReader extends AbstractIterator<K>
    {
        int lastIdx = entryCount - 1;
        int idx;
        K key;
        int offset;
        int size;

        IndexReader()
        {
            idx = -1;
        }

        public void seek(K key)
        {
            int i = binarySearch(key);
            if (i < 0) i = -1 - i;
            idx = i - 1;
        }

        public void seekEnd(K key)
        {
            int i = binarySearch(key);
            if (i < 0) i = -2 - i;
            lastIdx = i;
        }

        protected K computeNext()
        {
            if (advance())
                return key;
            else
                return endOfData();
        }

        public int offset()
        {
            ensureAdvanced();
            return offset;
        }

        public int recordSize()
        {
            ensureAdvanced();
            return size;
        }

        public boolean advance()
        {
            if (idx >= lastIdx)
                return false;

            idx++;
            key = keyAtIndex(idx);
            long record = recordAtIndex(idx);
            offset = Index.readOffset(record);
            size = Index.readSize(record);
            return true;
        }

        private void ensureAdvanced()
        {
            if (idx < 0)
                throw new IllegalStateException("Must call advance() before accessing entry content");
        }
    }

    private K keyAtIndex(int index)
    {
        return keySupport.deserialize(buffer, FILE_PREFIX_SIZE + index * ENTRY_SIZE, descriptor.userVersion);
    }

    private long recordAtIndex(int index)
    {
        return buffer.getLong(FILE_PREFIX_SIZE + index * ENTRY_SIZE + KEY_SIZE);
    }

    /*
     * This has been lifted from {@see IndexSummary}'s implementation,
     * which itself was lifted from Harmony's Collections implementation.
     */
    private int binarySearch(K key)
    {
        int low = 0, mid = entryCount, high = mid - 1, result = -1;
        while (low <= high)
        {
            mid = (low + high) >>> 1;
            result = compareWithKeyAt(key, mid);
            if (result > 0)
            {
                low = mid + 1;
            }
            else if (result == 0)
            {
                return mid;
            }
            else
            {
                high = mid - 1;
            }
        }
        return -mid - (result < 0 ? 1 : 2);
    }

    private int compareWithKeyAt(K key, int keyIndex)
    {
        int offset = FILE_PREFIX_SIZE + ENTRY_SIZE * keyIndex;
        return keySupport.compareWithKeyAt(key, buffer, offset, descriptor.userVersion);
    }

    static <K> OnDiskIndex<K> rebuildAndPersist(Descriptor descriptor, KeySupport<K> keySupport, int fsyncedLimit)
    {
        try (InMemoryIndex<K> index = InMemoryIndex.rebuild(descriptor, keySupport, fsyncedLimit))
        {
            index.persist(descriptor);
        }
        return open(descriptor, keySupport);
    }
}
