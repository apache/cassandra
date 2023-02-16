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
import java.util.Arrays;
import java.util.Map;
import java.util.NavigableMap;
import java.util.zip.CRC32;

import javax.annotation.Nullable;

import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.Crc;

import static org.apache.cassandra.journal.Journal.validateCRC;
import static org.apache.cassandra.utils.FBUtilities.updateChecksumInt;

/**
 * An on-disk (memory-mapped) index for a completed flushed segment.
 * <p/>
 * TODO (expected): block-level CRC
 */
final class OnDiskIndex<K> extends Index<K>
{
    private static final int[] EMPTY = new int[0];

    private static final int FILE_PREFIX_SIZE = 4 + 4; // count of entries, CRC
    private static final int VALUE_SIZE = 4;           // int offset

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
            FileUtils.clean(buffer);
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
            FileUtils.clean(buffer);
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
        NavigableMap<K, int[]> entries, KeySupport<K> keySupport, DataOutputPlus out, int userVersion) throws IOException
    {
        CRC32 crc = Crc.crc32();

        int size = entries.values()
                          .stream()
                          .mapToInt(offsets -> offsets.length)
                          .sum();
        out.writeInt(size);
        updateChecksumInt(crc, size);
        out.writeInt((int) crc.getValue());

        for (Map.Entry<K, int[]> entry : entries.entrySet())
        {
            for (int offset : entry.getValue())
            {
                K key = entry.getKey();
                keySupport.serialize(key, out, userVersion);
                keySupport.updateChecksum(crc, key, userVersion);

                out.writeInt(offset);
                updateChecksumInt(crc, offset);
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

    @Override
    public int[] lookUp(K id)
    {
        if (!mayContainId(id))
            return EMPTY;

        int keyIndex = binarySearch(id);
        if (keyIndex < 0)
            return EMPTY;

        int[] offsets = new int[] { offsetAtIndex(keyIndex) };

        /*
         * Duplicate entries are possible within one segment (but should be rare).
         * Check and add entries before and after the found result (not guaranteed to be first).
         */

        for (int i = keyIndex - 1; i >= 0 && id.equals(keyAtIndex(i)); i--)
        {
            int length = offsets.length;
            offsets = Arrays.copyOf(offsets, length + 1);
            offsets[length] = offsetAtIndex(i);
        }

        for (int i = keyIndex + 1; i < entryCount && id.equals(keyAtIndex(i)); i++)
        {
            int length = offsets.length;
            offsets = Arrays.copyOf(offsets, length + 1);
            offsets[length] = offsetAtIndex(i);
        }

        Arrays.sort(offsets);
        return offsets;
    }

    @Override
    public int lookUpFirst(K id)
    {
        if (!mayContainId(id))
            return -1;

        int keyIndex = binarySearch(id);

        /*
         * Duplicate entries are possible within one segment (but should be rare).
         * Check and add entries before until we find the first occurrence of key.
         */
        for (int i = keyIndex - 1; i >= 0 && id.equals(keyAtIndex(i)); i--)
            keyIndex = i;

        return keyIndex < 0 ? -1 : offsetAtIndex(keyIndex);
    }

    private K keyAtIndex(int index)
    {
        return keySupport.deserialize(buffer, FILE_PREFIX_SIZE + index * ENTRY_SIZE, descriptor.userVersion);
    }

    private int offsetAtIndex(int index)
    {
        return buffer.getInt(FILE_PREFIX_SIZE + index * ENTRY_SIZE + KEY_SIZE);
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
            mid = (low + high) >> 1;
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
