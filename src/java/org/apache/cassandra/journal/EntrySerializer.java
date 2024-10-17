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

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.zip.CRC32;

import accord.utils.Invariants;
import org.agrona.collections.IntHashSet;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Crc;

import static org.apache.cassandra.journal.Journal.validateCRC;

public final class EntrySerializer
{
    static <K> void write(K key,
                          ByteBuffer record,
                          Set<Integer> hosts,
                          KeySupport<K> keySupport,
                          ByteBuffer out,
                          int userVersion)
    throws IOException
    {
        int start = out.position();
        int totalSize = out.getInt() - start;
        Invariants.checkState(totalSize == out.remaining() + TypeSizes.INT_SIZE);
        Invariants.checkState(totalSize == record.remaining() + fixedEntrySize(keySupport, userVersion) + variableEntrySize(hosts.size()));

        keySupport.serialize(key, out, userVersion);
        out.putShort((short)hosts.size());

        int fixedCrcPosition = out.position();
        out.position(fixedCrcPosition + TypeSizes.INT_SIZE);

        for (int host : hosts)
            out.putInt(host);

        int recordSize = record.remaining();
        int recordEnd = out.position() + recordSize;
        Invariants.checkState(out.limit() == recordEnd + TypeSizes.INT_SIZE);
        ByteBufferUtil.copyBytes(record, record.position(), out, out.position(), recordSize);

        // update and write crcs
        CRC32 crc = Crc.crc32();
        out.position(start);
        out.limit(fixedCrcPosition);
        crc.update(out);
        out.limit(recordEnd);
        out.putInt((int) crc.getValue());
        crc.update(out);
        out.limit(recordEnd + 4);
        out.putInt((int) crc.getValue());
    }

    // we reuse record as the value we return
    static <K> void read(EntryHolder<K> into,
                         KeySupport<K> keySupport,
                         ByteBuffer from,
                         int userVersion)
    throws IOException
    {
        into.clear();

        int start = from.position();
        {
            int totalSize = from.getInt(start) - start;
            Invariants.checkState(totalSize == from.remaining());

            CRC32 crc = Crc.crc32();
            int fixedSize = EntrySerializer.fixedEntrySize(keySupport, userVersion);
            int fixedCrc = readAndUpdateFixedCrc(crc, from, fixedSize);
            validateCRC(crc, fixedCrc);

            int recordCrc = readAndUpdateRecordCrc(crc, from, start + totalSize);
            validateCRC(crc, recordCrc);
        }

        readValidated(into, from, start, keySupport, userVersion);
    }

    // slices the provided buffer to assign to into.value
    static <K> int tryRead(EntryHolder<K> into,
                           KeySupport<K> keySupport,
                           ByteBuffer from,
                           int syncedOffset,
                           int userVersion)
    throws IOException
    {
        CRC32 crc = Crc.crc32();
        into.clear();

        int start = from.position();
        if (from.remaining() < TypeSizes.INT_SIZE)
            return -1;

        int totalSize = from.getInt(start) - start;
        if (totalSize == 0)
            return -1;

        if (from.remaining() < totalSize)
            return handleReadException(new EOFException(), from.limit(), syncedOffset);

        {
            int fixedSize = EntrySerializer.fixedEntrySize(keySupport, userVersion);
            int fixedCrc = readAndUpdateFixedCrc(crc, from, fixedSize);
            try
            {
                validateCRC(crc, fixedCrc);
            }
            catch (IOException e)
            {
                return handleReadException(e, from.position() + fixedSize, syncedOffset);
            }

            int recordCrc = readAndUpdateRecordCrc(crc, from, start + totalSize);
            try
            {
                validateCRC(crc, recordCrc);
            }
            catch (IOException e)
            {
                return handleReadException(e, from.position(), syncedOffset);
            }
        }

        readValidated(into, from, start, keySupport, userVersion);
        return totalSize;
    }

    private static <K> void readValidated(EntryHolder<K> into, ByteBuffer from, int start, KeySupport<K> keySupport, int userVersion)
    {
        from.position(start + TypeSizes.INT_SIZE);
        into.key = keySupport.deserialize(from, userVersion);
        int hostCount = from.getShort();

        from.position(from.position() + 4);
        for (int i = 0; i < hostCount; i++)
        {
            int hostId = from.getInt();
            into.hosts.add(hostId);
        }

        into.value = from;
        into.userVersion = userVersion;
    }

    private static int readAndUpdateFixedCrc(CRC32 crc, ByteBuffer from, int fixedSize)
    {
        int fixedEnd = from.position() + fixedSize - TypeSizes.INT_SIZE;
        int fixedCrc = from.getInt(fixedEnd);
        from.limit(fixedEnd);
        crc.update(from);
        return fixedCrc;
    }

    private static int readAndUpdateRecordCrc(CRC32 crc, ByteBuffer from, int limit)
    {
        int recordEnd = limit - TypeSizes.INT_SIZE;
        from.limit(limit);
        int recordCrc = from.getInt(recordEnd);
        from.position(from.position() + 4);
        from.limit(recordEnd);
        crc.update(from);
        return recordCrc;
    }

    private static int handleReadException(IOException e, int bufferPosition, int fsyncedLimit) throws IOException
    {
        if (bufferPosition <= fsyncedLimit)
            throw e;
        else
            return -1;
    }

    static <K> int fixedEntrySize(KeySupport<K> keySupport, int userVersion)
    {
        return keySupport.serializedSize(userVersion) // key/id
             + TypeSizes.SHORT_SIZE                   // host count
             + TypeSizes.INT_SIZE                     // total size
             + TypeSizes.INT_SIZE;                    // CRC
    }

    static int variableEntrySize(int hostCount)
    {
        return TypeSizes.INT_SIZE * hostCount // hosts
             + TypeSizes.INT_SIZE;            // CRC
    }

    public static final class EntryHolder<K>
    {
        public K key;
        public ByteBuffer value;
        public IntHashSet hosts = new IntHashSet();

        public int userVersion;

        public void clear()
        {
            key = null;
            value = null;
            hosts.clear();
        }
    }
}
