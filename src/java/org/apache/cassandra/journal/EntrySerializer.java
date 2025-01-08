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
import java.util.zip.CRC32;

import accord.utils.Invariants;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Crc;

import static org.apache.cassandra.journal.Journal.validateCRC;

public final class EntrySerializer
{
    static <K> void write(K key,
                          ByteBuffer record,
                          KeySupport<K> keySupport,
                          ByteBuffer out,
                          int userVersion)
    throws IOException
    {
        int start = out.position();
        int totalSize = out.getInt() - start;
        Invariants.checkState(totalSize == TypeSizes.INT_SIZE + out.remaining());
        Invariants.checkState(totalSize == headerSize(keySupport, userVersion) + record.remaining() + TypeSizes.INT_SIZE);

        keySupport.serialize(key, out, userVersion);

        int headerCrcPosition = out.position();
        out.position(headerCrcPosition + TypeSizes.INT_SIZE);

        int recordSize = record.remaining();
        int recordEnd = out.position() + recordSize;
        Invariants.checkState(out.limit() == recordEnd + TypeSizes.INT_SIZE);
        ByteBufferUtil.copyBytes(record, record.position(), out, out.position(), recordSize);

        // update and write crcs
        CRC32 crc = Crc.crc32();
        out.position(start);
        out.limit(headerCrcPosition);
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
            int headerSize = EntrySerializer.headerSize(keySupport, userVersion);
            int headerCrc = readAndUpdateHeaderCrc(crc, from, headerSize);
            validateCRC(crc, headerCrc);

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
            int headerSize = EntrySerializer.headerSize(keySupport, userVersion);
            int headerCrc = readAndUpdateHeaderCrc(crc, from, headerSize);
            try
            {
                validateCRC(crc, headerCrc);
            }
            catch (IOException e)
            {
                return handleReadException(e, from.position() + headerSize, syncedOffset);
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
        from.position(from.position() + 4);
        into.value = from;
        into.userVersion = userVersion;
    }

    private static int readAndUpdateHeaderCrc(CRC32 crc, ByteBuffer from, int headerSize)
    {
        int headerEnd = from.position() + headerSize - TypeSizes.INT_SIZE;
        int headerCrc = from.getInt(headerEnd);
        from.limit(headerEnd);
        crc.update(from);
        return headerCrc;
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

    static <K> int headerSize(KeySupport<K> keySupport, int userVersion)
    {
        return TypeSizes.INT_SIZE                     // pointer to next entry
             + keySupport.serializedSize(userVersion) // key/id
             + TypeSizes.INT_SIZE;                    // CRC
    }

    public static final class EntryHolder<K>
    {
        public K key;
        public ByteBuffer value;

        public int userVersion;

        public void clear()
        {
            key = null;
            value = null;
        }
    }
}
