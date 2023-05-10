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

import org.agrona.collections.IntHashSet;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Crc;

import static org.apache.cassandra.journal.Journal.validateCRC;
import static org.apache.cassandra.utils.FBUtilities.updateChecksum;
import static org.apache.cassandra.utils.FBUtilities.updateChecksumInt;
import static org.apache.cassandra.utils.FBUtilities.updateChecksumShort;

final class EntrySerializer
{
    static <K> void write(K key,
                          ByteBuffer record,
                          Set<Integer> hosts,
                          KeySupport<K> keySupport,
                          DataOutputPlus out,
                          int userVersion)
    throws IOException
    {
        CRC32 crc = Crc.crc32();

        keySupport.serialize(key, out, userVersion);
        keySupport.updateChecksum(crc, key, userVersion);

        out.writeShort(hosts.size());
        updateChecksumShort(crc, (short) hosts.size());

        int recordSize = record.remaining();
        out.writeInt(recordSize);
        updateChecksumInt(crc, recordSize);

        out.writeInt((int) crc.getValue());

        for (int host : hosts)
        {
            out.writeInt(host);
            updateChecksumInt(crc, host);
        }

        out.write(record);
        Crc.updateCrc32(crc, record, record.position(), record.limit());

        out.writeInt((int) crc.getValue());
    }

    static <K> void read(EntryHolder<K> into,
                         KeySupport<K> keySupport,
                         ByteBuffer buffer,
                         int userVersion)
    throws IOException
    {
        CRC32 crc = Crc.crc32();
        into.clear();

        try (DataInputBuffer in = new DataInputBuffer(buffer, false))
        {
            K key = keySupport.deserialize(in, userVersion);
            keySupport.updateChecksum(crc, key, userVersion);
            into.key = key;

            int hostCount = in.readShort();
            updateChecksumShort(crc, (short) hostCount);

            int entrySize = in.readInt();
            updateChecksumInt(crc, entrySize);

            validateCRC(crc, in.readInt());

            for (int i = 0; i < hostCount; i++)
            {
                int hostId = in.readInt();
                updateChecksumInt(crc, hostId);
                into.hosts.add(hostId);
            }

            ByteBuffer entry = ByteBufferUtil.read(in, entrySize);
            updateChecksum(crc, entry);
            into.value = entry;

            validateCRC(crc, in.readInt());
        }
    }

    static <K> boolean tryRead(EntryHolder<K> into,
                               KeySupport<K> keySupport,
                               ByteBuffer buffer,
                               DataInputBuffer in,
                               int syncedOffset,
                               int userVersion)
    throws IOException
    {
        CRC32 crc = Crc.crc32();
        into.clear();

        int fixedSize = EntrySerializer.fixedEntrySize(keySupport, userVersion);
        if (buffer.remaining() < fixedSize)
            return handleReadException(new EOFException(), buffer.limit(), syncedOffset);

        updateChecksum(crc, buffer, buffer.position(), fixedSize - TypeSizes.INT_SIZE);
        int fixedCrc = buffer.getInt(buffer.position() + fixedSize - TypeSizes.INT_SIZE);

        try
        {
            validateCRC(crc, fixedCrc);
        }
        catch (IOException e)
        {
            return handleReadException(e, buffer.position() + fixedSize, syncedOffset);
        }

        int hostCount, recordSize;
        try
        {
            into.key = keySupport.deserialize(in, userVersion);
            hostCount = in.readShort();
            recordSize = in.readInt();
            in.skipBytesFully(TypeSizes.INT_SIZE);
        }
        catch (IOException e)
        {
            throw new RuntimeException(); // can't happen unless deserializer is buggy
        }

        int variableSize = EntrySerializer.variableEntrySize(hostCount, recordSize);
        if (buffer.remaining() < variableSize)
            return handleReadException(new EOFException(), buffer.limit(), syncedOffset);

        updateChecksum(crc, buffer, buffer.position(), variableSize - TypeSizes.INT_SIZE);
        int variableCrc = buffer.getInt(buffer.position() + variableSize - TypeSizes.INT_SIZE);

        try
        {
            validateCRC(crc, variableCrc);
        }
        catch (IOException e)
        {
            return handleReadException(e, buffer.position() + variableSize, syncedOffset);
        }

        for (int i = 0; i < hostCount; i++)
        {
            into.hosts.add(in.readInt());
        }

        try
        {
            in.skipBytesFully(recordSize);
        }
        catch (IOException e)
        {
            throw new AssertionError(); // can't happen
        }

        into.value = (ByteBuffer) buffer.duplicate()
                                        .position(buffer.position() - recordSize)
                                        .limit(buffer.position());

        in.skipBytesFully(TypeSizes.INT_SIZE);
        return true;
    }

    private static boolean handleReadException(IOException e, int bufferPosition, int fsyncedLimit) throws IOException
    {
        if (bufferPosition <= fsyncedLimit)
            throw e;
        else
            return false;
    }

    static <K> int fixedEntrySize(KeySupport<K> keySupport, int userVersion)
    {
        return keySupport.serializedSize(userVersion) // key/id
             + TypeSizes.SHORT_SIZE                   // host count
             + TypeSizes.INT_SIZE                     // record size
             + TypeSizes.INT_SIZE;                    // CRC
    }

    static int variableEntrySize(int hostCount, int recordSize)
    {
        return TypeSizes.INT_SIZE * hostCount // hosts
             + recordSize                     // record
             + TypeSizes.INT_SIZE;            // CRC
    }

    static final class EntryHolder<K>
    {
        K key;
        ByteBuffer value;
        IntHashSet hosts = new IntHashSet();

        void clear()
        {
            key = null;
            value = null;
            hosts.clear();
        }
    }
}
