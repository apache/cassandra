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

package org.apache.cassandra.service.accord.journal;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.zip.CRC32;

import com.google.common.primitives.Longs;

import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileInputStreamPlus;
import org.apache.cassandra.io.util.FileOutputStreamPlus;
import org.apache.cassandra.utils.NativeLibrary;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.config.DatabaseDescriptor.getAccordJournalDirectory;
import static org.apache.cassandra.utils.Crc.crc32;

public class ReplayMarkers
{
    public static File startMarker()
    {
        return new File(getAccordJournalDirectory(), "start.crc");
    }

    public static File safeStopMarker()
    {
        return new File(getAccordJournalDirectory(), "stop.crc");
    }

    public static void writeMarker(File file, long timestamp, long lastUniqueTimeStamp)
    {
        try (FileOutputStreamPlus out = new FileOutputStreamPlus(file))
        {
            CRC32 crc = crc32();
            out.writeLong(timestamp);
            crc.update(Longs.toByteArray(timestamp));
            out.writeLong(lastUniqueTimeStamp);
            crc.update(Longs.toByteArray(lastUniqueTimeStamp));
            out.writeInt((int) crc.getValue());
        }
        catch (IOException e)
        {
            throw new UncheckedIOException(e);
        }
        trySyncJournalDirectory();
    }

    public static Pair<Long, Long> readStartMarker()
    {
        File crcFile = new File(getAccordJournalDirectory(), "start.crc");
        if (crcFile.exists())
            return readCrcMarker(crcFile);
        else
            return readMarker(new File(getAccordJournalDirectory(), "started"));
    }

    public static Pair<Long, Long> readStopMarker()
    {
        File crcFile = new File(getAccordJournalDirectory(), "stop.crc");
        if (crcFile.exists())
            return readCrcMarker(crcFile);
        else
            return readMarker(new File(getAccordJournalDirectory(), "stopped"));
    }

    public static Pair<Long, Long> readCrcMarker(File file)
    {
        if (!file.exists())
            return Pair.create(-1L, -1L);

        try (FileInputStreamPlus in = new FileInputStreamPlus(file))
        {
            CRC32 crc = crc32();
            long timestamp = in.readLong();
            crc.update(Longs.toByteArray(timestamp));
            long lastUniqueTimeStamp = in.readLong();
            crc.update(Longs.toByteArray(lastUniqueTimeStamp));
            int checksum = in.readInt();
            if (in.read() != -1 || (int) crc.getValue() != checksum)
                throw new IOException("Stop.crc file corrupted");

            return Pair.create(timestamp, lastUniqueTimeStamp);
        }
        catch (IOException e)
        {
            throw new UncheckedIOException(e);
        }
    }

    public static Pair<Long, Long> readMarker(File file)
    {
        if (!file.exists())
            return Pair.create(-1L, -1L);

        try (FileInputStreamPlus in = new FileInputStreamPlus(file))
        {
            StringBuilder sb = new StringBuilder(8);
            for (int b = in.read(); b >= 0 ; b = in.read())
                sb.append((char)b);
            return Pair.create(Long.parseLong(sb.toString()), -1L);
        }
        catch (IOException e)
        {
            throw new UncheckedIOException(e);
        }
    }

    private static void trySyncJournalDirectory()
    {
        trySyncDirectory(getAccordJournalDirectory());
    }

    private static void trySyncDirectory(String path)
    {
        int fd = NativeLibrary.tryOpenDirectory(path);
        NativeLibrary.trySync(fd);
    }

    public static File saveDirectory()
    {
        return new File(getAccordJournalDirectory(), "save_state");
    }
}
