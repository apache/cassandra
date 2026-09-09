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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileInputStreamPlus;
import org.apache.cassandra.io.util.FileOutputStreamPlus;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.NativeLibrary;

import static org.apache.cassandra.config.DatabaseDescriptor.getAccordJournalDirectory;
import static org.apache.cassandra.utils.Crc.crc32;

public class ReplayMarkers
{
    private static final Logger logger = LoggerFactory.getLogger(ReplayMarkers.class);
    public static final String startMarkerCrc = "startedCrc.marker";
    public static final String endMarkerCrc = "stoppedCrc.marker";
    public static final String startMarker = "started.marker";
    public static final String endMarker = "stopped.marker";

    public static File startMarker()
    {
        return new File(getAccordJournalDirectory(), startMarkerCrc);
    }

    public static File safeStopMarker()
    {
        return new File(getAccordJournalDirectory(), endMarkerCrc);
    }

    public static class ReplayMarkerData
    {
        public final long segmentId;
        public final long lastUniqueTimestamp;

        public ReplayMarkerData(long segmentId, long lastUniqueTimestamp)
        {
            this.segmentId = segmentId;
            this.lastUniqueTimestamp = lastUniqueTimestamp;
        }

        public static ReplayMarkerData invalidMarker() {
            return new ReplayMarkerData(-1L, -1L);
        }

        public long getSegmentId()
        {
            return segmentId;
        }

        public long getLastUniqueTimestamp()
        {
            return lastUniqueTimestamp;
        }

        public boolean isValid() {
            return segmentId != -1L && lastUniqueTimestamp != -1L;
        }
    }

    public static void writeMarker(File file, long timestamp, long lastUniqueTimestamp)
    {
        try (FileOutputStreamPlus out = new FileOutputStreamPlus(file))
        {
            CRC32 crc = crc32();
            out.writeLong(timestamp);
            FBUtilities.updateChecksumLong(crc, timestamp);
            out.writeLong(lastUniqueTimestamp);
            FBUtilities.updateChecksumLong(crc, lastUniqueTimestamp);
            out.writeInt((int) crc.getValue());
            out.sync();
        }
        catch (IOException e)
        {
            throw new UncheckedIOException(e);
        }
        trySyncJournalDirectory();
    }

    public static ReplayMarkerData readStartMarker()
    {
        File crcFile = new File(getAccordJournalDirectory(), startMarkerCrc);
        if (crcFile.exists())
            return readCrcMarker(crcFile);
        else
            return readMarker(new File(getAccordJournalDirectory(), startMarker));
    }

    public static ReplayMarkerData readStopMarker()
    {
        File crcFile = new File(getAccordJournalDirectory(), endMarkerCrc);
        if (crcFile.exists())
            return readCrcMarker(crcFile);
        else
            return readMarker(new File(getAccordJournalDirectory(), endMarker));
    }

    public static ReplayMarkerData readCrcMarker(File file)
    {
        if (!file.exists())
        {
            logger.debug("{} does not exist", file);
            return ReplayMarkerData.invalidMarker();
        }

        try (FileInputStreamPlus in = new FileInputStreamPlus(file))
        {
            CRC32 crc = crc32();
            long timestamp = in.readLong();
            FBUtilities.updateChecksumLong(crc, timestamp);
            long lastUniqueTimestamp = in.readLong();
            FBUtilities.updateChecksumLong(crc, lastUniqueTimestamp);
            int checksum = in.readInt();
            if (in.read() != -1 || (int) crc.getValue() != checksum)
            {
                logger.debug("{} is corrupted", file);
                return ReplayMarkerData.invalidMarker();
            }

            return new ReplayMarkerData(timestamp, lastUniqueTimestamp);
        }
        catch (IOException e)
        {
            logger.debug("Encountered IO exception {}, while reading {}", e, file);
            return ReplayMarkerData.invalidMarker();
        }
    }

    public static ReplayMarkerData readMarker(File file)
    {
        if (!file.exists())
        {
            logger.debug("{} does not exist", file);
            return ReplayMarkerData.invalidMarker();
        }

        try (FileInputStreamPlus in = new FileInputStreamPlus(file))
        {
            StringBuilder sb = new StringBuilder(8);
            for (int b = in.read(); b >= 0 ; b = in.read())
                sb.append((char)b);
            return new ReplayMarkerData(Long.parseLong(sb.toString()), -1L);
        }
        catch (IOException e)
        {
            logger.debug("Encountered IO exception {}, while reading {}", e, file);
            return ReplayMarkerData.invalidMarker();
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
