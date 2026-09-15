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
    public static final String stopMarkerCrc = "stoppedCrc.marker";
    public static final String startMarker = "started.marker";
    public static final String stopMarker = "stopped.marker";

    public static File startMarker()
    {
        return new File(getAccordJournalDirectory(), startMarkerCrc);
    }

    public static File safeStopMarker()
    {
        return new File(getAccordJournalDirectory(), stopMarkerCrc);
    }

    public static class StartMarker
    {
        public final long segmentId;

        public StartMarker(long segmentId)
        {
            this.segmentId = segmentId;
        }

        public static StartMarker invalidMarker() {
            return new StartMarker(-1L);
        }

        public boolean isValid() {
            return segmentId != -1L;
        }

        public long getSegmentId()
        {
            return segmentId;
        }
    }

    public static class StopMarker
    {
        public final long segmentId;
        public final long lastUniqueTimestamp;

        public StopMarker(long segmentId, long lastUniqueTimestamp)
        {
            this.segmentId = segmentId;
            this.lastUniqueTimestamp = lastUniqueTimestamp;
        }

        public static StopMarker invalidMarker() {
            return new StopMarker(-1L, -1L);
        }

        public boolean isValid() {
            return segmentId != -1L && lastUniqueTimestamp != -1L;
        }

        public long getSegmentId()
        {
            return segmentId;
        }

        public long getLastUniqueTimestamp()
        {
            return lastUniqueTimestamp;
        }
    }

    public static void writeStartMarker(File file, long segmentId)
    {
        try (FileOutputStreamPlus out = new FileOutputStreamPlus(file))
        {
            CRC32 crc = crc32();
            out.writeLong(segmentId);
            FBUtilities.updateChecksumLong(crc, segmentId);
            out.writeInt((int) crc.getValue());
            out.sync();
        }
        catch (IOException e)
        {
            throw new UncheckedIOException(e);
        }
        trySyncJournalDirectory();
    }

    public static void writeStopMarker(File file, long segmentId, long lastUniqueTimestamp)
    {
        try (FileOutputStreamPlus out = new FileOutputStreamPlus(file))
        {
            CRC32 crc = crc32();
            out.writeLong(segmentId);
            FBUtilities.updateChecksumLong(crc, segmentId);
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

    public static StartMarker readStartMarker()
    {
        File crcFile = new File(getAccordJournalDirectory(), startMarkerCrc);
        if (crcFile.exists())
            return readCrcStartMarker(crcFile);
        else
            return readStartMarker(new File(getAccordJournalDirectory(), startMarker));
    }

    public static StopMarker readStopMarker()
    {
        File crcFile = new File(getAccordJournalDirectory(), stopMarkerCrc);
        if (crcFile.exists())
            return readCrcStopMarker(crcFile);
        else
            return readStopMarker(new File(getAccordJournalDirectory(), stopMarker));
    }

    public static StopMarker readCrcStopMarker(File file)
    {
        if (!file.exists())
        {
            logger.debug("{} does not exist", file);
            return StopMarker.invalidMarker();
        }

        try (FileInputStreamPlus in = new FileInputStreamPlus(file))
        {
            CRC32 crc = crc32();
            long segmentId = in.readLong();
            FBUtilities.updateChecksumLong(crc, segmentId);
            long lastUniqueTimestamp = in.readLong();
            FBUtilities.updateChecksumLong(crc, lastUniqueTimestamp);
            int checksum = in.readInt();
            if (in.read() != -1 || (int) crc.getValue() != checksum)
            {
                logger.debug("{} is corrupted", file);
                return StopMarker.invalidMarker();
            }

            return new StopMarker(segmentId, lastUniqueTimestamp);
        }
        catch (IOException e)
        {
            logger.debug("Encountered IO exception {}, while reading {}", e, file);
            return StopMarker.invalidMarker();
        }
    }

    public static StartMarker readCrcStartMarker(File file)
    {
        if (!file.exists())
        {
            logger.debug("{} does not exist", file);
            return StartMarker.invalidMarker();
        }

        try (FileInputStreamPlus in = new FileInputStreamPlus(file))
        {
            CRC32 crc = crc32();
            long segmentId = in.readLong();
            FBUtilities.updateChecksumLong(crc, segmentId);
            int checksum = in.readInt();
            if (in.read() != -1 || (int) crc.getValue() != checksum)
            {
                logger.debug("{} is corrupted", file);
                return StartMarker.invalidMarker();
            }

            return new StartMarker(segmentId);
        }
        catch (IOException e)
        {
            logger.debug("Encountered IO exception {}, while reading {}", e, file);
            return StartMarker.invalidMarker();
        }
    }

    public static StopMarker readStopMarker(File file)
    {
        if (!file.exists())
        {
            logger.debug("{} does not exist", file);
            return StopMarker.invalidMarker();
        }

        try (FileInputStreamPlus in = new FileInputStreamPlus(file))
        {
            StringBuilder sb = new StringBuilder(8);
            for (int b = in.read(); b >= 0 ; b = in.read())
                sb.append((char)b);
            return new StopMarker(Long.parseLong(sb.toString()), -1L);
        }
        catch (IOException e)
        {
            logger.debug("Encountered IO exception {}, while reading {}", e, file);
            return StopMarker.invalidMarker();
        }
    }

    public static StartMarker readStartMarker(File file)
    {
        if (!file.exists())
        {
            logger.debug("{} does not exist", file);
            return StartMarker.invalidMarker();
        }

        try (FileInputStreamPlus in = new FileInputStreamPlus(file))
        {
            StringBuilder sb = new StringBuilder(8);
            for (int b = in.read(); b >= 0 ; b = in.read())
                sb.append((char)b);
            return new StartMarker(Long.parseLong(sb.toString()));
        }
        catch (IOException e)
        {
            logger.debug("Encountered IO exception {}, while reading {}", e, file);
            return StartMarker.invalidMarker();
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
