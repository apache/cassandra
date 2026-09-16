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
    public static final String startMarker = "started";
    public static final String stopMarker = "stopped";

    public static File startMarker()
    {
        return new File(getAccordJournalDirectory(), startMarkerCrc);
    }

    public static File safeStopMarker()
    {
        return new File(getAccordJournalDirectory(), stopMarkerCrc);
    }

    public enum State
    {
        DOES_NOT_EXIST, // The marker file can not be found
        INVALID, // The marker file is corrupted or an exception was thrown while deserializing it from the file
        VALID
    }

    /**
     * Determines if the stopMarker is in a correct position with regard to the startMarker
     */
    public static boolean isValid(StartMarker startMarker, StopMarker stopMarker)
    {
        State startMarkerState = startMarker.getState();
        State stopMarkerState = stopMarker.getState();

        // Initial state, when we first start up neither file exists yet
        if (startMarkerState == State.DOES_NOT_EXIST && stopMarkerState == State.DOES_NOT_EXIST)
            return true;

        return startMarkerState == State.VALID && stopMarkerState == State.VALID && startMarker.getSegmentId() <= stopMarker.getSegmentId();
    }

    public static class StartMarker
    {
        private final State state;
        private final long segmentId;

        private StartMarker(State state, long segmentId)
        {
            this.state = state;
            this.segmentId = segmentId;
        }

        public static StartMarker validMarker(long segmentId)
        {
            return new StartMarker(State.VALID, segmentId);
        }

        public static StartMarker doesNotExistMarker()
        {
            return new StartMarker(State.DOES_NOT_EXIST, -1L);
        }

        public static StartMarker invalidMarker()
        {
            return new StartMarker(State.INVALID, -1L);
        }

        public State getState()
        {
            return state;
        }

        public long getSegmentId()
        {
            return segmentId;
        }
    }

    public static class StopMarker
    {
        private final State state;
        private final long segmentId;
        private final long lastUniqueTimestamp;

        private StopMarker(State state, long segmentId, long lastUniqueTimestamp)
        {
            this.state = state;
            this.segmentId = segmentId;
            this.lastUniqueTimestamp = lastUniqueTimestamp;
        }

        public static StopMarker validMarker(long segmentId, long lastUniqueTimestamp)
        {
            return new StopMarker(State.VALID, segmentId, lastUniqueTimestamp);
        }

        public static StopMarker doesNotExistMarker()
        {
            return new StopMarker(State.DOES_NOT_EXIST, -1L, -1L);
        }

        public static StopMarker invalidMarker()
        {
            return new StopMarker(State.INVALID, -1L, -1L);
        }

        public State getState()
        {
            return state;
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
        File tempFile = new File(file.path() + ".tmp");

        try
        {
            try (FileOutputStreamPlus out = new FileOutputStreamPlus(tempFile))
            {
                CRC32 crc = crc32();
                out.writeLong(segmentId);
                FBUtilities.updateChecksumLong(crc, segmentId);
                out.writeInt((int) crc.getValue());
                out.flush();
                out.sync();
            }

            tempFile.move(file);
        }
        catch (IOException e)
        {
            throw new UncheckedIOException(e);
        }
        trySyncJournalDirectory();
    }

    public static void writeStopMarker(File file, long segmentId, long lastUniqueTimestamp)
    {
        File tempFile = new File(file.path() + ".tmp");

        try
        {
            try (FileOutputStreamPlus out = new FileOutputStreamPlus(tempFile))
            {
                CRC32 crc = crc32();
                out.writeLong(segmentId);
                FBUtilities.updateChecksumLong(crc, segmentId);
                out.writeLong(lastUniqueTimestamp);
                FBUtilities.updateChecksumLong(crc, lastUniqueTimestamp);
                out.writeInt((int) crc.getValue());
                out.flush();
                out.sync();
            }

            tempFile.move(file);
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
            return StopMarker.doesNotExistMarker();
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
                logger.warn("{} is corrupted", file);
                return StopMarker.invalidMarker();
            }

            return StopMarker.validMarker(segmentId, lastUniqueTimestamp);
        }
        catch (IOException e)
        {
            logger.warn("Encountered IO exception while reading {}", file, e);
            return StopMarker.invalidMarker();
        }
    }

    public static StartMarker readCrcStartMarker(File file)
    {
        if (!file.exists())
        {
            logger.debug("{} does not exist", file);
            return StartMarker.doesNotExistMarker();
        }

        try (FileInputStreamPlus in = new FileInputStreamPlus(file))
        {
            CRC32 crc = crc32();
            long segmentId = in.readLong();
            FBUtilities.updateChecksumLong(crc, segmentId);
            int checksum = in.readInt();
            if (in.read() != -1 || (int) crc.getValue() != checksum)
            {
                logger.warn("{} is corrupted", file);
                return StartMarker.invalidMarker();
            }

            return StartMarker.validMarker(segmentId);
        }
        catch (IOException e)
        {
            logger.warn("Encountered IO exception while reading {}", file, e);
            return StartMarker.invalidMarker();
        }
    }

    public static StopMarker readStopMarker(File file)
    {
        if (!file.exists())
        {
            logger.debug("{} does not exist", file);
            return StopMarker.doesNotExistMarker();
        }

        try (FileInputStreamPlus in = new FileInputStreamPlus(file))
        {
            StringBuilder sb = new StringBuilder(8);
            for (int b = in.read(); b >= 0 ; b = in.read())
                sb.append((char)b);
            return StopMarker.validMarker(Long.parseLong(sb.toString()), -1L);
        }
        catch (IOException e)
        {
            logger.warn("Encountered IO exception while reading {}", file, e);
            return StopMarker.invalidMarker();
        }
        catch (NumberFormatException e)
        {
            logger.warn("Encountered NumberFormatException exception while reading {}", file, e);
            return StopMarker.invalidMarker();
        }
    }

    public static StartMarker readStartMarker(File file)
    {
        if (!file.exists())
        {
            logger.debug("{} does not exist", file);
            return StartMarker.doesNotExistMarker();
        }

        try (FileInputStreamPlus in = new FileInputStreamPlus(file))
        {
            StringBuilder sb = new StringBuilder(8);
            for (int b = in.read(); b >= 0 ; b = in.read())
                sb.append((char)b);
            return StartMarker.validMarker(Long.parseLong(sb.toString()));
        }
        catch (IOException e)
        {
            logger.warn("Encountered IO exception while reading {}", file, e);
            return StartMarker.invalidMarker();
        }
        catch (NumberFormatException e)
        {
            logger.warn("Encountered NumberFormatException exception while reading {}", file, e);
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
        try
        {
            NativeLibrary.trySync(fd);
        }
        finally
        {
            NativeLibrary.tryCloseFD(fd);
        }
    }

    public static File saveDirectory()
    {
        return new File(getAccordJournalDirectory(), "save_state");
    }
}
