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

import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileInputStreamPlus;
import org.apache.cassandra.io.util.FileOutputStreamPlus;
import org.apache.cassandra.utils.NativeLibrary;

import static org.apache.cassandra.config.DatabaseDescriptor.getAccordJournalDirectory;

public class ReplayMarkers
{
    public static File startMarker()
    {
        return new File(getAccordJournalDirectory(), "started");
    }

    public static File safeStopMarker()
    {
        return new File(getAccordJournalDirectory(), "stopped");
    }

    // TODO (required): add checksummed version and default to this (but support unchecksummed for manual editing)
    static void writeMarker(File file, long timestamp)
    {
        try (FileOutputStreamPlus out = new FileOutputStreamPlus(file))
        {
            out.writeBytes(Long.toString(timestamp));
        }
        catch (IOException e)
        {
            throw new UncheckedIOException(e);
        }
        trySyncJournalDirectory();
    }

    public static long readStartMarker()
    {
        return readMarker(startMarker());
    }

    public static long readStopMarker()
    {
        return readMarker(safeStopMarker());
    }

    public static long readMarker(File file)
    {
        if (!file.exists())
            return -1L;

        try (FileInputStreamPlus in = new FileInputStreamPlus(file))
        {
            StringBuilder sb = new StringBuilder(8);
            for (int b = in.read(); b >= 0 ; b = in.read())
                sb.append((char)b);
            return Long.parseLong(sb.toString());
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
        NativeLibrary.tryCloseFD(fd);
    }

    public static File saveDirectory()
    {
        return new File(getAccordJournalDirectory(), "save_state");
    }
}
