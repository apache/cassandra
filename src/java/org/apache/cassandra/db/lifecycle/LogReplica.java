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

package org.apache.cassandra.db.lifecycle;

import java.io.File;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.FSError;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.NativeLibrary;

/**
 * Because a column family may have sstables on different disks and disks can
 * be removed, we duplicate log files into many replicas so as to have a file
 * in each folder where sstables exist.
 *
 * Each replica contains the exact same content but we do allow for final
 * partial records in case we crashed after writing to one replica but
 * before compliting the write to another replica.
 *
 * @see LogFile
 */
final class LogReplica implements AutoCloseable
{
    private static final Logger logger = LoggerFactory.getLogger(LogReplica.class);

    private final File file;
    private int folderDescriptor;

    static LogReplica create(File folder, String fileName)
    {
        int folderFD = NativeLibrary.tryOpenDirectory(folder.getPath());
        if (folderFD == -1 && !FBUtilities.isWindows())
            throw new FSReadError(new IOException(String.format("Invalid folder descriptor trying to create log replica %s", folder.getPath())), folder.getPath());

        return new LogReplica(new File(fileName), folderFD);
    }

    static LogReplica open(File file)
    {
        int folderFD = NativeLibrary.tryOpenDirectory(file.getParentFile().getPath());
        if (folderFD == -1 && !FBUtilities.isWindows())
            throw new FSReadError(new IOException(String.format("Invalid folder descriptor trying to create log replica %s", file.getParentFile().getPath())), file.getParentFile().getPath());

        return new LogReplica(file, folderFD);
    }

    LogReplica(File file, int folderDescriptor)
    {
        this.file = file;
        this.folderDescriptor = folderDescriptor;
    }

    File file()
    {
        return file;
    }

    void append(LogRecord record)
    {
        boolean existed = exists();
        try
        {
            FileUtils.appendAndSync(file, record.toString());
        }
        catch (FSError e)
        {
            logger.error("Failed to sync file {}", file, e);
            FileUtils.handleFSErrorAndPropagate(e);
        }

        // If the file did not exist before appending the first
        // line, then sync the folder as well since now it must exist
        if (!existed)
            syncFolder();
    }

    void syncFolder()
    {
        try
        {
            if (folderDescriptor >= 0)
                NativeLibrary.trySync(folderDescriptor);
        }
        catch (FSError e)
        {
            logger.error("Failed to sync directory descriptor {}", folderDescriptor, e);
            FileUtils.handleFSErrorAndPropagate(e);
        }
    }

    void delete()
    {
        LogTransaction.delete(file);
        syncFolder();
    }

    boolean exists()
    {
        return file.exists();
    }

    public void close()
    {
        if (folderDescriptor >= 0)
        {
            NativeLibrary.tryCloseFD(folderDescriptor);
            folderDescriptor = -1;
        }
    }

    @Override
    public String toString()
    {
        return String.format("[%s] ", file);
    }
}
