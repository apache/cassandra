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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
 * in each directory where sstables exist.
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
    private int directoryDescriptor;
    private final Map<String, String> errors = new HashMap<>();

    static LogReplica create(File directory, String fileName)
    {
        int folderFD = NativeLibrary.tryOpenDirectory(directory.getPath());
        if (folderFD == -1 && !FBUtilities.isWindows)
            throw new FSReadError(new IOException(String.format("Invalid folder descriptor trying to create log replica %s", directory.getPath())), directory.getPath());

        return new LogReplica(new File(fileName), folderFD);
    }

    static LogReplica open(File file)
    {
        int folderFD = NativeLibrary.tryOpenDirectory(file.getParentFile().getPath());
        if (folderFD == -1 && !FBUtilities.isWindows)
            throw new FSReadError(new IOException(String.format("Invalid folder descriptor trying to create log replica %s", file.getParentFile().getPath())), file.getParentFile().getPath());

        return new LogReplica(file, folderFD);
    }

    LogReplica(File file, int directoryDescriptor)
    {
        this.file = file;
        this.directoryDescriptor = directoryDescriptor;
    }

    File file()
    {
        return file;
    }

    List<String> readLines()
    {
        return FileUtils.readLines(file);
    }

    String getFileName()
    {
        return file.getName();
    }

    String getDirectory()
    {
        return file.getParent();
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
        // line, then sync the directory as well since now it must exist
        if (!existed)
            syncDirectory();
    }

    void syncDirectory()
    {
        try
        {
            if (directoryDescriptor >= 0)
                NativeLibrary.trySync(directoryDescriptor);
        }
        catch (FSError e)
        {
            logger.error("Failed to sync directory descriptor {}", directoryDescriptor, e);
            FileUtils.handleFSErrorAndPropagate(e);
        }
    }

    void delete()
    {
        LogTransaction.delete(file);
        syncDirectory();
    }

    boolean exists()
    {
        return file.exists();
    }

    public void close()
    {
        if (directoryDescriptor >= 0)
        {
            NativeLibrary.tryCloseFD(directoryDescriptor);
            directoryDescriptor = -1;
        }
    }

    @Override
    public String toString()
    {
        return String.format("[%s] ", file);
    }

    void setError(String line, String error)
    {
        errors.put(line, error);
    }

    void printContentsWithAnyErrors(StringBuilder str)
    {
        str.append(file.getPath());
        str.append(System.lineSeparator());
        FileUtils.readLines(file).forEach(line -> printLineWithAnyError(str, line));
    }

    private void printLineWithAnyError(StringBuilder str, String line)
    {
        str.append('\t');
        str.append(line);
        str.append(System.lineSeparator());

        String error = errors.get(line);
        if (error != null)
        {
            str.append("\t\t***");
            str.append(error);
            str.append(System.lineSeparator());
        }
    }
}
