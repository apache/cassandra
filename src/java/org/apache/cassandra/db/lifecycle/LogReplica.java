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

import org.apache.cassandra.io.util.FileUtils;
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
    private final File file;
    private int directoryDescriptor;
    private final Map<String, String> errors = new HashMap<>();

    static LogReplica create(File directory, String fileName)
    {
        return new LogReplica(new File(fileName), NativeLibrary.tryOpenDirectory(directory.getPath()));
    }

    static LogReplica open(File file)
    {
        return new LogReplica(file, NativeLibrary.tryOpenDirectory(file.getParentFile().getPath()));
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
        FileUtils.appendAndSync(file, record.toString());

        // If the file did not exist before appending the first
        // line, then sync the directory as well since now it must exist
        if (!existed)
            syncDirectory();
    }

    void syncDirectory()
    {
        if (directoryDescriptor >= 0)
            NativeLibrary.trySync(directoryDescriptor);
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
