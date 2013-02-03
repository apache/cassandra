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
package org.apache.cassandra.io.util;

import java.io.File;

import org.apache.cassandra.db.Directories;
import org.apache.cassandra.utils.WrappedRunnable;

public abstract class DiskAwareRunnable extends WrappedRunnable
{
    /**
     * Run this task after selecting the optimal disk for it
     */
    protected void runMayThrow() throws Exception
    {
        long writeSize;
        Directories.DataDirectory directory;
        while (true)
        {
            writeSize = getExpectedWriteSize();
            directory = getDirectories().getLocationCapableOfSize(writeSize);
            if (directory != null || !reduceScopeForLimitedSpace())
                break;
        }
        if (directory == null)
            throw new RuntimeException("Insufficient disk space to write " + writeSize + " bytes");

        directory.currentTasks.incrementAndGet();
        directory.estimatedWorkingSize.addAndGet(writeSize);
        try
        {
            runWith(getDirectories().getLocationForDisk(directory));
        }
        finally
        {
            directory.estimatedWorkingSize.addAndGet(-1 * writeSize);
            directory.currentTasks.decrementAndGet();
        }
    }

    /**
     * Get sstable directories for the CF.
     * @return Directories instance for the CF.
     */
    protected abstract Directories getDirectories();

    /**
     * Executes this task on given {@code sstableDirectory}.
     * @param sstableDirectory sstable directory to work on
     */
    protected abstract void runWith(File sstableDirectory) throws Exception;

    /**
     * Get expected write size to determine which disk to use for this task.
     * @return expected size in bytes this task will write to disk.
     */
    public abstract long getExpectedWriteSize();

    /**
     * Called if no disk is available with free space for the full write size.
     * @return true if the scope of the task was successfully reduced.
     */
    public boolean reduceScopeForLimitedSpace()
    {
        return false;
    }
}
