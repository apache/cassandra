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

package org.apache.cassandra.db;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.FileUtils;


public class WindowsFailedSnapshotTracker
{
    private static final Logger logger = LoggerFactory.getLogger(WindowsFailedSnapshotTracker.class);
    private static PrintWriter _failedSnapshotFile;

    @VisibleForTesting
    // Need to handle null for unit tests
    public static final String TODELETEFILE = System.getenv("CASSANDRA_HOME") == null
                 ? ".toDelete"
                 : System.getenv("CASSANDRA_HOME") + File.separator + ".toDelete";

    public static void deleteOldSnapshots()
    {
        if (new File(TODELETEFILE).exists())
        {
            try
            {
                try (BufferedReader reader = new BufferedReader(new FileReader(TODELETEFILE)))
                {
                    String snapshotDirectory;
                    while ((snapshotDirectory = reader.readLine()) != null)
                    {
                        File f = new File(snapshotDirectory);

                        // Skip folders that aren't a subset of temp or a data folder. We don't want people to accidentally
                        // delete something important by virtue of adding something invalid to the .toDelete file.
                        boolean validFolder = FileUtils.isSubDirectory(new File(System.getenv("TEMP")), f);
                        for (String s : DatabaseDescriptor.getAllDataFileLocations())
                            validFolder |= FileUtils.isSubDirectory(new File(s), f);

                        if (!validFolder)
                        {
                            logger.warn("Skipping invalid directory found in .toDelete: {}. Only %TEMP% or data file subdirectories are valid.", f);
                            continue;
                        }

                        // Could be a non-existent directory if deletion worked on previous JVM shutdown.
                        if (f.exists())
                        {
                            logger.warn("Discovered obsolete snapshot. Deleting directory [{}]", snapshotDirectory);
                            FileUtils.deleteRecursive(new File(snapshotDirectory));
                        }
                    }
                }

                // Only delete the old .toDelete file if we succeed in deleting all our known bad snapshots.
                Files.delete(Paths.get(TODELETEFILE));
            }
            catch (IOException e)
            {
                logger.warn("Failed to open {}. Obsolete snapshots from previous runs will not be deleted.", TODELETEFILE, e);
            }
        }

        try
        {
            _failedSnapshotFile = new PrintWriter(new FileWriter(TODELETEFILE, true));
        }
        catch (IOException e)
        {
            throw new RuntimeException(String.format("Failed to create failed snapshot tracking file [%s]. Aborting", TODELETEFILE));
        }
    }

    public static synchronized void handleFailedSnapshot(File dir)
    {
        assert _failedSnapshotFile != null : "_failedSnapshotFile not initialized within WindowsFailedSnapshotTracker";
        FileUtils.deleteRecursiveOnExit(dir);
        _failedSnapshotFile.println(dir.toString());
        _failedSnapshotFile.flush();
    }

    @VisibleForTesting
    public static void resetForTests()
    {
        _failedSnapshotFile.close();
    }
}
