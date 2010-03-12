/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra;

import java.io.File;
import java.io.IOException;

import org.junit.BeforeClass;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.FileUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CleanupHelper
{
    private static Logger logger = LoggerFactory.getLogger(CleanupHelper.class);

    @BeforeClass
    public static void cleanupAndLeaveDirs() throws IOException
    {
        mkdirs();
        cleanup();
        mkdirs();
    }

    public static void cleanup() throws IOException
    {
        // clean up commitlog
        String[] directoryNames = {
                DatabaseDescriptor.getLogFileLocation(),
        };
        for (String dirName : directoryNames)
        {
            File dir = new File(dirName);
            if (!dir.exists())
            {
                throw new RuntimeException("No such directory: " + dir.getAbsolutePath());
            }
            for (File f : dir.listFiles())
            {
                FileUtils.deleteWithConfirm(f);
            }
            FileUtils.deleteWithConfirm(dir);
        }

        // clean up data directory which are stored as data directory/table/data files
        for (String dirName : DatabaseDescriptor.getAllDataFileLocations())
        {
            File dir = new File(dirName);
            if (!dir.exists())
            {
                throw new RuntimeException("No such directory: " + dir.getAbsolutePath());
            }
            for (File tableFile : dir.listFiles())
            {
                // table directory
                if (tableFile.isDirectory())
                {
                    for (File dataFile : tableFile.listFiles())
                    {
                        FileUtils.deleteWithConfirm(dataFile);
                    }
                }
                FileUtils.deleteWithConfirm(tableFile);
            }
            FileUtils.deleteWithConfirm(dir);
        }
    }

    public static void mkdirs()
    {
        try
        {
            DatabaseDescriptor.createAllDirectories();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }
}
