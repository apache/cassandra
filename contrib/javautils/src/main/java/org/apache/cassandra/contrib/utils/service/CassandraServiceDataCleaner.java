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

package org.apache.cassandra.contrib.utils.service;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.io.util.FileUtils;

/**
 * A cleanup utility that wipes the cassandra data directories.
 *
 * @author Ran Tavory (rantav@gmail.com)
 *
 */
public class CassandraServiceDataCleaner {

    /**
     * Creates all data dir if they don't exist and cleans them
     * @throws IOException
     */
    public void prepare() throws IOException {
        makeDirsIfNotExist();
        cleanupDataDirectories();
        CommitLog.instance.resetUnsafe();
    }

    /**
     * Deletes all data from cassandra data directories, including the commit log.
     * @throws IOException in case of permissions error etc.
     */
    public void cleanupDataDirectories() throws IOException {
        for (String s: getDataDirs()) {
            cleanDir(s);
        }
    }
    /**
     * Creates the data diurectories, if they didn't exist.
     * @throws IOException if directories cannot be created (permissions etc).
     */
    public void makeDirsIfNotExist() throws IOException {
        DatabaseDescriptor.createAllDirectories();
    }

    /**
     * Collects all data dirs and returns a set of String paths on the file system.
     *
     * @return
     */
    private Set<String> getDataDirs() {
        Set<String> dirs = new HashSet<String>();
        for (String s : DatabaseDescriptor.getAllDataFileLocations()) {
            dirs.add(s);
        }
        dirs.add(DatabaseDescriptor.getCommitLogLocation());
        return dirs;
    }

    /**
     * Removes all directory content from the file system
     *
     * @param dir
     * @throws IOException
     */
    private void cleanDir(String dir) throws IOException {
        File dirFile = new File(dir);
        if (dirFile.exists() && dirFile.isDirectory()) {
            for (File f : dirFile.listFiles()) {
                FileUtils.deleteRecursive(f);
            }
        }
    }
}
