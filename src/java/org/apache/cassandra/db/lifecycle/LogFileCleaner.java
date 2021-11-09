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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.util.File;

final class LogFileCleaner implements ILogFileCleaner
{
    private static final Logger logger = LoggerFactory.getLogger(LogFileCleaner.class);

    // This maps a transaction log file name to a list of physical files. Each sstable
    // can have multiple directories and a transaction is trakced by identical transaction log
    // files, one per directory. So for each transaction file name we can have multiple
    // physical files.
    Map<String, List<File>> files = new HashMap<>();

    @Override
    public void list(File directory)
    {
        Arrays.stream(directory.tryList(LogFile::isLogFile)).forEach(this::add);
    }

    void add(File file)
    {
        List<File> filesByName = files.get(file.name());
        if (filesByName == null)
        {
            filesByName = new ArrayList<>();
            files.put(file.name(), filesByName);
        }

        filesByName.add(file);
    }

    @Override
    public boolean removeUnfinishedLeftovers()
    {
        return files.entrySet()
                    .stream()
                    .map(LogFileCleaner::removeUnfinishedLeftovers)
                    .allMatch(Predicate.isEqual(true));
    }

    static boolean removeUnfinishedLeftovers(Map.Entry<String, List<File>> entry)
    {
        try(LogFile txn = LogFile.make(entry.getKey(), entry.getValue()))
        {
            logger.info("Verifying logfile transaction {}", txn);
            if (txn.verify())
            {
                Throwable failure = txn.removeUnfinishedLeftovers(null);
                if (failure != null)
                {
                    logger.error("Failed to remove unfinished transaction leftovers for transaction log {}",
                                 txn.toString(true), failure);
                    return false;
                }

                return true;
            }
            else
            {
                logger.error("Unexpected disk state: failed to read transaction log {}, " +
                             "check logs before last shutdown for any errors, and ensure txn log files were not edited manually.",
                             txn.toString(true));
                return false;
            }
        }
    }
}