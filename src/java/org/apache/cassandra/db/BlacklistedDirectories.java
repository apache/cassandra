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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

public class BlacklistedDirectories
{
    private static final Logger logger = LoggerFactory.getLogger(BlacklistedDirectories.class);

    private static Set<File> unreadableDirectories = new CopyOnWriteArraySet<File>();
    private static Set<File> unwritableDirectories = new CopyOnWriteArraySet<File>();

    /**
     * Adds parent directory of the file (or the file itself, if it is a directory)
     * to the set of unreadable directories.
     *
     * @return the blacklisted directory or null if nothing has been added to the list.
     */
    public static File maybeMarkUnreadable(File path)
    {
        File directory = getDirectory(path);
        if (unreadableDirectories.add(directory))
        {
            logger.warn("Blacklisting {} for reads", directory);
            return directory;
        }
        return null;
    }

    /**
     * Adds parent directory of the file (or the file itself, if it is a directory)
     * to the set of unwritable directories.
     *
     * @return the blacklisted directory or null if nothing has been added to the list.
     */
    public static File maybeMarkUnwritable(File path)
    {
        File directory = getDirectory(path);
        if (unwritableDirectories.add(directory))
        {
            logger.warn("Blacklisting {} for writes", directory);
            return directory;
        }
        return null;
    }

    /**
     * Tells whether or not the directory is blacklisted for reads.
     * @return whether or not the directory is blacklisted for reads.
     */
    public static boolean isUnreadable(File directory)
    {
        return unreadableDirectories.contains(directory);
    }

    /**
     * Tells whether or not the directory is blacklisted for writes.
     * @return whether or not the directory is blacklisted for reads.
     */
    public static boolean isUnwritable(File directory)
    {
        return unwritableDirectories.contains(directory);
    }

    private static File getDirectory(File file)
    {
        if (file.isDirectory())
            return file;

        if (file.isFile())
            return file.getParentFile();

        // the file with path cannot be read - try determining the directory manually.
        if (file.getPath().endsWith(".db"))
            return file.getParentFile();

        throw new IllegalStateException("Unable to parse directory from path " + file);
    }
}
