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

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.io.util.File;
import org.apache.cassandra.utils.MBeanWrapper;

public class DisallowedDirectories implements DisallowedDirectoriesMBean
{
    public static final String DEPRECATED_MBEAN_NAME = "org.apache.cassandra.db:type=BlacklistedDirectories";
    public static final String MBEAN_NAME = "org.apache.cassandra.db:type=DisallowedDirectories";
    private static final Logger logger = LoggerFactory.getLogger(DisallowedDirectories.class);
    private static final DisallowedDirectories instance = new DisallowedDirectories();

    private final Set<File> unreadableDirectories = new CopyOnWriteArraySet<File>();
    private final Set<File> unwritableDirectories = new CopyOnWriteArraySet<File>();

    private static final AtomicInteger directoriesVersion = new AtomicInteger();

    private DisallowedDirectories()
    {
        // Register this instance with JMX
        MBeanWrapper.instance.registerMBean(this, DEPRECATED_MBEAN_NAME, MBeanWrapper.OnException.LOG);
        MBeanWrapper.instance.registerMBean(this, MBEAN_NAME, MBeanWrapper.OnException.LOG);
    }

    @Override
    public Set<java.io.File> getUnreadableDirectories()
    {
        return toJmx(unreadableDirectories);
    }

    @Override
    public Set<java.io.File> getUnwritableDirectories()
    {
        return toJmx(unwritableDirectories);
    }

    private static Set<java.io.File> toJmx(Set<File> set)
    {
        return set.stream().map(f -> f.toPath().toFile()).collect(Collectors.toSet()); // checkstyle: permit this invocation
    }

    public void markUnreadable(String path)
    {
        maybeMarkUnreadable(new File(path));
    }

    public void markUnwritable(String path)
    {
        maybeMarkUnwritable(new File(path));
    }

    /**
     * Adds parent directory of the file (or the file itself, if it is a directory)
     * to the set of unreadable directories.
     *
     * @return the disallowed directory or null if nothing has been added to the list.
     */
    public static File maybeMarkUnreadable(File path)
    {
        File directory = getDirectory(path);
        if (instance.unreadableDirectories.add(directory))
        {
            directoriesVersion.incrementAndGet();
            logger.warn("Disallowing {} for reads", directory);
            return directory;
        }
        return null;
    }

    /**
     * Adds parent directory of the file (or the file itself, if it is a directory)
     * to the set of unwritable directories.
     *
     * @return the disallowed directory or null if nothing has been added to the list.
     */
    public static File maybeMarkUnwritable(File path)
    {
        File directory = getDirectory(path);
        if (instance.unwritableDirectories.add(directory))
        {
            directoriesVersion.incrementAndGet();
            logger.warn("Disallowing {} for writes", directory);
            return directory;
        }
        return null;
    }

    public static int getDirectoriesVersion()
    {
        return directoriesVersion.get();
    }

    /**
     * Testing only!
     * Clear the set of unwritable directories.
     */
    @VisibleForTesting
    public static void clearUnwritableUnsafe()
    {
        instance.unwritableDirectories.clear();
    }


    /**
     * Tells whether or not the directory is disallowed for reads.
     * @return whether or not the directory is disallowed for reads.
     */
    public static boolean isUnreadable(File directory)
    {
        return instance.unreadableDirectories.contains(directory);
    }

    /**
     * Tells whether or not the directory is disallowed for writes.
     * @return whether or not the directory is disallowed for reads.
     */
    public static boolean isUnwritable(File directory)
    {
        return instance.unwritableDirectories.contains(directory);
    }

    private static File getDirectory(File file)
    {
        if (file.isDirectory())
            return file;

        if (file.isFile())
            return file.parent();

        // the file with path cannot be read - try determining the directory manually.
        if (file.path().endsWith(".db"))
            return file.parent();

        // We may not be able to determine if it's a file or a directory if
        // we were called because we couldn't create the file/directory.
        return file;
    }
}
