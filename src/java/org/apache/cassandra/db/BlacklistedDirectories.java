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
import java.lang.management.ManagementFactory;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.cassandra.utils.JVMStabilityInspector;

public class BlacklistedDirectories implements BlacklistedDirectoriesMBean
{
    public static final String MBEAN_NAME = "org.apache.cassandra.db:type=BlacklistedDirectories";
    private static final Logger logger = LoggerFactory.getLogger(BlacklistedDirectories.class);
    private static final BlacklistedDirectories instance = new BlacklistedDirectories();

    private final Set<File> unreadableDirectories = new CopyOnWriteArraySet<File>();
    private final Set<File> unwritableDirectories = new CopyOnWriteArraySet<File>();

    private BlacklistedDirectories()
    {
        // Register this instance with JMX
        try
        {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            mbs.registerMBean(this, new ObjectName(MBEAN_NAME));
        }
        catch (Exception e)
        {
            JVMStabilityInspector.inspectThrowable(e);
            logger.error("error registering MBean {}", MBEAN_NAME, e);
            //Allow the server to start even if the bean can't be registered
        }
    }

    public Set<File> getUnreadableDirectories()
    {
        return Collections.unmodifiableSet(unreadableDirectories);
    }

    public Set<File> getUnwritableDirectories()
    {
        return Collections.unmodifiableSet(unwritableDirectories);
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
     * @return the blacklisted directory or null if nothing has been added to the list.
     */
    public static File maybeMarkUnreadable(File path)
    {
        File directory = getDirectory(path);
        if (instance.unreadableDirectories.add(directory))
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
        if (instance.unwritableDirectories.add(directory))
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
        return instance.unreadableDirectories.contains(directory);
    }

    /**
     * Tells whether or not the directory is blacklisted for writes.
     * @return whether or not the directory is blacklisted for reads.
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
            return file.getParentFile();

        // the file with path cannot be read - try determining the directory manually.
        if (file.getPath().endsWith(".db"))
            return file.getParentFile();

        // We may not be able to determine if it's a file or a directory if
        // we were called because we couldn't create the file/directory.
        return file;
    }
}
