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
package org.apache.cassandra.utils;

import java.io.*;
import java.lang.management.ManagementFactory;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.text.StrBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility to log heap histogram.
 *
 */
public final class HeapUtils
{
    private static final Logger logger = LoggerFactory.getLogger(HeapUtils.class);

    /**
     * Generates a HEAP dump in the directory specified by the <code>HeapDumpPath</code> JVM option
     * or in the <code>CASSANDRA_HOME</code> directory.
     */
    public static void logHeapHistogram()
    {
        try
        {
            logger.info("Trying to log the heap histogram using jmap");

            Long processId = getProcessId();
            if (processId == null)
            {
                logger.error("The process ID could not be retrieved. Skipping heap histogram generation.");
                return;
            }

            String jmapPath = getJmapPath();

            // The jmap file could not be found. In this case let's default to jmap in the hope that it is in the path.
            String jmapCommand = jmapPath == null ? "jmap" : jmapPath;

            String[] histoCommands = new String[] {jmapCommand,
                    "-histo",
                    processId.toString()};

            logProcessOutput(Runtime.getRuntime().exec(histoCommands));
        }
        catch (Throwable e)
        {
            logger.error("The heap histogram could not be generated due to the following error: ", e);
        }
    }

    /**
     * Retrieve the path to the JMAP executable.
     * @return the path to the JMAP executable or null if it cannot be found.
     */
    private static String getJmapPath()
    {
        // Searching in the JAVA_HOME is safer than searching into System.getProperty("java.home") as the Oracle
        // JVM might use the JRE which do not contains jmap.
        String javaHome = System.getenv("JAVA_HOME");
        if (javaHome == null)
            return null;

        File javaBinDirectory = new File(javaHome, "bin");
        File[] files = javaBinDirectory.listFiles(new FilenameFilter()
        {
            public boolean accept(File dir, String name)
            {
                return name.startsWith("jmap");
            }
        });
        return ArrayUtils.isEmpty(files) ? null : files[0].getPath();
    }

    /**
     * Logs the output of the specified process.
     *
     * @param p the process
     * @throws IOException if an I/O problem occurs
     */
    private static void logProcessOutput(Process p) throws IOException
    {
        BufferedReader input = new BufferedReader(new InputStreamReader(p.getInputStream()));

        StrBuilder builder = new StrBuilder();
        String line;
        while ((line = input.readLine()) != null)
        {
            builder.appendln(line);
        }
        logger.info(builder.toString());
    }

    /**
     * Retrieves the process ID or <code>null</code> if the process ID cannot be retrieved.
     * @return the process ID or <code>null</code> if the process ID cannot be retrieved.
     */
    private static Long getProcessId()
    {
        // Once Java 9 is ready the process API should provide a better way to get the process ID.
        long pid = SigarLibrary.instance.getPid();

        if (pid >= 0)
            return Long.valueOf(pid);

        return getProcessIdFromJvmName();
    }

    /**
     * Retrieves the process ID from the JVM name.
     * @return the process ID or <code>null</code> if the process ID cannot be retrieved.
     */
    private static Long getProcessIdFromJvmName()
    {
        // the JVM name in Oracle JVMs is: '<pid>@<hostname>' but this might not be the case on all JVMs
        String jvmName = ManagementFactory.getRuntimeMXBean().getName();
        try
        {
            return Long.parseLong(jvmName.split("@")[0]);
        }
        catch (NumberFormatException e)
        {
            // ignore
        }
        return null;
    }

    /**
     * The class must not be instantiated.
     */
    private HeapUtils()
    {
    }
}
