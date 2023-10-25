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

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CassandraRelevantProperties;
import oshi.PlatformEnum;

/**
 * An abstraction of System information, this class provides access to system information without specifying how
 * it is retrieved.
 */
public class SystemInfo
{

    private static long INFINITY = -1l;
    private static long EXPECTED_MIN_NOFILE = 10000l; // number of files that can be opened
    private static long EXPECTED_NPROC = 32768l; // number of processes
    private static long EXPECTED_AS = 0x7FFFFFFFl; // address space
    /**
     * The default number of processes that are reported if the actual value can not be retrieved.
     */
    public static long DEFAULT_MAX_PROCESSES = 1024;

    private Logger logger = LoggerFactory.getLogger(SystemInfo.class);

    /* The oshi.SystemInfo has  the following note:
     * Platform-specific Hardware and Software objects are retrieved via memoized suppliers. To conserve memory at the
     * cost of additional processing time, create a new version of SystemInfo() for subsequent calls. To conserve
     * processing time at the cost of additional memory usage, re-use the same {@link SystemInfo} object for future
     * queries.
     *
     * We are opting for minimal memory footprint. */
    private oshi.SystemInfo si ;

    public SystemInfo() {
        si = new oshi.SystemInfo();
    }

    /**
     *
     * @return The name of the current platform. (e.g. Linux)
     */
    public String platform()
    {
        return oshi.SystemInfo.getCurrentPlatform().name();
    }

    /**
     * Gets the maximum number of processes the user can create.
     * Note: if not on a Linux system this always return the
     * @{code DEFAULT_MAX_PROCESSES} value.
     * @return The maximum number of processes.
     * @see #DEFAULT_MAX_PROCESSES
     */
    long getMaxProcess() {
        // this check only works on Linux systems.
        if (oshi.SystemInfo.getCurrentPlatform() == PlatformEnum.LINUX) {
            Path p =  FileSystems.getDefault().getPath("/proc", Long.toString(getPid()), "limits");
            try
            {
                List<String> lines = Files.readAllLines(p);
                for (String s : lines)
                {
                    if (s.startsWith("Max processes"))
                    {
                        String[] parts = s.split("\s+");
                        String limit = parts[2];
                        if ("unlimited".equals(limit))
                        {
                            return INFINITY;
                        }
                        return Long.parseLong(limit);
                    }
                }
                logger.error("'Max processes' not found in " + p);
            }
            catch (IOException e)
            {
                logger.error("Unable to read "+p, e);
            }
        }
        /* return the default value for non-Linux systems.
         * can not return 0 as we know there is at least 1 process (this one) and
         * -1 historically represents infinity.
        */
        return DEFAULT_MAX_PROCESSES;
    }

    /**
     *
     * @return The maximum number of open files allowd to the current process/user.
     */
    long getMaxOpenFiles()  {
        // ulimit -H -n
        return si.getOperatingSystem().getCurrentProcess().getHardOpenFileLimit();
    }

    /**
     * Gets the Virtual Memory Size (VSZ). Includes all memory that the process can access, including memory that is swapped out and memory that is from shared libraries.
     * @return The amount of virtual memory allowed to be allocatedby the current process/user.
     */
    long getVirtualMemoryMax() {
        return si.getOperatingSystem().getCurrentProcess().getVirtualSize();
    }

    /**
     *
     * @return The amount of swap space allocated on the system.
     */
    long getSwapSize() {
        return si.getHardware().getMemory().getVirtualMemory().getSwapTotal();
    }

    /**
     * @return the PID of the current system.
     */
    public long getPid() {
        return si.getOperatingSystem().getProcessId();
    }

    /**
     * Checks if a value if valide (i.e. value >= min or value == INFINITY..
     * @param value the value to check.
     * @param min the minimum value.
     * @return true if value is valid.
     */
    private boolean valid(long value, long min)
    {
        return value >= min || value == INFINITY;
    }

    /**
     * Tests if the system is running in degraded mode.
     * If the system is running in degraded mode this method will log information.
     * @return @{code true} if the system is in degraded mode, @{code false} otherwise.
     */
    public boolean warnIfRunningInDegradedMode()
    {
        boolean degraded = false;
        StringBuilder sb = new StringBuilder();

        if (oshi.SystemInfo.getCurrentPlatform() == PlatformEnum.LINUX)
        {
            // only check proc on nproc linux
            if (!valid(getMaxProcess(), EXPECTED_NPROC))
            {
                sb.append("Number of processes should be >= ").append(EXPECTED_NPROC).append("\n");
                degraded = true;
            }
        } else
        {
            degraded = true;
            sb.append("System is running ").append(platform()).append(", Linux OS is recommended");
        }

        if (getSwapSize() > 0)
        {
            sb.append("Swap should be disabled\n");
            degraded = true;
        }
        if (!valid(getMaxProcess(), EXPECTED_NPROC))
        {
            sb.append("Minimum value for max number of processes should be >= ").append(EXPECTED_NPROC).append("\n");
            degraded = true;
        }
        if (!valid(getVirtualMemoryMax(), EXPECTED_AS))
        {
            sb.append("Amount of available address space should be >= ").append(EXPECTED_AS).append("\n");
            degraded = true;
        }
        if (!valid(getMaxOpenFiles(), EXPECTED_MIN_NOFILE))
        {
            sb.append("Minimum value for max open files should be >=  ").append(EXPECTED_MIN_NOFILE).append("\n");
            degraded = true;
        }
        if (degraded) {
            if (CassandraRelevantProperties.TEST_IGNORE_SIGAR.getBoolean())
            {
                logger.warn("Cassandra server running in degraded mode.\n" + sb.toString());
            } else {
                logger.info("Cassandra server running in degraded mode.\n" + sb.toString());
            }
        } else
        {
            logger.info("Checked OS settings and found them configured for optimal performance.");
        }
        return degraded;
    }
}
