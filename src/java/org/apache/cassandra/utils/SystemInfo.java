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

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import oshi.PlatformEnum;

import static java.lang.String.format;
import static java.util.Optional.empty;
import static java.util.Optional.of;

/**
 * An abstraction of System information, this class provides access to system information without specifying how
 * it is retrieved.
 */
public class SystemInfo
{
    private static final Logger logger = LoggerFactory.getLogger(SystemInfo.class);

    private static final long INFINITY = -1L;
    private static final long EXPECTED_MIN_NOFILE = 10000L; // number of files that can be opened
    private static final long EXPECTED_NPROC = 32768L; // number of processes
    private static final long EXPECTED_AS = 0x7FFFFFFFL; // address space

    /**
     * The default number of processes that are reported if the actual value can not be retrieved.
     */
    private static final long DEFAULT_MAX_PROCESSES = 1024;

    private static final Pattern SPACES_PATTERN = Pattern.compile("\\s+");

    /**
     * The oshi.SystemInfo has the following note:
     * Platform-specific Hardware and Software objects are retrieved via memoized suppliers. To conserve memory at the
     * cost of additional processing time, create a new version of SystemInfo() for subsequent calls. To conserve
     * processing time at the cost of additional memory usage, re-use the same {@link SystemInfo} object for future
     * queries.
     * <p>
     * We are opting for minimal memory footprint.
     */
    private final oshi.SystemInfo si;

    public SystemInfo()
    {
        si = new oshi.SystemInfo();
    }

    /**
     * @return The name of the current platform. (e.g. Linux)
     */
    public String platform()
    {
        return oshi.SystemInfo.getCurrentPlatform().name();
    }

    /**
     * Gets the maximum number of processes the user can create.
     * Note: if not on a Linux system this always return the
     *
     * @return The maximum number of processes.
     * @see #DEFAULT_MAX_PROCESSES
     */
    long getMaxProcess()
    {
        // this check only works on Linux systems.
        if (oshi.SystemInfo.getCurrentPlatform() == PlatformEnum.LINUX)
        {
            String path = format("/proc/%s/limits", getPid());
            try
            {
                List<String> lines = FileUtils.readLines(new File(path));
                for (String line : lines)
                {
                    if (line.startsWith("Max processes"))
                    {
                        String[] parts = SPACES_PATTERN.split(line);

                        if (parts.length < 3)
                            continue;

                        String limit = parts[2];
                        return "unlimited".equals(limit) ? INFINITY : Long.parseLong(limit);
                    }
                }

                /* return the default value for non-Linux systems.
                 * can not return 0 as we know there is at least 1 process (this one) and
                 * -1 historically represents infinity.
                 */
                return DEFAULT_MAX_PROCESSES;
            }
            catch (Throwable t)
            {
                logger.error(format("Unable to read %s", path), t);
            }
        }

        /* return the default value for non-Linux systems.
         * can not return 0 as we know there is at least 1 process (this one) and
         * -1 historically represents infinity.
         */
        return DEFAULT_MAX_PROCESSES;
    }

    /**
     * @return The maximum number of open files allowd to the current process/user.
     */
    public long getMaxOpenFiles()
    {
        // ulimit -H -n
        return si.getOperatingSystem().getCurrentProcess().getHardOpenFileLimit();
    }

    /**
     * Gets the Virtual Memory Size (VSZ). Includes all memory that the process can access,
     * including memory that is swapped out and memory that is from shared libraries.
     *
     * @return The amount of virtual memory allowed to be allocatedby the current process/user.
     */
    public long getVirtualMemoryMax()
    {
        return si.getOperatingSystem().getCurrentProcess().getVirtualSize();
    }

    /**
     * @return The amount of swap space allocated on the system.
     */
    public long getSwapSize()
    {
        return si.getHardware().getMemory().getVirtualMemory().getSwapTotal();
    }

    /**
     * @return the PID of the current system.
     */
    public long getPid()
    {
        return si.getOperatingSystem().getProcessId();
    }

    /**
     * Tests if the system is running in degraded mode.
     *
     * @return non-empty optional with degradation messages if the system is in degraded mode, empty optional otherwise.
     */
    public Optional<String> isDegraded()
    {
        Supplier<Optional<String>> expectedNumProc = () -> {
            // only check proc on nproc linux
            if (oshi.SystemInfo.getCurrentPlatform() == PlatformEnum.LINUX)
                return invalid(getMaxProcess(), EXPECTED_NPROC) ? of(format("Number of processes should be >= %s. ", EXPECTED_NPROC))
                                                                : empty();
            else
                return of(format("System is running %s, Linux OS is recommended", platform()));
        };

        Supplier<Optional<String>> swapShouldBeDisabled = () -> (getSwapSize() > 0)
                                                                ? of("Swap should be disabled. ")
                                                                : empty();

        Supplier<Optional<String>> expectedAddressSpace = () -> invalid(getVirtualMemoryMax(), EXPECTED_AS)
                                                                ? of(format("Amount of available address space should be >= %s. ", EXPECTED_AS))
                                                                : empty();

        Supplier<Optional<String>> expectedMinNoFile = () -> invalid(getMaxOpenFiles(), EXPECTED_MIN_NOFILE)
                                                             ? of(format("Minimum value for max open files should be >= %s. ", EXPECTED_MIN_NOFILE))
                                                             : empty();

        StringBuilder sb = new StringBuilder();

        for (Supplier<Optional<String>> check : ImmutableList.of(expectedNumProc,
                                                                 swapShouldBeDisabled,
                                                                 expectedAddressSpace,
                                                                 expectedMinNoFile))
        {
            check.get().map(sb::append);
        }

        String message = sb.toString();
        return message.isEmpty() ? empty() : of(message);
    }

    /**
     * Checks if a value is invalid.
     * <p>
     * Value is invalid if it is smaller than {@code min} and it is not {@code INFINITY},
     * here represented as a value of -1;
     *
     * @param value the value to check
     * @param min   the minimum value
     * @return true if value is valid
     */
    private boolean invalid(long value, long min)
    {
        return value < min && value != INFINITY;
    }
}
