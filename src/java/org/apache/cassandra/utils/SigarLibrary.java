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

import org.hyperic.sigar.*;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

public class SigarLibrary
{
    private Logger logger = LoggerFactory.getLogger(SigarLibrary.class);

    public static final SigarLibrary instance = new SigarLibrary();

    private Sigar sigar;
    private FileSystemMap mounts = null;
    private boolean initialized = false;
    private long INFINITY = -1;
    private long EXPECTED_MIN_NOFILE = 10000l; // number of files that can be opened
    private long EXPECTED_NPROC = 32768l; // number of processes
    private long EXPECTED_AS = INFINITY; // address space

    // TODO: Determine memlock limits if possible
    // TODO: Determine if file system is remote or local
    // TODO: Determine if disk latency is within acceptable limits

    private SigarLibrary()
    {
        logger.info("Initializing SIGAR library");
        try
        {
            sigar = new Sigar();
            mounts = sigar.getFileSystemMap();
            initialized = true;
        }
        catch (SigarException e)
        {
            logger.info("Could not initialize SIGAR library {} ", e.getMessage());
        }
        catch (UnsatisfiedLinkError linkError)
        {
            logger.info("Could not initialize SIGAR library {} ", linkError.getMessage());
        }
    }

    /**
     *
     * @return true or false indicating if sigar was successfully initialized
     */
    public boolean initialized()
    {
        return initialized;
    }

    private boolean hasAcceptableProcNumber()
    {
        try
        {
            long fileMax = sigar.getResourceLimit().getProcessesMax();
            if (fileMax >= EXPECTED_NPROC || fileMax == INFINITY)
            {
                return true;
            }
            else
            {
                return false;
            }
        }
        catch (SigarException sigarException)
        {
            logger.warn("Could not determine if max processes was acceptable. Error message: {}", sigarException);
            return false;
        }
    }

    private boolean hasAcceptableFileLimits()
    {
        try
        {
            long fileMax = sigar.getResourceLimit().getOpenFilesMax();
            if (fileMax >= EXPECTED_MIN_NOFILE || fileMax == INFINITY)
            {
                return true;
            }
            else
            {
                return false;
            }
        }
        catch (SigarException sigarException)
        {
            logger.warn("Could not determine if max open file handle limit is correctly configured. Error message: {}", sigarException);
            return false;
        }
    }

    private boolean hasAcceptableAddressSpace()
    {
        // Check is invalid on Windows
        if (FBUtilities.isWindows)
            return true;

        try
        {
            long fileMax = sigar.getResourceLimit().getVirtualMemoryMax();
            if (fileMax == EXPECTED_AS)
            {
                return true;
            }
            else
            {
                return false;
            }
        }
        catch (SigarException sigarException)
        {
            logger.warn("Could not determine if VirtualMemoryMax was acceptable. Error message: {}", sigarException);
            return false;
        }
    }

    private boolean isSwapEnabled()
    {
        try
        {
            Swap swap = sigar.getSwap();
            long swapSize = swap.getTotal();
            if (swapSize > 0)
            {
                return true;
            }
            else
            {
                return false;
            }
        }
        catch (SigarException sigarException)
        {
            logger.warn("Could not determine if swap configuration is acceptable. Error message: {}", sigarException);
            return false;
        }
    }

    public long getPid()
    {
        return initialized ? sigar.getPid() : -1;
    }

    public void warnIfRunningInDegradedMode()
    {
        if (initialized)
        {
            boolean swapEnabled = isSwapEnabled();
            boolean goodAddressSpace = hasAcceptableAddressSpace();
            boolean goodFileLimits = hasAcceptableFileLimits();
            boolean goodProcNumber = hasAcceptableProcNumber();
            if (swapEnabled || !goodAddressSpace || !goodFileLimits || !goodProcNumber)
            {
                logger.warn("Cassandra server running in degraded mode. Is swap disabled? : {},  Address space adequate? : {}, " +
                            " nofile limit adequate? : {}, nproc limit adequate? : {} ", !swapEnabled, goodAddressSpace,
                            goodFileLimits, goodProcNumber );
            }
            else
            {
                logger.info("Checked OS settings and found them configured for optimal performance.");
            }
        }
        else
        {
            logger.info("Sigar could not be initialized, test for checking degraded mode omitted.");
        }
    }
}
