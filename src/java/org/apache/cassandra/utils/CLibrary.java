/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.utils;

import java.io.File;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.jna.LastErrorException;
import com.sun.jna.Native;

public final class CLibrary
{
    private static Logger logger = LoggerFactory.getLogger(CLibrary.class);

    private static final int MCL_CURRENT = 1;
    private static final int MCL_FUTURE = 2;
    
    private static final int ENOMEM = 12;

    static
    {
        try
        {
            Native.register("c");
        }
        catch (NoClassDefFoundError e)
        {
            logger.info("JNA not found. Native methods will be disabled.");
        }
        catch (UnsatisfiedLinkError e)
        {
            logger.info("Unable to link C library. Native methods will be disabled.");
        }
        catch (NoSuchMethodError e)
        {
            logger.warn("Obsolete version of JNA present; unable to register C library. Upgrade to JNA 3.2.7 or later");
        }
    }

    private static native int mlockall(int flags) throws LastErrorException;
    private static native int munlockall() throws LastErrorException;

    private static native int link(String from, String to) throws LastErrorException;

    private static int errno(RuntimeException e)
    {
        assert e instanceof LastErrorException;
        try
        {
            return ((LastErrorException) e).getErrorCode();
        }
        catch (NoSuchMethodError x)
        {
            logger.warn("Obsolete version of JNA present; unable to read errno. Upgrade to JNA 3.2.7 or later");
            return 0;
        }
    }

    private CLibrary() {}

    public static void tryMlockall()
    {
        try
        {
            int result = mlockall(MCL_CURRENT);
            assert result == 0; // mlockall should always be zero on success
            logger.info("JNA mlockall successful");
        }
        catch (UnsatisfiedLinkError e)
        {
            // this will have already been logged by CLibrary, no need to repeat it
        }
        catch (RuntimeException e)
        {
            if (!(e instanceof LastErrorException))
                throw e;
            if (errno(e) == ENOMEM && System.getProperty("os.name").toLowerCase().contains("linux"))
            {
                logger.warn("Unable to lock JVM memory (ENOMEM)."
                             + " This can result in part of the JVM being swapped out, especially with mmapped I/O enabled."
                             + " Increase RLIMIT_MEMLOCK or run Cassandra as root.");
            }
            else if (!System.getProperty("os.name").toLowerCase().contains("mac"))
            {
                // OS X allows mlockall to be called, but always returns an error
                logger.warn("Unknown mlockall error " + errno(e));
            }
        }
    }

    /**
     * Create a hard link for a given file.
     *
     * @param sourceFile      The name of the source file.
     * @param destinationFile The name of the destination file.
     *
     * @throws java.io.IOException if an error has occurred while creating the link.
     */
    public static void createHardLink(File sourceFile, File destinationFile) throws IOException
    {
        try
        {
            int result = link(sourceFile.getAbsolutePath(), destinationFile.getAbsolutePath());
            assert result == 0; // success is always zero
        }
        catch (UnsatisfiedLinkError e)
        {
            createHardLinkWithExec(sourceFile, destinationFile);
        }
        catch (RuntimeException e)
        {
            if (!(e instanceof LastErrorException))
                throw e;
            // there are 17 different error codes listed on the man page.  punt until/unless we find which
            // ones actually turn up in practice.
            throw new IOException(String.format("Unable to create hard link from %s to %s (errno %d)",
                                                sourceFile, destinationFile, errno(e)));
        }
    }

    private static void createHardLinkWithExec(File sourceFile, File destinationFile) throws IOException
    {
        String osname = System.getProperty("os.name");
        ProcessBuilder pb;
        if (osname.startsWith("Windows"))
        {
            float osversion = Float.parseFloat(System.getProperty("os.version"));
            if (osversion >= 6.0f)
            {
                pb = new ProcessBuilder("cmd", "/c", "mklink", "/H", destinationFile.getAbsolutePath(), sourceFile.getAbsolutePath());
            }
            else
            {
                pb = new ProcessBuilder("fsutil", "hardlink", "create", destinationFile.getAbsolutePath(), sourceFile.getAbsolutePath());
            }
        }
        else
        {
            pb = new ProcessBuilder("ln", sourceFile.getAbsolutePath(), destinationFile.getAbsolutePath());
            pb.redirectErrorStream(true);
        }
        Process p = pb.start();
        try
        {
            p.waitFor();
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
    }
}
