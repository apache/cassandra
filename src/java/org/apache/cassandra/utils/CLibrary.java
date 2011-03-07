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

import java.io.*;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.jna.LastErrorException;
import com.sun.jna.Native;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.BufferedRandomAccessFile;
import org.apache.cassandra.io.util.FileUtils;

public final class CLibrary
{
    private static Logger logger = LoggerFactory.getLogger(CLibrary.class);

    private static final int MCL_CURRENT = 1;
    private static final int MCL_FUTURE = 2;
    
    private static final int ENOMEM = 12;

    private static final int F_GETFL   = 3;  /* get file status flags */
    private static final int F_SETFL   = 4;  /* set file status flags */
    private static final int F_NOCACHE = 48; /* Mac OS X specific flag, turns cache on/off */
    private static final int O_DIRECT  = 040000; /* fcntl.h */

    private static final int POSIX_FADV_NORMAL     = 0; /* fadvise.h */
    private static final int POSIX_FADV_RANDOM     = 1; /* fadvise.h */
    private static final int POSIX_FADV_SEQUENTIAL = 2; /* fadvise.h */
    private static final int POSIX_FADV_WILLNEED   = 3; /* fadvise.h */
    private static final int POSIX_FADV_DONTNEED   = 4; /* fadvise.h */
    private static final int POSIX_FADV_NOREUSE    = 5; /* fadvise.h */

    private static native int mlockall(int flags) throws LastErrorException;
    private static native int munlockall() throws LastErrorException;

    private static native int link(String from, String to) throws LastErrorException;

    // fcntl - manipulate file descriptor, `man 2 fcntl`
    public static native int fcntl(int fd, int command, long flags) throws LastErrorException;

    // fadvice
    public static native int posix_fadvise(int fd, long offset, long len, int flag) throws LastErrorException;

    public static native int mincore(ByteBuffer buf, int length, char[] vec) throws LastErrorException;

    private static native int getpagesize() throws LastErrorException;
    public  static Integer pageSize = null;

    static
    {
        try
        {
            Native.register("c");
            pageSize = CLibrary.getPageSize();
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

    private static Integer getPageSize()
    {
        int ps = -1;

        if (!DatabaseDescriptor.isPageCaheMigrationEnabled())
            return null;

        try
        {
            ps = CLibrary.getpagesize();
            assert ps >= 0; // on error a value of -1 is returned and errno is set to indicate the error.

            logger.info("PageSize = " + ps);
        }
        catch (RuntimeException e)
        {
            if (!(e instanceof LastErrorException))
                throw e;

            logger.warn(String.format("getpagesize failed, errno (%d).", CLibrary.errno(e)));
        }
        catch (UnsatisfiedLinkError e)
        {
            // this will have already been logged by CLibrary, no need to repeat it
        }

        return ps > 0 ? ps : null;
    }

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

    public static void createHardLinkWithExec(File sourceFile, File destinationFile) throws IOException
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

    public static void trySkipCache(int fd, long offset, long len)
    {
        if (fd < 0 || !DatabaseDescriptor.isPageCaheMigrationEnabled())
            return;

        try
        {
            if (System.getProperty("os.name").toLowerCase().contains("linux"))
            {
                posix_fadvise(fd, offset, len, POSIX_FADV_DONTNEED);
            }
            else if (System.getProperty("os.name").toLowerCase().contains("mac"))
            {
                tryFcntl(fd, F_NOCACHE, 1);
            }
        }
        catch (UnsatisfiedLinkError e)
        {
            // if JNA is unavailable just skipping Direct I/O
            // instance of this class will act like normal RandomAccessFile
        }
    }

    public static PageCacheMetrics getCachedPages(BufferedRandomAccessFile braf) throws IOException
    {
        if (pageSize == null || pageSize == 0 || !DatabaseDescriptor.isPageCaheMigrationEnabled())
            return null;

        long length = braf.length();

        PageCacheMetrics pageCacheMetrics = new PageCacheMetrics(pageSize, length);

        try
        {
            char[] pages = null;

            // MMap 2G chunks
            for (long offset = 0; offset < length; offset += Integer.MAX_VALUE)
            {
                long limit = (offset + Integer.MAX_VALUE) > length ? (length - offset) : Integer.MAX_VALUE;

                ByteBuffer buf = braf.getChannel().map(MapMode.READ_ONLY, offset, limit);

                int numPages = (int) ((limit + pageSize - 1) / pageSize);

                if (pages == null || pages.length < numPages)
                    pages = new char[numPages];

                int rc = mincore(buf, (int) limit, pages);

                if (rc != 0)
                {
                    logger.warn(String.format("mincore failed, rc (%d).", rc));
                    break;
                }

                for (long i = 0, position = offset; position < limit; position += pageSize, i++)
                {
                    if ((pages[(int) i] & 1) == 1)
                    {
                        pageCacheMetrics.setPage(position);
                    }
                }
            }
        }
        catch (RuntimeException e)
        {
            if (!(e instanceof LastErrorException))
                throw e;

            logger.warn(String.format("mincore failed, errno (%d).", CLibrary.errno(e)));
        }
        catch (UnsatisfiedLinkError e)
        {
            // this will have already been logged by CLibrary, no need to repeat it
        }

        return pageCacheMetrics;
    }

    public static int tryFcntl(int fd, int command, int flags)
    {
        int result = -1;

        try
        {
            result = CLibrary.fcntl(fd, command, flags);
            assert result >= 0; // on error a value of -1 is returned and errno is set to indicate the error.
        }
        catch (RuntimeException e)
        {
            if (!(e instanceof LastErrorException))
                throw e;

            logger.warn(String.format("fcntl(%d, %d, %d) failed, errno (%d).",
                                      fd, command, flags, CLibrary.errno(e)));
        }

        return result;
    }

    /**
     * Get system file descriptor from FileDescriptor object.
     * @param descriptor - FileDescriptor objec to get fd from
     * @return file descriptor, -1 or error
     */
    public static int getfd(FileDescriptor descriptor)
    {
        Field field = FBUtilities.getProtectedField(descriptor.getClass(), "fd");

        if (field == null)
            return -1;

        try
        {
            return field.getInt(descriptor);
        }
        catch (Exception e)
        {
            logger.warn("unable to read fd field from FileDescriptor");
        }

        return -1;
    }
}
