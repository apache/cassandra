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

import java.io.File;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.channels.FileChannel;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.jna.LastErrorException;
import sun.nio.ch.FileChannelImpl;

import org.apache.cassandra.io.FSWriteError;

import static org.apache.cassandra.utils.NativeLibrary.OSType.LINUX;
import static org.apache.cassandra.utils.NativeLibrary.OSType.MAC;
import static org.apache.cassandra.utils.NativeLibrary.OSType.WINDOWS;
import static org.apache.cassandra.utils.NativeLibrary.OSType.AIX;

public final class NativeLibrary
{
    private static final Logger logger = LoggerFactory.getLogger(NativeLibrary.class);

    public enum OSType
    {
        LINUX,
        MAC,
        WINDOWS,
        AIX,
        OTHER;
    }

    private static final OSType osType;

    private static final int MCL_CURRENT;
    private static final int MCL_FUTURE;

    private static final int ENOMEM = 12;

    private static final int F_GETFL   = 3;  /* get file status flags */
    private static final int F_SETFL   = 4;  /* set file status flags */
    private static final int F_NOCACHE = 48; /* Mac OS X specific flag, turns cache on/off */
    private static final int O_DIRECT  = 040000; /* fcntl.h */
    private static final int O_RDONLY  = 00000000; /* fcntl.h */

    private static final int POSIX_FADV_NORMAL     = 0; /* fadvise.h */
    private static final int POSIX_FADV_RANDOM     = 1; /* fadvise.h */
    private static final int POSIX_FADV_SEQUENTIAL = 2; /* fadvise.h */
    private static final int POSIX_FADV_WILLNEED   = 3; /* fadvise.h */
    private static final int POSIX_FADV_DONTNEED   = 4; /* fadvise.h */
    private static final int POSIX_FADV_NOREUSE    = 5; /* fadvise.h */

    private static final NativeLibraryWrapper wrappedLibrary;
    private static boolean jnaLockable = false;

    private static final Field FILE_DESCRIPTOR_FD_FIELD;
    private static final Field FILE_CHANNEL_FD_FIELD;

    static
    {
        FILE_DESCRIPTOR_FD_FIELD = FBUtilities.getProtectedField(FileDescriptor.class, "fd");
        FILE_CHANNEL_FD_FIELD = FBUtilities.getProtectedField(FileChannelImpl.class, "fd");

        // detect the OS type the JVM is running on and then set the CLibraryWrapper
        // instance to a compatable implementation of CLibraryWrapper for that OS type
        osType = getOsType();
        switch (osType)
        {
            case MAC: wrappedLibrary = new NativeLibraryDarwin(); break;
            case WINDOWS: wrappedLibrary = new NativeLibraryWindows(); break;
            case LINUX:
            case AIX:
            case OTHER:
            default: wrappedLibrary = new NativeLibraryLinux();
        }

        if (System.getProperty("os.arch").toLowerCase().contains("ppc"))
        {
            if (osType == LINUX)
            {
               MCL_CURRENT = 0x2000;
               MCL_FUTURE = 0x4000;
            }
            else if (osType == AIX)
            {
                MCL_CURRENT = 0x100;
                MCL_FUTURE = 0x200;
            }
            else
            {
                MCL_CURRENT = 1;
                MCL_FUTURE = 2;
            }
        }
        else
        {
            MCL_CURRENT = 1;
            MCL_FUTURE = 2;
        }
    }

    private NativeLibrary() {}

    /**
     * @return the detected OSType of the Operating System running the JVM using crude string matching
     */
    private static OSType getOsType()
    {
        String osName = System.getProperty("os.name").toLowerCase();
        if (osName.contains("mac"))
            return MAC;
        else if (osName.contains("windows"))
            return WINDOWS;
        else if (osName.contains("aix"))
            return AIX;
        else
            // fall back to the Linux impl for all unknown OS types until otherwise implicitly supported as needed
            return LINUX;
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

    /**
     * Checks if the library has been successfully linked.
     * @return {@code true} if the library has been successfully linked, {@code false} otherwise.
     */
    public static boolean isAvailable()
    {
        return wrappedLibrary.isAvailable();
    }

    public static boolean jnaMemoryLockable()
    {
        return jnaLockable;
    }

    public static void tryMlockall()
    {
        try
        {
            wrappedLibrary.callMlockall(MCL_CURRENT);
            jnaLockable = true;
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

            if (errno(e) == ENOMEM && osType == LINUX)
            {
                logger.warn("Unable to lock JVM memory (ENOMEM)."
                        + " This can result in part of the JVM being swapped out, especially with mmapped I/O enabled."
                        + " Increase RLIMIT_MEMLOCK or run Cassandra as root.");
            }
            else if (osType != MAC)
            {
                // OS X allows mlockall to be called, but always returns an error
                logger.warn("Unknown mlockall error {}", errno(e));
            }
        }
    }

    public static void trySkipCache(String path, long offset, long len)
    {
        File f = new File(path);
        if (!f.exists())
            return;

        try (FileInputStream fis = new FileInputStream(f))
        {
            trySkipCache(getfd(fis.getChannel()), offset, len, path);
        }
        catch (IOException e)
        {
            logger.warn("Could not skip cache", e);
        }
    }

    public static void trySkipCache(int fd, long offset, long len, String path)
    {
        if (len == 0)
            trySkipCache(fd, 0, 0, path);

        while (len > 0)
        {
            int sublen = (int) Math.min(Integer.MAX_VALUE, len);
            trySkipCache(fd, offset, sublen, path);
            len -= sublen;
            offset -= sublen;
        }
    }

    public static void trySkipCache(int fd, long offset, int len, String path)
    {
        if (fd < 0)
            return;

        try
        {
            if (osType == LINUX)
            {
                int result = wrappedLibrary.callPosixFadvise(fd, offset, len, POSIX_FADV_DONTNEED);
                if (result != 0)
                    NoSpamLogger.log(
                            logger,
                            NoSpamLogger.Level.WARN,
                            10,
                            TimeUnit.MINUTES,
                            "Failed trySkipCache on file: {} Error: " + wrappedLibrary.callStrerror(result).getString(0),
                            path);
            }
        }
        catch (UnsatisfiedLinkError e)
        {
            // if JNA is unavailable just skipping Direct I/O
            // instance of this class will act like normal RandomAccessFile
        }
        catch (RuntimeException e)
        {
            if (!(e instanceof LastErrorException))
                throw e;

            logger.warn("posix_fadvise({}, {}) failed, errno ({}).", fd, offset, errno(e));
        }
    }

    public static int tryFcntl(int fd, int command, int flags)
    {
        // fcntl return value may or may not be useful, depending on the command
        int result = -1;

        try
        {
            result = wrappedLibrary.callFcntl(fd, command, flags);
        }
        catch (UnsatisfiedLinkError e)
        {
            // if JNA is unavailable just skipping
        }
        catch (RuntimeException e)
        {
            if (!(e instanceof LastErrorException))
                throw e;

            logger.warn("fcntl({}, {}, {}) failed, errno ({}).", fd, command, flags, errno(e));
        }

        return result;
    }

    public static int tryOpenDirectory(String path)
    {
        int fd = -1;

        try
        {
            return wrappedLibrary.callOpen(path, O_RDONLY);
        }
        catch (UnsatisfiedLinkError e)
        {
            // JNA is unavailable just skipping Direct I/O
        }
        catch (RuntimeException e)
        {
            if (!(e instanceof LastErrorException))
                throw e;

            logger.warn("open({}, O_RDONLY) failed, errno ({}).", path, errno(e));
        }

        return fd;
    }

    public static void trySync(int fd)
    {
        if (fd == -1)
            return;

        try
        {
            wrappedLibrary.callFsync(fd);
        }
        catch (UnsatisfiedLinkError e)
        {
            // JNA is unavailable just skipping Direct I/O
        }
        catch (RuntimeException e)
        {
            if (!(e instanceof LastErrorException))
                throw e;

            String errMsg = String.format("fsync(%s) failed, errno %s", fd, errno(e));
            logger.warn(errMsg);
            throw new FSWriteError(e, errMsg);
        }
    }

    public static void tryCloseFD(int fd)
    {
        if (fd == -1)
            return;

        try
        {
            wrappedLibrary.callClose(fd);
        }
        catch (UnsatisfiedLinkError e)
        {
            // JNA is unavailable just skipping Direct I/O
        }
        catch (RuntimeException e)
        {
            if (!(e instanceof LastErrorException))
                throw e;

            String errMsg = String.format("close(%d) failed, errno (%d).", fd, errno(e));
            logger.warn(errMsg);
            throw new FSWriteError(e, errMsg);
        }
    }

    public static int getfd(FileChannel channel)
    {
        try
        {
            return getfd((FileDescriptor)FILE_CHANNEL_FD_FIELD.get(channel));
        }
        catch (IllegalArgumentException|IllegalAccessException e)
        {
            logger.warn("Unable to read fd field from FileChannel");
        }
        return -1;
    }

    /**
     * Get system file descriptor from FileDescriptor object.
     * @param descriptor - FileDescriptor objec to get fd from
     * @return file descriptor, -1 or error
     */
    public static int getfd(FileDescriptor descriptor)
    {
        try
        {
            return FILE_DESCRIPTOR_FD_FIELD.getInt(descriptor);
        }
        catch (Exception e)
        {
            JVMStabilityInspector.inspectThrowable(e);
            logger.warn("Unable to read fd field from FileDescriptor");
        }

        return -1;
    }

    /**
     * @return the PID of the JVM or -1 if we failed to get the PID
     */
    public static long getProcessID()
    {
        try
        {
            return wrappedLibrary.callGetpid();
        }
        catch (Exception e)
        {
            logger.info("Failed to get PID from JNA", e);
        }

        return -1;
    }
}
