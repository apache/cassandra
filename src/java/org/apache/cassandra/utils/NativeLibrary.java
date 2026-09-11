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

import java.io.FileDescriptor;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.channels.FileChannel;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import com.sun.jna.LastErrorException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileInputStreamPlus;
import org.apache.cassandra.io.util.FileUtils;

import jnr.constants.Constant;
import jnr.constants.ConstantSet;

import static org.apache.cassandra.config.CassandraRelevantProperties.IGNORE_MISSING_NATIVE_FILE_HINTS;
import static org.apache.cassandra.config.CassandraRelevantProperties.JAVA_IO_TMPDIR;
import static org.apache.cassandra.config.CassandraRelevantProperties.OS_ARCH;
import static org.apache.cassandra.config.CassandraRelevantProperties.OS_NAME;
import static org.apache.cassandra.utils.LocalizeString.toLowerCaseLocalized;
import static org.apache.cassandra.utils.NativeLibrary.OSType.AIX;
import static org.apache.cassandra.utils.NativeLibrary.OSType.LINUX;
import static org.apache.cassandra.utils.NativeLibrary.OSType.MAC;

public final class NativeLibrary
{
    private static final Logger logger = LoggerFactory.getLogger(NativeLibrary.class);
    private static final boolean REQUIRE = !IGNORE_MISSING_NATIVE_FILE_HINTS.getBoolean();

    public enum OSType
    {
        LINUX,
        MAC,
        AIX,
        OTHER;
    }

    public static final OSType osType;

    private static final int MCL_CURRENT;
    private static final int MCL_FUTURE;

    private static final int ENOMEM = 12;

    private static final int F_GETFL   = 3;  /* get file status flags */
    private static final int F_SETFL   = 4;  /* set file status flags */
    private static final int F_NOCACHE = 48; /* Mac OS X specific flag, turns cache on/off */
    private static final int O_DIRECT  = 040000; /* fcntl.h */
    private static final int O_RDONLY  = 00000000; /* fcntl.h */
    @VisibleForTesting
    static final int O_DIRECTORY; /* fcntl.h; jnr-resolved value is confirmed against the kernel at class init, see verifyODirectory() */

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
        try
        {
            FILE_CHANNEL_FD_FIELD = FBUtilities.getProtectedField(Class.forName("sun.nio.ch.FileChannelImpl"), "fd");
        }
        catch (ClassNotFoundException e)
        {
            throw new RuntimeException(e);
        }

        // detect the OS type the JVM is running on and then set the CLibraryWrapper
        // instance to a compatable implementation of CLibraryWrapper for that OS type
        osType = getOsType();
        switch (osType)
        {
            case MAC: wrappedLibrary = new NativeLibraryDarwin(); break;
            case LINUX:
            case AIX:
            case OTHER:
            default: wrappedLibrary = new NativeLibraryLinux();
        }

        ConstantSet openFlags = ConstantSet.getConstantSet("OpenFlags");
        Constant oDirectory = openFlags == null ? null : openFlags.getConstant("O_DIRECTORY");
        O_DIRECTORY = verifyODirectory(isHostConstant(oDirectory) ? oDirectory.intValue() : 0);

        if (toLowerCaseLocalized(OS_ARCH.getString()).contains("ppc"))
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

    /**
     * jnr-constants only maps a handful of architectures (aarch64, s390x, mips64el, loongarch64) to their
     * per-arch OpenFlags tables; on others (e.g. ppc64le, arm32) a resolved value can silently be the wrong
     * flag for this host. Do not trust the table: confirm it opens java.io.tmpdir and rejects a regular file
     * with ENOTDIR before relying on it to gate directory-fsync tolerance.
     */
    private static int verifyODirectory(int candidate)
    {
        if (candidate == 0)
            return 0;
        File probe = FileUtils.createDeletableTempFile("odirectory-probe", "tmp");
        boolean verified;
        try
        {
            wrappedLibrary.callClose(wrappedLibrary.callOpen(JAVA_IO_TMPDIR.getString(), O_RDONLY | candidate));
            wrappedLibrary.callClose(wrappedLibrary.callOpen(probe.path(), O_RDONLY | candidate));
            verified = false; // must reject a regular file with ENOTDIR; it did not
        }
        catch (RuntimeException | UnsatisfiedLinkError e)
        {
            ConstantSet errnos = ConstantSet.getConstantSet("Errno");
            Constant enotdir = errnos == null ? null : errnos.getConstant("ENOTDIR");
            verified = e instanceof LastErrorException && matchesErrno(enotdir, errno((LastErrorException) e));
        }
        finally
        {
            probe.tryDelete();
        }
        if (!verified)
            logger.info("O_DIRECTORY capability probe failed; disabling directory fsync tolerance");
        return verified ? candidate : 0;
    }

    private NativeLibrary() {}

    /**
     * @return the detected OSType of the Operating System running the JVM using crude string matching
     */
    private static OSType getOsType()
    {
        String osName = toLowerCaseLocalized(OS_NAME.getString());
        if  (osName.contains("linux"))
            return LINUX;
        else if (osName.contains("mac"))
            return MAC;

        logger.warn("the current operating system, {}, is unsupported by Cassandra", osName);
        if (osName.contains("aix"))
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
            if (REQUIRE)
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
                        + " Increase RLIMIT_MEMLOCK.");
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

        try (FileInputStreamPlus fis = new FileInputStreamPlus(f))
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

            if (REQUIRE)
                logger.warn("fcntl({}, {}, {}) failed, errno ({}).", fd, command, flags, errno(e));
        }

        return result;
    }

    public static int tryOpenDirectory(String path)
    {
        return tryOpenDirectory(path, wrappedLibrary);
    }

    private static int tryOpenDirectory(String path, NativeLibraryWrapper library)
    {
        int fd = -1;

        try
        {
            return library.callOpen(path, O_RDONLY | O_DIRECTORY);
        }
        catch (UnsatisfiedLinkError e)
        {
            // JNA is unavailable just skipping Direct I/O
        }
        catch (RuntimeException e)
        {
            if (!(e instanceof LastErrorException))
                throw e;

            if (REQUIRE)
                logger.warn("openDirectory({}) failed, errno ({}).", path, errno(e));
        }

        return fd;
    }

    public static void trySync(int fd)
    {
        trySync(fd, null, wrappedLibrary);
    }

    /**
     * Sync a descriptor opened for a directory, tolerating filesystems without directory fsync support.
     * The caller retains ownership of the descriptor.
     */
    public static void trySyncDirectory(int fd, String path)
    {
        trySync(fd, path, wrappedLibrary);
    }

    public static void trySyncDirectory(String path)
    {
        trySyncDirectory(path, wrappedLibrary);
    }

    static void trySyncDirectory(String path, NativeLibraryWrapper library)
    {
        int fd = tryOpenDirectory(path, library);
        Throwables.maybeFail(() -> trySync(fd, path, library), () -> tryCloseFD(fd, library));
    }

    static void trySync(int fd, String directory, NativeLibraryWrapper library)
    {
        if (fd == -1)
            return;

        try
        {
            library.callFsync(fd);
        }
        catch (UnsatisfiedLinkError e)
        {
            // JNA is unavailable just skipping Direct I/O
        }
        catch (RuntimeException e)
        {
            if (!(e instanceof LastErrorException))
                throw e;

            int err = errno(e);
            // Capability errors are safe to ignore only for a kernel-verified directory descriptor.
            if (directory != null && isUnsupportedDirectorySync(err))
            {
                // Key the throttle on the directory so one unsupported mount cannot silence the others.
                NoSpamLogger.log(logger, NoSpamLogger.Level.WARN, directory, 10, TimeUnit.MINUTES,
                                 "Directory fsync on {} not supported by underlying filesystem, ignoring: errno ({})", directory, err);
                return;
            }

            if (REQUIRE)
            {
                String errMsg = String.format("fsync(%s) failed, errno (%s) %s", fd, err, e.getMessage());
                logger.warn(errMsg);
                throw new FSWriteError(e, errMsg);
            }
        }
    }

    private static boolean isUnsupportedDirectorySync(int error)
    {
        // Without a kernel-verified O_DIRECTORY the descriptor cannot be confirmed to be a directory,
        // so capability errors are not tolerated.
        if (O_DIRECTORY == 0)
            return false;

        ConstantSet errors = ConstantSet.getConstantSet("Errno");
        // Missing host constants remain failures rather than using another platform's values.
        return errors != null && (matchesErrno(errors.getConstant("EINVAL"), error)
                                  || matchesErrno(errors.getConstant("ENOTSUP"), error)
                                  || matchesErrno(errors.getConstant("EOPNOTSUPP"), error));
    }

    private static boolean matchesErrno(Constant expected, int actual)
    {
        return expected != null && expected.defined() && expected.intValue() == actual;
    }

    private static boolean isHostConstant(Constant constant)
    {
        // jnr's unknown-platform fallback reports defined() == true for synthetic, non-native OpenFlags
        // values; used only to sanity-check the resolved O_DIRECTORY candidate before it is probed above.
        return constant != null && constant.defined() && !(constant instanceof jnr.constants.platform.fake.OpenFlags);
    }

    public static void tryCloseFD(int fd)
    {
        tryCloseFD(fd, wrappedLibrary);
    }

    private static void tryCloseFD(int fd, NativeLibraryWrapper library)
    {
        if (fd == -1)
            return;

        try
        {
            library.callClose(fd);
        }
        catch (UnsatisfiedLinkError e)
        {
            // JNA is unavailable just skipping Direct I/O
        }
        catch (RuntimeException e)
        {
            if (!(e instanceof LastErrorException))
                throw e;

            if (REQUIRE)
            {
                String errMsg = String.format("close(%d) failed, errno (%d).", fd, errno(e));
                logger.warn(errMsg);
                throw new FSWriteError(e, errMsg);
            }
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
            if (REQUIRE)
                logger.warn("Unable to read fd field from FileChannel", e);
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
            if (REQUIRE)
            {
                JVMStabilityInspector.inspectThrowable(e);
                logger.warn("Unable to read fd field from FileDescriptor", e);
            }
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
        catch (UnsatisfiedLinkError e)
        {
            // if JNA is unavailable just skipping
        }
        catch (Exception e)
        {
            if (REQUIRE)
                logger.info("Failed to get PID from JNA", e);
        }

        return -1;
    }

    public static boolean isEnabled()
    {
        return REQUIRE;
    }
}
