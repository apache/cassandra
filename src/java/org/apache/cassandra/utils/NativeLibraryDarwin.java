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

import java.util.Collections;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.jna.LastErrorException;
import com.sun.jna.Native;
import com.sun.jna.Pointer;

/**
 * A {@code NativeLibraryWrapper} implementation for Darwin/Mac.
 * <p>
 * When JNA is initialized, all methods that have the 'native' keyword
 * will be attmpted to be linked against. As Java doesn't have the equivalent
 * of a #ifdef, this means if a native method like posix_fadvise is defined in the
 * class but not available on the target operating system (e.g.
 * posix_fadvise is not availble on Darwin/Mac) this will cause the entire
 * initial linking and initialization of JNA to fail. This means other
 * native calls that are supported on that target operating system will be
 * unavailable simply because of one native defined method not supported
 * on the runtime operating system.
 * @see org.apache.cassandra.utils.NativeLibraryWrapper
 * @see NativeLibrary
 */
public class NativeLibraryDarwin implements NativeLibraryWrapper
{
    private static final Logger logger = LoggerFactory.getLogger(NativeLibraryDarwin.class);

    private static boolean available;

    static
    {
        try
        {
            Native.register(com.sun.jna.NativeLibrary.getInstance("c", Collections.emptyMap()));
            available = true;
        }
        catch (NoClassDefFoundError e)
        {
            logger.warn("JNA not found. Native methods will be disabled.");
        }
        catch (UnsatisfiedLinkError e)
        {
            logger.error("Failed to link the C library against JNA. Native methods will be unavailable.", e);
        }
        catch (NoSuchMethodError e)
        {
            logger.warn("Obsolete version of JNA present; unable to register C library. Upgrade to JNA 3.2.7 or later");
        }
    }

    private static native int mlockall(int flags) throws LastErrorException;
    private static native int munlockall() throws LastErrorException;
    private static native int fcntl(int fd, int command, long flags) throws LastErrorException;
    private static native int open(String path, int flags) throws LastErrorException;
    private static native int fsync(int fd) throws LastErrorException;
    private static native int close(int fd) throws LastErrorException;
    private static native Pointer strerror(int errnum) throws LastErrorException;
    private static native long getpid() throws LastErrorException;

    public int callMlockall(int flags) throws UnsatisfiedLinkError, RuntimeException
    {
        return mlockall(flags);
    }

    public int callMunlockall() throws UnsatisfiedLinkError, RuntimeException
    {
        return munlockall();
    }

    public int callFcntl(int fd, int command, long flags) throws UnsatisfiedLinkError, RuntimeException
    {
        return fcntl(fd, command, flags);
    }

    public int callPosixFadvise(int fd, long offset, int len, int flag) throws UnsatisfiedLinkError, RuntimeException
    {
        // posix_fadvise is not available on Darwin/Mac
        throw new UnsatisfiedLinkError();
    }

    public int callOpen(String path, int flags) throws UnsatisfiedLinkError, RuntimeException
    {
        return open(path, flags);
    }

    public int callFsync(int fd) throws UnsatisfiedLinkError, RuntimeException
    {
        return fsync(fd);
    }

    public int callClose(int fd) throws UnsatisfiedLinkError, RuntimeException
    {
        return close(fd);
    }

    public Pointer callStrerror(int errnum) throws UnsatisfiedLinkError, RuntimeException
    {
        return strerror(errnum);
    }

    public long callGetpid() throws UnsatisfiedLinkError, RuntimeException
    {
        return getpid();
    }

    public boolean isAvailable()
    {
        return available;
    }
}
