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
 * A {@code NativeLibraryWrapper} implementation for Windows.
 * <p> This implementation only offers support for the {@code callGetpid} method
 * using the Windows/Kernel32 library.</p>
 *
 * @see org.apache.cassandra.utils.NativeLibraryWrapper
 * @see NativeLibrary
 */
public class NativeLibraryWindows implements NativeLibraryWrapper
{
    private static final Logger logger = LoggerFactory.getLogger(NativeLibraryWindows.class);

    private static boolean available;

    static
    {
        try
        {
            Native.register(com.sun.jna.NativeLibrary.getInstance("kernel32", Collections.emptyMap()));
            available = true;
        }
        catch (NoClassDefFoundError e)
        {
            logger.warn("JNA not found. Native methods will be disabled.");
        }
        catch (UnsatisfiedLinkError e)
        {
            logger.error("Failed to link the Windows/Kernel32 library against JNA. Native methods will be unavailable.", e);
        }
        catch (NoSuchMethodError e)
        {
            logger.warn("Obsolete version of JNA present; unable to register Windows/Kernel32 library. Upgrade to JNA 3.2.7 or later");
        }
    }

    /**
     * Retrieves the process identifier of the calling process (<a href='https://msdn.microsoft.com/en-us/library/windows/desktop/ms683180(v=vs.85).aspx'>GetCurrentProcessId function</a>).
     *
     * @return the process identifier of the calling process
     */
    private static native long GetCurrentProcessId() throws LastErrorException;

    public int callMlockall(int flags) throws UnsatisfiedLinkError, RuntimeException
    {
        throw new UnsatisfiedLinkError();
    }

    public int callMunlockall() throws UnsatisfiedLinkError, RuntimeException
    {
        throw new UnsatisfiedLinkError();
    }

    public int callFcntl(int fd, int command, long flags) throws UnsatisfiedLinkError, RuntimeException
    {
        throw new UnsatisfiedLinkError();
    }

    public int callPosixFadvise(int fd, long offset, int len, int flag) throws UnsatisfiedLinkError, RuntimeException
    {
        throw new UnsatisfiedLinkError();
    }

    public int callOpen(String path, int flags) throws UnsatisfiedLinkError, RuntimeException
    {
        throw new UnsatisfiedLinkError();
    }

    public int callFsync(int fd) throws UnsatisfiedLinkError, RuntimeException
    {
        throw new UnsatisfiedLinkError();
    }

    public int callClose(int fd) throws UnsatisfiedLinkError, RuntimeException
    {
        throw new UnsatisfiedLinkError();
    }

    public Pointer callStrerror(int errnum) throws UnsatisfiedLinkError, RuntimeException
    {
        throw new UnsatisfiedLinkError();
    }

    /**
     * @return the PID of the JVM running
     * @throws UnsatisfiedLinkError if we fail to link against Sigar
     * @throws RuntimeException if another unexpected error is thrown by Sigar
     */
    public long callGetpid() throws UnsatisfiedLinkError, RuntimeException
    {
        return GetCurrentProcessId();
    }

    public boolean isAvailable()
    {
        return available;
    }
}
