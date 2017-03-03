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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.jna.Pointer;

/**
 * A CLibraryWrapper implementation for Windows.
 * <p>
 * As libc isn't available on Windows these implementations
 * will obviously be a no-op, however when possible implementations
 * are used that are Windows friendly that will return the same
 * return value.
 * @see org.apache.cassandra.utils.CLibraryWrapper
 * @see CLibrary
 */
public class CLibraryWindows implements CLibraryWrapper
{
    private static final Logger logger = LoggerFactory.getLogger(CLibraryWindows.class);

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
        try
        {
            return SigarLibrary.instance.getPid();
        }
        catch (Exception e)
        {
            logger.error("Failed to initialize or use Sigar Library", e);
        }

        return -1;
    }

    public boolean jnaAvailable()
    {
        return false;
    }
}
