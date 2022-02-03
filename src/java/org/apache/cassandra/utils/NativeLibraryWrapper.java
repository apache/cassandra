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

import com.sun.jna.Pointer;

/**
 * An interface to implement for using OS specific native methods.
 * @see NativeLibrary
 */
@Shared
public interface NativeLibraryWrapper
{
    /**
     * Checks if the library has been successfully linked.
     * @return {@code true} if the library has been successfully linked, {@code false} otherwise.
     */
    boolean isAvailable();

    int callMlockall(int flags) throws UnsatisfiedLinkError, RuntimeException;
    int callMunlockall() throws UnsatisfiedLinkError, RuntimeException;
    int callFcntl(int fd, int command, long flags) throws UnsatisfiedLinkError, RuntimeException;
    int callPosixFadvise(int fd, long offset, int len, int flag) throws UnsatisfiedLinkError, RuntimeException;
    int callOpen(String path, int flags) throws UnsatisfiedLinkError, RuntimeException;
    int callFsync(int fd) throws UnsatisfiedLinkError, RuntimeException;
    int callClose(int fd) throws UnsatisfiedLinkError, RuntimeException;
    Pointer callStrerror(int errnum) throws UnsatisfiedLinkError, RuntimeException;
    long callGetpid() throws UnsatisfiedLinkError, RuntimeException;
}
