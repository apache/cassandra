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
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.FileChannel;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.util.File;

import static org.apache.cassandra.config.CassandraRelevantProperties.CUSTOM_NATIVE_LIBRARY;

public interface INativeLibrary
{
    static final Logger logger = LoggerFactory.getLogger(INativeLibrary.class);

    INativeLibrary instance = !CUSTOM_NATIVE_LIBRARY.isPresent()
                             ? new NativeLibrary()
                             : FBUtilities.construct(CUSTOM_NATIVE_LIBRARY.getString(), "native library");

    public enum OSType
    {
        LINUX,
        MAC,
        WINDOWS,
        AIX,
        OTHER;
    }

    /**
     * @return true if current OS type is same the provided type
     */
    boolean isOS(INativeLibrary.OSType type);

    /**
     * Checks if the library has been successfully linked.
     * @return {@code true} if the library has been successfully linked, {@code false} otherwise.
     */
    boolean isAvailable();

    /**
     * @return true if jna memory is lockable
     */
    boolean jnaMemoryLockable();

    /**
     * try to lock JVM memory to avoid memory being swapped out
     */
    void tryMlockall();

    /**
     * try to advice OS to to free cached pages associated with the specified region.
     */
    void trySkipCache(File f, long offset, long len);

    /**
     * try to advice OS to to free cached pages associated with the specified region.
     */
    void trySkipCache(int fd, long offset, long len, String fileName);

    /**
     * try to advice OS to to free cached pages associated with the specified region.
     */
    void trySkipCache(int fd, long offset, int len, String fileName);

    /**
     * execute OS file control command
     */
    int tryFcntl(int fd, int command, int flags);

    /**
     * try to open given directory
     */
    int tryOpenDirectory(File path);

    /**
     * try to open given directory
     */
    int tryOpenDirectory(String path);

    /**
     * try fsync on given file
     */
    void trySync(int fd);

    /**
     * try to close given file
     */
    void tryCloseFD(int fd);

    /**
     * @return file descriptor for given async channel
     */
    int getfd(AsynchronousFileChannel channel);

    /**
     * @return file descriptor for given async channel
     */
    FileDescriptor getFileDescriptor(AsynchronousFileChannel channel);

    /**
     * @return file descriptor for given channel
     */
    int getfd(FileChannel channel);

    /**
     * @return file descriptor for given channel
     */
    @Nullable
    FileDescriptor getFileDescriptor(FileChannel channel);

    /**
     * Get system file descriptor from FileDescriptor object.
     * @param descriptor - FileDescriptor objec to get fd from
     * @return file descriptor, -1 or error
     */
    int getfd(FileDescriptor descriptor);

    /**
     * @return the PID of the JVM or -1 if we failed to get the PID
     */
    long getProcessID();
}
