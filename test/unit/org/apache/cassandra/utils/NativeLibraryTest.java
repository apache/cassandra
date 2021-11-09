/**
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


import java.io.FileDescriptor;
import java.io.IOException;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.FileChannel;
import java.util.Arrays;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;

public class NativeLibraryTest
{
    @BeforeClass
    public static void init()
    {
        DatabaseDescriptor.toolInitialization();
    }

    @Test
    public void testIsOs()
    {
        Assert.assertTrue(Arrays.stream(INativeLibrary.OSType.values()).anyMatch(INativeLibrary.instance::isOS));
    }

    @Test
    public void testIsAvailable()
    {
        Assert.assertTrue(INativeLibrary.instance.isAvailable());
    }

    @Test
    public void testGetfdForAsynchronousFileChannel() throws IOException
    {
        File file = FileUtils.createDeletableTempFile("testSkipCache", "get_fd_async");

        try (AsynchronousFileChannel channel = AsynchronousFileChannel.open(file.toPath()))
        {
            Assert.assertTrue(INativeLibrary.instance.getfd(channel) > 0);

            FileDescriptor fileDescriptor = INativeLibrary.instance.getFileDescriptor(channel);
            Assert.assertNotNull(fileDescriptor);
            Assert.assertEquals(INativeLibrary.instance.getfd(channel), INativeLibrary.instance.getfd(fileDescriptor));
        }
    }

    @Test
    public void testGetfdForFileChannel() throws IOException
    {
        File file = FileUtils.createDeletableTempFile("testSkipCache", "get_fd");

        try (FileChannel channel = FileChannel.open(file.toPath()))
        {
            Assert.assertTrue(INativeLibrary.instance.getfd(channel) > 0);

            FileDescriptor fileDescriptor = INativeLibrary.instance.getFileDescriptor(channel);
            Assert.assertNotNull(fileDescriptor);
            Assert.assertEquals(INativeLibrary.instance.getfd(channel), INativeLibrary.instance.getfd(fileDescriptor));
        }
    }

    @Test
    public void testInvalidFileDescriptor()
    {
        Assert.assertEquals(-1, INativeLibrary.instance.getfd((FileDescriptor) null));
    }

    @Test
    public void testTryFcntlWithIllegalArgument()
    {
        int invalidFd = 199991;
        Assert.assertEquals(-1, INativeLibrary.instance.tryFcntl(invalidFd, -1, -1));
    }

    @Test
    public void testOpenDirectory()
    {
        File file = FileUtils.createDeletableTempFile("testOpenDirectory", "1");

        int fd = INativeLibrary.instance.tryOpenDirectory(file.parent());
        INativeLibrary.instance.tryCloseFD(fd);
    }

    @Test
    public void testOpenDirectoryWithIllegalArgument()
    {
        File file = FileUtils.createDeletableTempFile("testOpenDirectoryWithIllegalArgument", "1");
        Assert.assertEquals(-1, INativeLibrary.instance.tryOpenDirectory(file.resolve("no_existing")));
    }

    @Test
    public void testTrySyncWithIllegalArgument()
    {
        INativeLibrary.instance.trySync(-1);

        int invalidFd = 199991;
        Assert.assertThrows(FSWriteError.class, () -> INativeLibrary.instance.trySync(invalidFd));
    }

    @Test
    public void testTryCloseFDWithIllegalArgument()
    {
        INativeLibrary.instance.tryCloseFD(-1);

        int invalidFd = 199991;
        Assert.assertThrows(FSWriteError.class, () -> INativeLibrary.instance.tryCloseFD(invalidFd));
    }

    @Test
    public void testSkipCache()
    {
        File file = FileUtils.createDeletableTempFile("testSkipCache", "1");

        INativeLibrary.instance.trySkipCache(file, 0, 0);
        INativeLibrary.instance.trySkipCache(file.resolve("no_existing"), 0, 0);

        // non-existing FD
        INativeLibrary.instance.trySkipCache(-1, 0, 0L, "non-existing file");
        INativeLibrary.instance.trySkipCache(-1, 0, 0, "non-existing file");
    }

    @Test
    public void getPid()
    {
        long pid = INativeLibrary.instance.getProcessID();
        Assert.assertTrue(pid > 0);
    }
}
