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

import java.io.IOException;
import java.nio.file.Files;

import com.sun.jna.LastErrorException;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import jnr.constants.Constant;
import jnr.constants.ConstantSet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.Assume.assumeTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class NativeLibraryTest
{
    private static final int FD = 42;
    private static final String DIRECTORY = "directory";

    @Test
    public void testSkipCache()
    {
        File file = FileUtils.createDeletableTempFile("testSkipCache", "1");

        NativeLibrary.trySkipCache(file.path(), 0, 0);
    }

    @Test
    public void testCapabilityErrorsAreToleratedForDirectoriesOnly()
    {
        for (String name : new String[]{ "EINVAL", "ENOTSUP", "EOPNOTSUPP" })
            assertDirectoryOnlyCapability(name);
    }

    @Test
    public void testSyncPreservesStorageAndDescriptorErrors()
    {
        assumeTrue(NativeLibrary.isEnabled());
        for (String name : new String[]{ "EIO", "EBADF" })
            assertSyncFailure(new LastErrorException(hostErrno(name)));
    }

    @Test
    public void testNativeDirectorySync() throws IOException
    {
        File directory = new File(Files.createTempDirectory("native-directory-sync"));
        try
        {
            int fd = NativeLibrary.tryOpenDirectory(directory.path());
            assumeTrue(fd != -1);
            try
            {
                NativeLibrary.trySyncDirectory(fd, directory.path());
            }
            finally
            {
                NativeLibrary.tryCloseFD(fd);
            }
            Assert.assertTrue(new File(directory, "after-sync").createFileIfNotExists());
        }
        finally
        {
            directory.deleteRecursive();
        }
    }

    @Test
    public void testOpenDirectoryRejectsNonDirectory()
    {
        assumeTrue(NativeLibrary.isEnabled() && NativeLibrary.O_DIRECTORY != 0);
        File file = FileUtils.createDeletableTempFile("testOpenDirectory", "1");
        Assert.assertEquals(-1, NativeLibrary.tryOpenDirectory(file.path()));
    }

    @Test
    public void testUnsupportedDirectorySyncNamesEveryDirectory()
    {
        assumeTrue(NativeLibrary.isEnabled() && NativeLibrary.O_DIRECTORY != 0);
        LastErrorException failure = new LastErrorException(hostErrno("EINVAL"));
        Logger logger = (Logger) LoggerFactory.getLogger(NativeLibrary.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        try
        {
            // Distinct directories must each be reported: the throttle is keyed per directory.
            for (String directory : new String[]{ "/unsupported-mount-a", "/unsupported-mount-b" })
            {
                NativeLibraryWrapper library = mock(NativeLibraryWrapper.class);
                when(library.callOpen(eq(directory), anyInt())).thenReturn(FD);
                when(library.callFsync(FD)).thenThrow(failure);
                NativeLibrary.trySyncDirectory(directory, library);
            }
            assertThat(appender.list).anyMatch(event -> event.getFormattedMessage().contains("/unsupported-mount-a"))
                                     .anyMatch(event -> event.getFormattedMessage().contains("/unsupported-mount-b"));
        }
        finally
        {
            logger.detachAppender(appender);
        }
    }

    @Test
    public void testDirectorySyncClosesDescriptorOnUnexpectedFailure()
    {
        NativeLibraryWrapper library = directoryLibrary();
        IllegalStateException failure = new IllegalStateException("fsync failed");
        when(library.callFsync(FD)).thenThrow(failure);

        assertThatThrownBy(() -> NativeLibrary.trySyncDirectory(DIRECTORY, library)).isSameAs(failure);
        verify(library).callClose(FD);
    }

    @Test
    public void testDirectorySyncPreservesFailureWhenCloseFails()
    {
        assumeTrue(NativeLibrary.isEnabled());
        LastErrorException syncFailure = new LastErrorException(hostErrno("EIO"));
        LastErrorException closeFailure = new LastErrorException(hostErrno("EBADF"));
        NativeLibraryWrapper library = directoryLibrary();
        when(library.callFsync(FD)).thenThrow(syncFailure);
        when(library.callClose(FD)).thenThrow(closeFailure);

        Throwable failure = catchThrowable(() -> NativeLibrary.trySyncDirectory(DIRECTORY, library));
        assertThat(failure).isInstanceOf(FSWriteError.class).hasCause(syncFailure);
        Assert.assertEquals(1, failure.getSuppressed().length);
        assertThat(failure.getSuppressed()[0]).isInstanceOf(FSWriteError.class).hasCause(closeFailure);
    }

    private static void assertDirectoryOnlyCapability(String name)
    {
        assumeTrue(NativeLibrary.isEnabled() && NativeLibrary.O_DIRECTORY != 0);
        LastErrorException failure = new LastErrorException(hostErrno(name));
        NativeLibraryWrapper library = directoryLibrary();
        when(library.callFsync(FD)).thenThrow(failure);

        NativeLibrary.trySyncDirectory(DIRECTORY, library);
        verify(library).callFsync(FD);
        verify(library).callClose(FD);

        assertThatThrownBy(() -> NativeLibrary.trySync(FD, null, library))
        .isInstanceOf(FSWriteError.class)
        .hasCause(failure);
    }

    private static void assertSyncFailure(LastErrorException failure)
    {
        NativeLibraryWrapper library = directoryLibrary();
        when(library.callFsync(FD)).thenThrow(failure);

        assertThatThrownBy(() -> NativeLibrary.trySync(FD, null, library))
        .isInstanceOf(FSWriteError.class)
        .hasCause(failure);
        assertThatThrownBy(() -> NativeLibrary.trySyncDirectory(DIRECTORY, library))
        .isInstanceOf(FSWriteError.class)
        .hasCause(failure);
        verify(library).callClose(FD);
    }

    private static NativeLibraryWrapper directoryLibrary()
    {
        NativeLibraryWrapper library = mock(NativeLibraryWrapper.class);
        when(library.callOpen(eq(DIRECTORY), anyInt())).thenReturn(FD);
        return library;
    }

    private static int hostErrno(String name)
    {
        ConstantSet constants = ConstantSet.getConstantSet("Errno");
        assumeTrue(constants != null);
        Constant error = constants.getConstant(name);
        assumeTrue(error != null && error.defined() && !(error instanceof jnr.constants.platform.fake.Errno));
        return error.intValue();
    }

    @Test
    public void getPid()
    {
        long pid = NativeLibrary.getProcessID();
        Assert.assertTrue(pid > 0);
    }
}
