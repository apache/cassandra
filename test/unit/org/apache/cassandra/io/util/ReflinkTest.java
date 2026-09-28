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

package org.apache.cassandra.io.util;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.utils.FBUtilities;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests {@link Reflink}: argument validation, the negative-support caches, the errno classification, the
 * "false means nothing was written" contract, and -- on a reflink-capable filesystem -- the clones themselves.
 * <p>
 * Tests that need extent sharing are skipped unless {@code java.io.tmpdir} supports it; tests that need a filesystem
 * that refuses it are skipped when it does. Run once as-is and once with {@code -Dtmp.dir=} on an xfs
 * ({@code reflink=1}) or btrfs mount to exercise both. Cross-filesystem tests additionally use {@code /dev/shm} when
 * it is a separate tmpfs mount.
 */
public class ReflinkTest
{
    private static final long ALIGNMENT = Reflink.RANGE_ALIGNMENT;
    private static final int SOURCE_LENGTH = (int) (4 * ALIGNMENT);
    private static final long LARGEST_ALIGNED = Long.MAX_VALUE & -ALIGNMENT;

    private static final int EPERM = 1;
    private static final int EBADF = 9;
    private static final int EXDEV = 18;
    private static final int EISDIR = 21;
    private static final int EINVAL = 22;
    private static final int ENOTTY = 25;
    private static final int EFBIG = 27;
    private static final int ENOSYS = 38;
    private static final int EOPNOTSUPP = 95;

    private static final Path SHM = Paths.get("/dev/shm");

    /** Whether the test directory's filesystem actually shares extents, found by trying once. */
    private static boolean reflinkSupported;

    private File dir;
    private final List<File> otherDirs = new ArrayList<>();

    private Logger reflinkLogger;
    private Level previousLevel;
    private ListAppender<ILoggingEvent> logs;

    @BeforeClass
    public static void setupClass() throws IOException
    {
        DatabaseDescriptor.daemonInitialization();

        File probe = new File(Files.createTempDirectory(FileUtils.getTempDir().toPath(), "reflink-probe"));
        try
        {
            File src = write(probe, "src", random(SOURCE_LENGTH, 0));
            File dst = write(probe, "dst", new byte[0]);
            reflinkSupported = cloneFile(src, 0, dst, 0, ALIGNMENT);
        }
        finally
        {
            probe.deleteRecursive();
            Reflink.resetSupportCache();
        }
    }

    @Before
    public void setUp() throws IOException
    {
        Reflink.resetSupportCache();
        dir = new File(Files.createTempDirectory(FileUtils.getTempDir().toPath(), "reflink"));

        reflinkLogger = (Logger) LoggerFactory.getLogger(Reflink.class);
        previousLevel = reflinkLogger.getLevel();
        reflinkLogger.setLevel(Level.TRACE);
        logs = new ListAppender<>();
        logs.start();
        reflinkLogger.addAppender(logs);
    }

    @After
    public void tearDown()
    {
        reflinkLogger.detachAppender(logs);
        reflinkLogger.setLevel(previousLevel);
        Reflink.resetSupportCache();
        dir.deleteRecursive();
        for (File other : otherDirs)
            other.deleteRecursive();
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Constants and errno classification
    // ---------------------------------------------------------------------------------------------------------------

    @Test
    public void rangeAlignmentIsAMultipleOfEveryXfsAndBtrfsBlockSize()
    {
        assertEquals(64 << 10, ALIGNMENT);
        assertEquals(1, Long.bitCount(ALIGNMENT));
        for (long blockSize = 512; blockSize <= 64 << 10; blockSize <<= 1)
            assertEquals("block size " + blockSize, 0, ALIGNMENT % blockSize);
    }

    @Test
    public void ioctlRequestIsTheAsmGenericEncodingOfFicloneRange()
    {
        long expected = (1L << 30) | (32L << 16) | (0x94L << 8) | 13;
        assertEquals(expected, (long) staticField("FICLONERANGE"));
        assertEquals(0x4020940DL, (long) staticField("FICLONERANGE"));
        assertEquals("four __u64/__s64 fields, no padding", 4 * Long.BYTES, (int) staticField("FILE_CLONE_RANGE_SIZE"));
    }

    @Test
    public void onlyErrnosThatCondemnTheWholeFilesystemAreRemembered()
    {
        assertTrue(isFilesystemLimitation(EOPNOTSUPP));
        assertTrue(isFilesystemLimitation(ENOTTY));
        assertTrue(isFilesystemLimitation(ENOSYS));

        // per-call, per-file or per-pair answers: remembering any would switch a capable filesystem off
        for (int errno : new int[]{ 0, EPERM, EBADF, EXDEV, EISDIR, EINVAL, EFBIG, 45 /* BSD EOPNOTSUPP */, -EOPNOTSUPP })
            assertFalse("errno " + errno, isFilesystemLimitation(errno));
    }

    @Test
    public void strerrorNamesEveryErrnoItCanReport()
    {
        assertEquals("EPERM", strerror(EPERM));
        assertEquals("EBADF", strerror(EBADF));
        assertEquals("EINVAL", strerror(EINVAL));
        assertEquals("ENOSYS", strerror(ENOSYS));
        assertThat(strerror(EXDEV)).startsWith("EXDEV").contains("different filesystems");
        assertThat(strerror(ENOTTY)).startsWith("ENOTTY");
        assertThat(strerror(EOPNOTSUPP)).startsWith("EOPNOTSUPP");
        assertEquals("errno 12345", strerror(12345));
        assertEquals("errno -1", strerror(-1));
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Argument validation: caller bugs throw, on every platform, and change nothing
    // ---------------------------------------------------------------------------------------------------------------

    @Test
    public void nonPositiveLengthsAreRejected() throws IOException
    {
        try (FileChannel in = source().newReadChannel(); FileChannel out = emptyDestination().newReadWriteChannel())
        {
            for (long length : new long[]{ 0, -1, -ALIGNMENT, Long.MIN_VALUE })
                assertThatThrownBy(() -> Reflink.tryCloneRange(in, 0, out, 0, length, dir))
                .as("length %d", length)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("positive");
        }
    }

    @Test
    public void unalignedArgumentsAreRejectedAndNamed() throws IOException
    {
        try (FileChannel in = source().newReadChannel(); FileChannel out = emptyDestination().newReadWriteChannel())
        {
            // 4096 is a whole filesystem block on most mounts, and still not enough
            for (long unaligned : new long[]{ 1, 512, 4096, ALIGNMENT / 2, ALIGNMENT - 1, ALIGNMENT + 1, ALIGNMENT + 4096 })
            {
                assertThatThrownBy(() -> Reflink.tryCloneRange(in, unaligned, out, 0, ALIGNMENT, dir))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("srcOffset").hasMessageContaining(Long.toString(unaligned));
                assertThatThrownBy(() -> Reflink.tryCloneRange(in, 0, out, unaligned, ALIGNMENT, dir))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("dstOffset").hasMessageContaining(Long.toString(unaligned));
                assertThatThrownBy(() -> Reflink.tryCloneRange(in, 0, out, 0, unaligned, dir))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("length").hasMessageContaining(Long.toString(unaligned));
            }
        }
    }

    @Test
    public void negativeOffsetsAreRejectedEvenWhenTheMaskAlonePasses() throws IOException
    {
        try (FileChannel in = source().newReadChannel(); FileChannel out = emptyDestination().newReadWriteChannel())
        {
            for (long negative : new long[]{ -ALIGNMENT, -(1L << 62), Long.MIN_VALUE })
            {
                assertEquals("mask-aligned", 0, negative & (ALIGNMENT - 1));
                assertThatThrownBy(() -> Reflink.tryCloneRange(in, negative, out, 0, ALIGNMENT, dir))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("srcOffset").hasMessageContaining("non-negative");
                assertThatThrownBy(() -> Reflink.tryCloneRange(in, 0, out, negative, ALIGNMENT, dir))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("dstOffset").hasMessageContaining("non-negative");
            }
        }
    }

    @Test
    public void sourceRangesPastTheEndAreRejected() throws IOException
    {
        File unalignedSource = write(dir, "unaligned-src", random((int) (2 * ALIGNMENT + 100), 3));
        File emptySource = write(dir, "empty-src", new byte[0]);
        try (FileChannel in = source().newReadChannel();
             FileChannel unaligned = unalignedSource.newReadChannel();
             FileChannel empty = emptySource.newReadChannel();
             FileChannel out = emptyDestination().newReadWriteChannel())
        {
            assertPastTheEnd(in, SOURCE_LENGTH, ALIGNMENT, out);
            assertPastTheEnd(in, SOURCE_LENGTH - ALIGNMENT, 2 * ALIGNMENT, out);
            assertPastTheEnd(in, 0, SOURCE_LENGTH + ALIGNMENT, out);
            assertPastTheEnd(in, 100 * ALIGNMENT, ALIGNMENT, out);
            // the partial last block may only be cloned whole-to-EOF, which alignment already forbids here
            assertPastTheEnd(unaligned, 2 * ALIGNMENT, ALIGNMENT, out);
            assertPastTheEnd(unaligned, 0, 3 * ALIGNMENT, out);
            assertPastTheEnd(empty, 0, ALIGNMENT, out);
            assertEquals(0, out.size());
        }
    }

    @Test
    public void sourceRangeEndingExactlyAtEndOfFileIsAccepted() throws IOException
    {
        byte[] source = random(SOURCE_LENGTH, 0);
        File src = write(dir, "src", source);
        File dst = emptyDestination();

        boolean cloned = cloneFile(src, SOURCE_LENGTH - ALIGNMENT, dst, 0, ALIGNMENT);
        assertEquals(reflinkSupported, cloned);
        byte[] expected = cloned ? Arrays.copyOfRange(source, SOURCE_LENGTH - (int) ALIGNMENT, SOURCE_LENGTH) : new byte[0];
        assertArrayEquals(expected, readAll(dst));
    }

    @Test
    public void sourceRangeWhoseEndOverflowsIsRejected() throws IOException
    {
        // srcOffset + length wraps negative, which must not pass for "within the source"
        try (FileChannel in = source().newReadChannel(); FileChannel out = emptyDestination().newReadWriteChannel())
        {
            assertPastTheEnd(in, LARGEST_ALIGNED, ALIGNMENT, out);
            assertPastTheEnd(in, ALIGNMENT, LARGEST_ALIGNED, out);
            assertPastTheEnd(in, LARGEST_ALIGNED, LARGEST_ALIGNED, out);
            assertEquals(0, out.size());
        }
        assertNull("a caller bug must not be remembered against the filesystem", Reflink.unsupportedErrno(dir));
    }

    @Test
    public void argumentsAreValidatedEvenWhenSharingIsKnownToBeImpossible() throws IOException
    {
        rememberUnsupported(dir, EOPNOTSUPP);
        invoke("noteDescriptorsUnreachable", new Class<?>[0]);
        assertFalse(Reflink.isPossibleIn(dir));

        try (FileChannel in = source().newReadChannel(); FileChannel out = emptyDestination().newReadWriteChannel())
        {
            assertThatThrownBy(() -> Reflink.tryCloneRange(in, 1, out, 0, ALIGNMENT, dir))
            .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> Reflink.tryCloneRange(in, 0, out, 0, 0, dir))
            .isInstanceOf(IllegalArgumentException.class);
            assertPastTheEnd(in, SOURCE_LENGTH, ALIGNMENT, out);
        }
    }

    @Test
    public void argumentErrorsLeaveDestinationPositionsSupportAndLogsUntouched() throws IOException
    {
        byte[] existing = random((int) ALIGNMENT, 1);
        File dst = write(dir, "dst", existing);
        boolean possibleBefore = Reflink.isPossibleIn(dir);

        try (FileChannel in = source().newReadChannel(); FileChannel out = dst.newReadWriteChannel())
        {
            in.position(17);
            out.position(23);
            assertThatThrownBy(() -> Reflink.tryCloneRange(in, 1, out, 0, ALIGNMENT, dir))
            .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> Reflink.tryCloneRange(in, 0, out, -ALIGNMENT, ALIGNMENT, dir))
            .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> Reflink.tryCloneRange(in, 0, out, ALIGNMENT, -ALIGNMENT, dir))
            .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> Reflink.tryCloneRange(in, SOURCE_LENGTH, out, ALIGNMENT, ALIGNMENT, dir))
            .isInstanceOf(IllegalArgumentException.class);
            assertEquals(17, in.position());
            assertEquals(23, out.position());
        }

        assertArrayEquals(existing, readAll(dst));
        assertEquals(possibleBefore, Reflink.isPossibleIn(dir));
        assertNull(Reflink.unsupportedErrno(dir));
        assertThat(logs.list).as("caller bugs are thrown, not logged").isEmpty();
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Channels that cannot be sized or whose descriptors cannot be read
    // ---------------------------------------------------------------------------------------------------------------

    @Test
    public void unsizableSourceFallsBackWithoutDisablingSharing() throws IOException
    {
        byte[] existing = random((int) ALIGNMENT, 1);
        File dst = write(dir, "dst", existing);
        boolean possibleBefore = Reflink.isPossibleIn(dir);

        FileChannel closed = source().newReadChannel();
        closed.close();
        FileChannel failing = mock(FileChannel.class);
        when(failing.size()).thenThrow(new IOException("injected"));

        try (FileChannel out = dst.newReadWriteChannel())
        {
            assertFalse(Reflink.tryCloneRange(closed, 0, out, ALIGNMENT, ALIGNMENT, dir));
            assertFalse(Reflink.tryCloneRange(failing, 0, out, ALIGNMENT, ALIGNMENT, dir));
        }

        assertArrayEquals(existing, readAll(dst));
        assertEquals(possibleBefore, Reflink.isPossibleIn(dir));
        assertNull(Reflink.unsupportedErrno(dir));
        assertEquals(2, count(Level.WARN, "Could not size the clone source"));
    }

    @Test
    public void closedDestinationDoesNotDisableSharingForTheProcess() throws IOException
    {
        assumeTrue(FBUtilities.isLinux);
        File dst = emptyDestination();
        FileChannel closed = dst.newReadWriteChannel();
        closed.close();

        try (FileChannel in = source().newReadChannel())
        {
            assertFalse(Reflink.tryCloneRange(in, 0, closed, 0, ALIGNMENT, dir));
        }

        assertEquals(0, dst.length());
        assertNull(Reflink.unsupportedErrno(dir));
        assertTrue("one caller's closed channel is not a property of the JVM, and must not switch sharing off for "
                   + "every channel and filesystem until restart", Reflink.isPossibleIn(dir));
    }

    @Test
    public void foreignSourceChannelDisablesSharingForTheJvmRatherThanTheFilesystem() throws IOException
    {
        assumeTrue(FBUtilities.isLinux);
        byte[] existing = random((int) ALIGNMENT, 1);
        File dst = write(dir, "dst", existing);
        File elsewhere = new File(dir, "does-not-exist");

        FileChannel foreign = foreignChannel(SOURCE_LENGTH);

        try (FileChannel out = dst.newReadWriteChannel())
        {
            assertFalse(Reflink.tryCloneRange(foreign, 0, out, ALIGNMENT, ALIGNMENT, dir));
            assertFalse(Reflink.tryCloneRange(foreign, 0, out, ALIGNMENT, ALIGNMENT, dir));
        }

        assertArrayEquals(existing, readAll(dst));
        assertNull("the filesystem must not be blamed", Reflink.unsupportedErrno(dir));
        assertFalse(Reflink.isPossibleIn(dir));
        assertFalse("remembered for the process, not a directory", Reflink.isPossibleIn(elsewhere));
        assertEquals("warned once per process", 1, count(Level.WARN, "--add-opens"));
        assertEquals(0, count(Level.INFO, ""));

        // a perfectly good pair of channels is now refused without an attempt
        try (FileChannel in = source().newReadChannel(); FileChannel out = dst.newReadWriteChannel())
        {
            assertFalse(Reflink.tryCloneRange(in, 0, out, ALIGNMENT, ALIGNMENT, dir));
        }
        assertArrayEquals(existing, readAll(dst));
        assertEquals(1, count(Level.WARN, "--add-opens"));

        Reflink.resetSupportCache();
        assertTrue(Reflink.isPossibleIn(dir));
    }

    @Test
    public void foreignDestinationChannelIsNeverTouched() throws IOException
    {
        assumeTrue(FBUtilities.isLinux);
        FileChannel foreign = foreignChannel(0);

        try (FileChannel in = source().newReadChannel())
        {
            assertFalse(Reflink.tryCloneRange(in, 0, foreign, 0, ALIGNMENT, dir));
        }

        verify(foreign, never()).truncate(anyLong());
        verify(foreign, never()).write(any(ByteBuffer.class));
        verify(foreign, never()).write(any(ByteBuffer.class), anyLong());
        verify(foreign, never()).transferFrom(any(), anyLong(), anyLong());
        assertFalse(Reflink.isPossibleIn(dir));
        assertNull(Reflink.unsupportedErrno(dir));
    }

    // ---------------------------------------------------------------------------------------------------------------
    // The negative-support cache
    // ---------------------------------------------------------------------------------------------------------------

    @Test
    public void untriedFilesystemIsOptimisticOnLinuxOnly()
    {
        assertEquals(FBUtilities.isLinux, Reflink.isPossibleIn(dir));
        assertEquals(FBUtilities.isLinux, Reflink.isPossibleIn(new File(dir, "does-not-exist")));
        assertNull(Reflink.unsupportedErrno(dir));
    }

    @Test
    public void cachedRefusalFallsBackWithoutTouchingTheDestination() throws IOException
    {
        byte[] existing = random((int) ALIGNMENT, 2);
        File dst = write(dir, "dst", existing);
        File sibling = new File(dir, "sibling");
        sibling.createDirectoriesIfNotExists();

        rememberUnsupported(dir, EOPNOTSUPP);
        assertEquals(Integer.valueOf(EOPNOTSUPP), Reflink.unsupportedErrno(dir));
        assertEquals("directories on one filesystem share the answer",
                     Integer.valueOf(EOPNOTSUPP), Reflink.unsupportedErrno(sibling));
        assertFalse(Reflink.isPossibleIn(dir));
        assertFalse(Reflink.isPossibleIn(sibling));

        try (FileChannel in = source().newReadChannel(); FileChannel out = dst.newReadWriteChannel())
        {
            in.position(5);
            out.position(7);
            assertFalse(Reflink.tryCloneRange(in, 0, out, ALIGNMENT, ALIGNMENT, dir));
            assertFalse(Reflink.tryCloneRange(in, 0, out, 0, ALIGNMENT, sibling));
            assertEquals(5, in.position());
            assertEquals(7, out.position());
        }
        assertArrayEquals("must neither extend nor overwrite", existing, readAll(dst));

        Reflink.resetSupportCache();
        assertNull(Reflink.unsupportedErrno(dir));
        assertNull(Reflink.unsupportedErrno(sibling));
        assertEquals(FBUtilities.isLinux, Reflink.isPossibleIn(dir));
    }

    @Test
    public void unidentifiableDirectoriesAreRememberedByPath()
    {
        File missing = new File(dir, "missing");
        File otherMissing = new File(dir, "other-missing");

        rememberUnsupported(missing, ENOTTY);

        assertEquals(Integer.valueOf(ENOTTY), Reflink.unsupportedErrno(missing));
        assertEquals(Integer.valueOf(ENOTTY), Reflink.unsupportedErrno(new File(missing.path())));
        assertNull(Reflink.unsupportedErrno(otherMissing));
        assertNull("a path key must not condemn the filesystem it would be on", Reflink.unsupportedErrno(dir));
        assertEquals(FBUtilities.isLinux, Reflink.isPossibleIn(dir));
        assertFalse(Reflink.isPossibleIn(missing));
    }

    @Test
    public void refusalIsRememberedForTheDestinationFilesystemOnly() throws IOException
    {
        File shm = shmDirectory();
        File src = write(shm, "src", random(SOURCE_LENGTH, 0));
        File dst = write(shm, "dst", new byte[0]);

        // tmpfs cannot share extents
        assertFalse(cloneFile(src, 0, dst, 0, ALIGNMENT));
        assertEquals(0, dst.length());
        assertEquals(Integer.valueOf(EOPNOTSUPP), Reflink.unsupportedErrno(shm));
        assertFalse(Reflink.isPossibleIn(shm));

        assertNull("tmpfs must not condemn another filesystem", Reflink.unsupportedErrno(dir));
        assertTrue(Reflink.isPossibleIn(dir));
        assertEquals(1, count(Level.INFO, "unavailable on the filesystem holding " + shm));

        // remembered: the second attempt is not made, so is neither logged nor able to write
        assertFalse(cloneFile(src, 0, dst, 0, ALIGNMENT));
        assertEquals(0, dst.length());
        assertEquals(1, count(Level.INFO, ""));
        assertEquals(0, count(Level.WARN, ""));
    }

    @Test
    public void aDifferentErrnoIsLoggedAgainButARepeatIsNot()
    {
        rememberUnsupported(dir, EOPNOTSUPP);
        rememberUnsupported(dir, EOPNOTSUPP);
        assertEquals(1, count(Level.INFO, "(" + EOPNOTSUPP + ")"));

        rememberUnsupported(dir, ENOTTY);
        assertEquals(1, count(Level.INFO, "(" + ENOTTY + ")"));
        assertEquals("the latest answer is the one remembered", Integer.valueOf(ENOTTY), Reflink.unsupportedErrno(dir));

        rememberUnsupported(dir, ENOTTY);
        assertEquals(2, count(Level.INFO, ""));
        assertThat(logs.list.get(0).getFormattedMessage()).contains(dir.toString());
    }

    @Test
    public void resetForgetsBothFilesystemAndJvmAnswers()
    {
        File missing = new File(dir, "missing");
        rememberUnsupported(dir, EOPNOTSUPP);
        rememberUnsupported(missing, ENOSYS);
        invoke("noteDescriptorsUnreachable", new Class<?>[0]);
        assertFalse(Reflink.isPossibleIn(dir));

        Reflink.resetSupportCache();

        assertNull(Reflink.unsupportedErrno(dir));
        assertNull(Reflink.unsupportedErrno(missing));
        assertEquals(FBUtilities.isLinux, Reflink.isPossibleIn(dir));
        assertEquals(FBUtilities.isLinux, Reflink.isPossibleIn(missing));

        // and the JVM-wide warning can be given again after a reset
        invoke("noteDescriptorsUnreachable", new Class<?>[0]);
        assertEquals(2, count(Level.WARN, "--add-opens"));
    }

    @Test
    public void concurrentFirstRefusalsAreLoggedOnce() throws Exception
    {
        File shm = shmDirectory();
        File src = write(shm, "src", random(SOURCE_LENGTH, 0));
        int threads = 16;
        List<File> destinations = new ArrayList<>();
        for (int i = 0; i < threads; i++)
            destinations.add(write(shm, "dst" + i, new byte[0]));

        List<Boolean> results = runConcurrently(threads, i -> cloneFile(src, 0, destinations.get(i), 0, ALIGNMENT));

        assertThat(results).containsOnly(false);
        for (File dst : destinations)
            assertEquals(0, dst.length());
        assertEquals(Integer.valueOf(EOPNOTSUPP), Reflink.unsupportedErrno(shm));
        assertEquals(1, count(Level.INFO, "unavailable"));
        assertEquals(0, count(Level.WARN, ""));
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Refusals the kernel gives per call: never remembered, always WARNed, destination unchanged
    // ---------------------------------------------------------------------------------------------------------------

    @Test
    public void readOnlyDestinationIsRefusedPerCall() throws IOException
    {
        assumeTrue(FBUtilities.isLinux);
        byte[] existing = random((int) ALIGNMENT, 1);
        File dst = write(dir, "dst", existing);

        try (FileChannel in = source().newReadChannel(); FileChannel out = dst.newReadChannel())
        {
            assertFalse(Reflink.tryCloneRange(in, 0, out, ALIGNMENT, ALIGNMENT, dir));
            assertFalse(Reflink.tryCloneRange(in, 0, out, ALIGNMENT, ALIGNMENT, dir));
        }

        assertRefusedPerCall(EBADF, 2);
        assertArrayEquals(existing, readAll(dst));
        assertStillUsable();
    }

    @Test
    public void appendOnlyDestinationIsRefusedPerCall() throws IOException
    {
        assumeTrue(FBUtilities.isLinux);
        byte[] existing = random((int) ALIGNMENT, 1);
        File dst = write(dir, "dst", existing);

        try (FileChannel in = source().newReadChannel(); FileChannel out = dst.newWriteChannel(File.WriteMode.APPEND))
        {
            assertFalse(Reflink.tryCloneRange(in, 0, out, ALIGNMENT, ALIGNMENT, dir));
        }

        assertRefusedPerCall(EBADF, 1);
        assertArrayEquals(existing, readAll(dst));
        assertStillUsable();
    }

    @Test
    public void writeOnlySourceIsRefusedPerCall() throws IOException
    {
        assumeTrue(FBUtilities.isLinux);
        File src = source();
        File dst = emptyDestination();

        try (FileChannel in = FileChannel.open(src.toPath(), StandardOpenOption.WRITE);
             FileChannel out = dst.newReadWriteChannel())
        {
            assertFalse(Reflink.tryCloneRange(in, 0, out, 0, ALIGNMENT, dir));
        }

        assertRefusedPerCall(EBADF, 1);
        assertEquals(0, dst.length());
        assertStillUsable();
    }

    @Test
    public void crossFilesystemCloneIsRefusedPerCallAndPerPair() throws IOException
    {
        File shm = shmDirectory();
        File foreignSource = write(shm, "src", random(SOURCE_LENGTH, 0));
        byte[] existing = random((int) ALIGNMENT, 1);
        File dst = write(dir, "dst", existing);

        assertFalse(cloneFile(foreignSource, 0, dst, ALIGNMENT, ALIGNMENT));
        assertFalse(cloneFile(foreignSource, 0, dst, ALIGNMENT, ALIGNMENT));

        assertRefusedPerCall(EXDEV, 2);
        assertNull("EXDEV is a property of the pair, not the source's filesystem", Reflink.unsupportedErrno(shm));
        assertArrayEquals(existing, readAll(dst));
        assertStillUsable();
    }

    @Test
    public void refusedCloneLeavesBothChannelPositionsAlone() throws IOException
    {
        assumeTrue(FBUtilities.isLinux);
        File dst = write(dir, "dst", random((int) ALIGNMENT, 1));

        try (FileChannel in = source().newReadChannel(); FileChannel out = dst.newReadChannel())
        {
            in.position(3 * ALIGNMENT + 1);
            out.position(11);
            assertFalse(Reflink.tryCloneRange(in, 0, out, ALIGNMENT, ALIGNMENT, dir));
            assertEquals(3 * ALIGNMENT + 1, in.position());
            assertEquals(11, out.position());
        }
    }

    @Test
    public void unsupportedFilesystemIsRememberedAfterOneAttempt() throws IOException
    {
        assumeTrue(FBUtilities.isLinux);
        assumeFalse("the test directory supports extent sharing", reflinkSupported);
        byte[] existing = random((int) ALIGNMENT, 1);
        File dst = write(dir, "dst", existing);

        assertFalse(cloneFile(source(), 0, dst, ALIGNMENT, 2 * ALIGNMENT));
        assertEquals("a valid same-filesystem FICLONERANGE should fail only because sharing is unsupported; ENOTTY "
                     + "would mean a wrong request number and EBADF a wrongly marshalled descriptor",
                     Integer.valueOf(EOPNOTSUPP), Reflink.unsupportedErrno(dir));
        assertFalse(Reflink.isPossibleIn(dir));
        assertEquals(1, count(Level.INFO, "EOPNOTSUPP"));

        assertFalse(cloneFile(source(), 0, dst, ALIGNMENT, 2 * ALIGNMENT));
        assertEquals(1, count(Level.INFO, ""));
        assertEquals(0, count(Level.WARN, ""));
        assertArrayEquals(existing, readAll(dst));
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Successful clones (need a reflink-capable java.io.tmpdir)
    // ---------------------------------------------------------------------------------------------------------------

    @Test
    public void clonesTheWholeRangeOrLeavesTheDestinationUntouched() throws IOException
    {
        byte[] source = random(SOURCE_LENGTH, 0);
        byte[] prefix = random((int) ALIGNMENT, 1);
        File src = write(dir, "src", source);
        File dst = write(dir, "dst", prefix);

        boolean cloned;
        try (FileChannel in = src.newReadChannel(); FileChannel out = dst.newReadWriteChannel())
        {
            cloned = Reflink.tryCloneRange(in, ALIGNMENT, out, ALIGNMENT, 2 * ALIGNMENT, dir);
            out.force(true);
        }

        assertEquals(reflinkSupported, cloned);
        if (cloned)
            assertArrayEquals(splice(prefix, ALIGNMENT, source, ALIGNMENT, 2 * ALIGNMENT), readAll(dst));
        else
            assertArrayEquals("a refused clone must not change the destination", prefix, readAll(dst));
    }

    @Test
    public void successIsTracedAndLeavesSharingEnabled() throws IOException
    {
        assumeTrue(reflinkSupported);
        assertTrue(cloneFile(source(), 0, emptyDestination(), 0, ALIGNMENT));

        assertTrue(Reflink.isPossibleIn(dir));
        assertNull(Reflink.unsupportedErrno(dir));
        assertEquals(1, count(Level.TRACE, "Shared " + ALIGNMENT + " bytes"));
        assertEquals(0, count(Level.WARN, ""));
        assertEquals(0, count(Level.INFO, ""));
    }

    @Test
    public void writingTheCloneDoesNotChangeTheSource() throws IOException
    {
        assumeTrue(reflinkSupported);
        byte[] source = random(SOURCE_LENGTH, 0);
        File src = write(dir, "src", source);
        File dst = emptyDestination();

        try (FileChannel in = src.newReadChannel(); FileChannel out = dst.newReadWriteChannel())
        {
            assertTrue(Reflink.tryCloneRange(in, 0, out, 0, 2 * ALIGNMENT, dir));
            writeFully(out, 0, new byte[]{ (byte) ~source[0], (byte) ~source[1] });
            writeFully(out, 2 * ALIGNMENT - 1, new byte[]{ (byte) ~source[(int) (2 * ALIGNMENT - 1)] });
            out.force(true);
        }

        assertArrayEquals("writing the clone must not change its source", source, readAll(src));

        byte[] expected = Arrays.copyOf(source, (int) (2 * ALIGNMENT));
        expected[0] = (byte) ~expected[0];
        expected[1] = (byte) ~expected[1];
        expected[expected.length - 1] = (byte) ~expected[expected.length - 1];
        assertArrayEquals(expected, readAll(dst));
    }

    @Test
    public void writingTheSourceDoesNotChangeTheClone() throws IOException
    {
        assumeTrue(reflinkSupported);
        byte[] source = random(SOURCE_LENGTH, 0);
        File src = write(dir, "src", source);
        File dst = emptyDestination();

        assertTrue(cloneFile(src, ALIGNMENT, dst, 0, 2 * ALIGNMENT));
        try (FileChannel out = src.newReadWriteChannel())
        {
            writeFully(out, ALIGNMENT, random((int) (2 * ALIGNMENT), 9));
            out.truncate(ALIGNMENT + 1);
            out.force(true);
        }

        assertArrayEquals(Arrays.copyOfRange(source, (int) ALIGNMENT, (int) (3 * ALIGNMENT)), readAll(dst));
    }

    @Test
    public void cloneOutlivesItsSource() throws IOException
    {
        assumeTrue(reflinkSupported);
        byte[] source = random(SOURCE_LENGTH, 0);
        File src = write(dir, "src", source);
        File dst = emptyDestination();

        assertTrue(cloneFile(src, 0, dst, 0, SOURCE_LENGTH));
        src.delete();
        assertFalse(src.exists());

        assertArrayEquals(source, readAll(dst));
    }

    @Test
    public void cloneBeyondTheEndOfTheDestinationLeavesAZeroFilledHole() throws IOException
    {
        assumeTrue(reflinkSupported);
        byte[] source = random(SOURCE_LENGTH, 0);
        byte[] prefix = random(100, 1);
        File src = write(dir, "src", source);
        File dst = write(dir, "dst", prefix);

        assertTrue(cloneFile(src, 0, dst, 3 * ALIGNMENT, ALIGNMENT));

        assertEquals(4 * ALIGNMENT, dst.length());
        assertArrayEquals(splice(prefix, 3 * ALIGNMENT, source, 0, ALIGNMENT), readAll(dst));
    }

    @Test
    public void cloneIntoTheMiddleReplacesOnlyThatRange() throws IOException
    {
        assumeTrue(reflinkSupported);
        byte[] source = random(SOURCE_LENGTH, 0);
        byte[] existing = random((int) (4 * ALIGNMENT + 100), 1);
        File src = write(dir, "src", source);
        File dst = write(dir, "dst", existing);

        assertTrue(cloneFile(src, 2 * ALIGNMENT, dst, ALIGNMENT, 2 * ALIGNMENT));

        assertEquals("an interior clone must not change the length", existing.length, dst.length());
        assertArrayEquals(splice(existing, ALIGNMENT, source, 2 * ALIGNMENT, 2 * ALIGNMENT), readAll(dst));
    }

    @Test
    public void rangesClonedOutOfOrderReassembleTheSource() throws IOException
    {
        assumeTrue(reflinkSupported);
        byte[] source = random(SOURCE_LENGTH, 0);
        File src = write(dir, "src", source);
        File dst = emptyDestination();

        try (FileChannel in = src.newReadChannel(); FileChannel out = dst.newReadWriteChannel())
        {
            for (long chunk : new long[]{ 3, 1, 0, 2 })
            {
                assertTrue(Reflink.tryCloneRange(in, chunk * ALIGNMENT, out, chunk * ALIGNMENT, ALIGNMENT, dir));
                assertEquals("the first clone, of the last chunk, sets the length", SOURCE_LENGTH, out.size());
            }
        }

        assertArrayEquals(source, readAll(dst));
    }

    @Test
    public void oneSourceRangeCanBeClonedManyTimes() throws IOException
    {
        assumeTrue(reflinkSupported);
        byte[] source = random(SOURCE_LENGTH, 0);
        File src = write(dir, "src", source);
        File dst = emptyDestination();

        int copies = 32;
        try (FileChannel in = src.newReadChannel(); FileChannel out = dst.newReadWriteChannel())
        {
            for (int i = 0; i < copies; i++)
                assertTrue(Reflink.tryCloneRange(in, ALIGNMENT, out, i * ALIGNMENT, ALIGNMENT, dir));
        }

        byte[] actual = readAll(dst);
        assertEquals(copies * ALIGNMENT, actual.length);
        byte[] expected = Arrays.copyOfRange(source, (int) ALIGNMENT, (int) (2 * ALIGNMENT));
        for (int i = 0; i < copies; i++)
            assertArrayEquals("copy " + i, expected,
                              Arrays.copyOfRange(actual, (int) (i * ALIGNMENT), (int) ((i + 1) * ALIGNMENT)));
    }

    @Test
    public void cloneMovesNeitherChannelsPosition() throws IOException
    {
        assumeTrue(reflinkSupported);
        byte[] source = random(SOURCE_LENGTH, 0);
        byte[] tail = random(100, 7);
        File src = write(dir, "src", source);
        File dst = emptyDestination();

        try (FileChannel in = src.newReadChannel(); FileChannel out = dst.newReadWriteChannel())
        {
            in.position(ALIGNMENT + 3);
            assertTrue(Reflink.tryCloneRange(in, 0, out, 0, 2 * ALIGNMENT, dir));
            assertEquals(ALIGNMENT + 3, in.position());
            assertEquals("the caller must position the destination itself", 0, out.position());

            out.position(out.size());
            out.write(ByteBuffer.wrap(tail));
        }

        assertArrayEquals(splice(Arrays.copyOf(source, (int) (2 * ALIGNMENT)), 2 * ALIGNMENT, tail, 0, tail.length),
                          readAll(dst));
    }

    @Test
    public void alignedInteriorOfAnUnalignedSourceClonesAndTheTailCopies() throws IOException
    {
        assumeTrue(reflinkSupported);
        byte[] source = random((int) (3 * ALIGNMENT + 1234), 0);
        File src = write(dir, "src", source);
        File dst = emptyDestination();

        long interior = (source.length / ALIGNMENT) * ALIGNMENT;
        try (FileChannel in = src.newReadChannel(); FileChannel out = dst.newReadWriteChannel())
        {
            assertTrue(Reflink.tryCloneRange(in, 0, out, 0, interior, dir));
            out.position(interior);
            in.transferTo(interior, source.length - interior, out);
        }

        assertArrayEquals(source, readAll(dst));
    }

    @Test
    public void largeRangeIsSharedInOneCall() throws IOException
    {
        assumeTrue(reflinkSupported);
        int length = 32 << 20;
        byte[] source = random(length, 0);
        File src = write(dir, "src", source);
        File dst = write(dir, "dst", random((int) ALIGNMENT, 1));

        assertTrue(cloneFile(src, 0, dst, ALIGNMENT, length));

        byte[] actual = readAll(dst);
        assertEquals(ALIGNMENT + length, actual.length);
        assertArrayEquals(source, Arrays.copyOfRange(actual, (int) ALIGNMENT, actual.length));
    }

    @Test
    public void clonesWithinOneFileSucceedUnlessTheRangesOverlap() throws IOException
    {
        assumeTrue(reflinkSupported);
        byte[] source = random(SOURCE_LENGTH, 0);
        File file = write(dir, "self", source);

        try (FileChannel in = file.newReadChannel(); FileChannel out = file.newReadWriteChannel())
        {
            assertTrue(Reflink.tryCloneRange(in, 0, out, 4 * ALIGNMENT, ALIGNMENT, dir));
            assertFalse(Reflink.tryCloneRange(in, 0, out, ALIGNMENT, 2 * ALIGNMENT, dir));
        }

        assertRefusedPerCall(EINVAL, 1);
        assertArrayEquals(splice(source, 4 * ALIGNMENT, source, 0, ALIGNMENT), readAll(file));
        assertTrue(Reflink.isPossibleIn(dir));
    }

    @Test
    public void wrappingDestinationRangeIsRefusedWithoutDisablingSharing() throws IOException
    {
        assumeTrue(reflinkSupported);
        byte[] existing = random((int) ALIGNMENT, 1);
        File dst = write(dir, "dst", existing);

        boolean cloned;
        try
        {
            cloned = cloneFile(source(), 0, dst, LARGEST_ALIGNED, ALIGNMENT);
        }
        catch (IllegalArgumentException e)
        {
            cloned = false;
        }

        assertFalse(cloned);
        assertArrayEquals(existing, readAll(dst));
        assertNull(Reflink.unsupportedErrno(dir));
        assertStillUsable();
    }

    @Test
    public void concurrentClonesOfOneSourceAllSucceed() throws Exception
    {
        assumeTrue(reflinkSupported);
        byte[] source = random(SOURCE_LENGTH, 0);
        File src = write(dir, "src", source);
        int threads = 8;
        List<File> destinations = new ArrayList<>();
        for (int i = 0; i < threads; i++)
            destinations.add(emptyDestination("dst" + i));

        try (FileChannel in = src.newReadChannel())
        {
            List<Boolean> results = runConcurrently(threads, i -> {
                try (FileChannel out = destinations.get(i).newReadWriteChannel())
                {
                    // each thread takes the chunks in a different order to interleave with the others
                    for (int c = 0; c < 4; c++)
                    {
                        long chunk = (c + i) % 4;
                        if (!Reflink.tryCloneRange(in, chunk * ALIGNMENT, out, chunk * ALIGNMENT, ALIGNMENT, dir))
                            return false;
                    }
                    return true;
                }
            });
            assertThat(results).containsOnly(true);
        }

        for (File dst : destinations)
            assertArrayEquals(dst.toString(), source, readAll(dst));
    }

    // ---------------------------------------------------------------------------------------------------------------
    // undoPartialShare: what keeps a false return meaning "nothing was written" after a short remap
    // ---------------------------------------------------------------------------------------------------------------

    @Test
    public void undoDiscardsOnlyWhatARefusedCloneAppended() throws IOException
    {
        byte[] original = random((int) ALIGNMENT, 1);
        File dst = write(dir, "dst", original);

        try (FileChannel out = dst.newReadWriteChannel())
        {
            writeFully(out, ALIGNMENT, random((int) (2 * ALIGNMENT), 2));
            assertEquals(3 * ALIGNMENT, out.size());
            undoPartialShare(out, ALIGNMENT, dir);
            assertEquals(ALIGNMENT, out.size());
        }

        assertArrayEquals(original, readAll(dst));
        assertEquals(1, count(Level.WARN, "already shared " + 2 * ALIGNMENT + " bytes"));
    }

    @Test
    public void undoLeavesAnUnextendedDestinationAlone() throws IOException
    {
        FileChannel unchanged = mock(FileChannel.class);
        when(unchanged.size()).thenReturn(ALIGNMENT);
        undoPartialShare(unchanged, ALIGNMENT, dir);
        verify(unchanged, never()).truncate(anyLong());

        // shrunk by someone else meanwhile: must not be extended back either
        FileChannel shrunk = mock(FileChannel.class);
        when(shrunk.size()).thenReturn(ALIGNMENT / 2);
        undoPartialShare(shrunk, ALIGNMENT, dir);
        verify(shrunk, never()).truncate(anyLong());

        assertThat(logs.list).isEmpty();
    }

    @Test
    public void undoThatCannotTruncateThrowsRatherThanReturningFalse() throws IOException
    {
        IOException cause = new IOException("injected");
        FileChannel unshrinkable = mock(FileChannel.class);
        when(unshrinkable.size()).thenReturn(2 * ALIGNMENT);
        when(unshrinkable.truncate(anyLong())).thenThrow(cause);

        Throwable thrown = catchThrowable(() -> undoPartialShare(unshrinkable, ALIGNMENT, dir));
        assertThat(thrown).isInstanceOf(FSWriteError.class).hasCause(cause);
        assertEquals(dir.toString(), ((FSWriteError) thrown).path);
        verify(unshrinkable).truncate(ALIGNMENT);

        FileChannel unsizable = mock(FileChannel.class);
        when(unsizable.size()).thenThrow(cause);
        assertThatThrownBy(() -> undoPartialShare(unsizable, ALIGNMENT, dir))
        .isInstanceOf(FSWriteError.class)
        .hasCause(cause);
        verify(unsizable, never()).truncate(anyLong());
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------------------------------------------

    private interface IndexedTask<T>
    {
        T run(int index) throws Exception;
    }

    private static <T> List<T> runConcurrently(int threads, IndexedTask<T> task) throws Exception
    {
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        try
        {
            CountDownLatch start = new CountDownLatch(1);
            List<Future<T>> futures = new ArrayList<>();
            for (int i = 0; i < threads; i++)
            {
                int index = i;
                Callable<T> callable = () -> {
                    start.await();
                    return task.run(index);
                };
                futures.add(executor.submit(callable));
            }
            start.countDown();
            List<T> results = new ArrayList<>();
            for (Future<T> future : futures)
                results.add(future.get(1, TimeUnit.MINUTES));
            return results;
        }
        finally
        {
            executor.shutdownNow();
        }
    }

    /** Refused by the kernel with {@code errno} {@code times} times, and neither remembered nor promoted to INFO. */
    private void assertRefusedPerCall(int errno, int times)
    {
        assertEquals(logs.list.stream().map(ILoggingEvent::getFormattedMessage).collect(Collectors.joining("\n")),
                     times, count(Level.WARN, "failed with errno " + errno + " (" + strerror(errno) + ")"));
        assertEquals(0, count(Level.INFO, ""));
        assertNull(Reflink.unsupportedErrno(dir));
        assertTrue(Reflink.isPossibleIn(dir));
    }

    /** A valid clone in the test directory still behaves exactly as on a fresh process. */
    private void assertStillUsable() throws IOException
    {
        byte[] source = random(SOURCE_LENGTH, 4);
        File src = write(dir, "usable-src", source);
        File dst = emptyDestination("usable-dst");
        boolean cloned = cloneFile(src, 0, dst, 0, 2 * ALIGNMENT);
        assertEquals(reflinkSupported, cloned);
        assertArrayEquals(cloned ? Arrays.copyOf(source, (int) (2 * ALIGNMENT)) : new byte[0], readAll(dst));
    }

    private void assertPastTheEnd(FileChannel in, long srcOffset, long length, FileChannel out)
    {
        assertThatThrownBy(() -> Reflink.tryCloneRange(in, srcOffset, out, 0, length, dir))
        .as("srcOffset %d, length %d", srcOffset, length)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("past the source");
    }

    private int count(Level level, String substring)
    {
        return (int) logs.list.stream()
                              .filter(e -> e.getLevel() == level && e.getFormattedMessage().contains(substring))
                              .count();
    }

    /** An open channel of the given size that is not a {@code sun.nio.ch.FileChannelImpl}, as another provider's. */
    private static FileChannel foreignChannel(long size) throws IOException
    {
        FileChannel foreign = mock(FileChannel.class);
        when(foreign.isOpen()).thenReturn(true);
        when(foreign.size()).thenReturn(size);
        return foreign;
    }

    private File shmDirectory() throws IOException
    {
        assumeTrue(FBUtilities.isLinux && Files.isDirectory(SHM) && Files.isWritable(SHM));
        assumeFalse("/dev/shm must be a separate filesystem from the test directory",
                    Files.getFileStore(SHM).equals(Files.getFileStore(dir.toPath())));
        File shm = new File(Files.createTempDirectory(SHM, "reflink"));
        otherDirs.add(shm);
        return shm;
    }

    private File source() throws IOException
    {
        File src = new File(dir, "source");
        return src.exists() ? src : write(dir, "source", random(SOURCE_LENGTH, 0));
    }

    private File emptyDestination() throws IOException
    {
        return emptyDestination("destination");
    }

    private File emptyDestination(String name) throws IOException
    {
        return write(dir, name, new byte[0]);
    }

    private static boolean cloneFile(File src, long srcOffset, File dst, long dstOffset, long length) throws IOException
    {
        try (FileChannel in = src.newReadChannel(); FileChannel out = dst.newReadWriteChannel())
        {
            return Reflink.tryCloneRange(in, srcOffset, out, dstOffset, length, dst.parent());
        }
    }

    /** {@code base} with {@code length} bytes of {@code from} at {@code fromOffset} laid over it at {@code at}. */
    private static byte[] splice(byte[] base, long at, byte[] from, long fromOffset, long length)
    {
        byte[] result = Arrays.copyOf(base, (int) Math.max(base.length, at + length));
        System.arraycopy(from, (int) fromOffset, result, (int) at, (int) length);
        return result;
    }

    private static void writeFully(FileChannel channel, long position, byte[] bytes) throws IOException
    {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        while (buffer.hasRemaining())
            position += channel.write(buffer, position);
    }

    private static byte[] random(int length, long salt)
    {
        byte[] bytes = new byte[length];
        new Random(20260824L ^ salt).nextBytes(bytes);
        return bytes;
    }

    private static File write(File directory, String name, byte[] bytes) throws IOException
    {
        File file = new File(directory, name);
        Files.write(file.toPath(), bytes);
        return file;
    }

    private static byte[] readAll(File file) throws IOException
    {
        return Files.readAllBytes(file.toPath());
    }

    private static void rememberUnsupported(File directory, int errno)
    {
        String key = (String) invoke("cacheKey", new Class<?>[]{ File.class }, directory);
        invoke("noteUnsupported", new Class<?>[]{ String.class, File.class, int.class, String.class },
               key, directory, errno, strerror(errno));
    }

    private static boolean isFilesystemLimitation(int errno)
    {
        return (boolean) invoke("isFilesystemLimitation", new Class<?>[]{ int.class }, errno);
    }

    private static String strerror(int errno)
    {
        return (String) invoke("strerror", new Class<?>[]{ int.class }, errno);
    }

    private static void undoPartialShare(FileChannel dst, long lengthOnEntry, File directory)
    {
        invoke("undoPartialShare", new Class<?>[]{ FileChannel.class, long.class, File.class }, dst, lengthOnEntry, directory);
    }

    private static Object staticField(String name)
    {
        try
        {
            Field field = Reflink.class.getDeclaredField(name);
            field.setAccessible(true);
            return field.get(null);
        }
        catch (ReflectiveOperationException e)
        {
            throw new AssertionError("Reflink." + name + " is unavailable", e);
        }
    }

    /** Invokes a private static method of {@link Reflink}, rethrowing what it threw unwrapped. */
    private static Object invoke(String name, Class<?>[] signature, Object... arguments)
    {
        try
        {
            Method method = Reflink.class.getDeclaredMethod(name, signature);
            method.setAccessible(true);
            return method.invoke(null, arguments);
        }
        catch (InvocationTargetException e)
        {
            if (e.getCause() instanceof RuntimeException)
                throw (RuntimeException) e.getCause();
            if (e.getCause() instanceof Error)
                throw (Error) e.getCause();
            throw new AssertionError(e.getCause());
        }
        catch (ReflectiveOperationException e)
        {
            throw new AssertionError("Reflink." + name + " is unavailable", e);
        }
    }
}
