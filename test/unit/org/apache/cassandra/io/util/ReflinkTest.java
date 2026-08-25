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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

/**
 * Tests the contract that lets callers safely fall back to copying when extent sharing is unavailable. Tests that
 * require a reflink-capable filesystem are skipped unless the ioctl actually succeeds; run with
 * {@code -Djava.io.tmpdir} on an xfs or btrfs mount to exercise them.
 */
public class ReflinkTest
{
    private static final long ALIGNMENT = Reflink.RANGE_ALIGNMENT;
    private static final int SOURCE_LENGTH = (int) (4 * ALIGNMENT);
    private static final int EOPNOTSUPP = 95;

    @BeforeClass
    public static void setupClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void resetBeforeTest()
    {
        Reflink.resetSupportCache();
    }

    @After
    public void resetAfterTest()
    {
        Reflink.resetSupportCache();
    }

    @Test
    public void clonesTheWholeRangeOrLeavesTheDestinationUntouched() throws IOException
    {
        byte[] source = random(SOURCE_LENGTH);
        byte[] prefix = random((int) ALIGNMENT, 1L);
        File src = write("reflink-range-src", source);
        File dst = write("reflink-range-dst", prefix);

        boolean cloned;
        try (FileChannel in = src.newReadChannel();
             FileChannel out = dst.newReadWriteChannel())
        {
            cloned = Reflink.tryCloneRange(in, ALIGNMENT, out, ALIGNMENT, 2 * ALIGNMENT, dst.parent());
            out.force(true);
        }

        if (cloned)
        {
            byte[] expected = new byte[(int) (3 * ALIGNMENT)];
            System.arraycopy(prefix, 0, expected, 0, prefix.length);
            System.arraycopy(source, (int) ALIGNMENT, expected, prefix.length, (int) (2 * ALIGNMENT));
            assertArrayEquals(expected, readAll(dst));
        }
        else
        {
            assertArrayEquals("a refused clone must not change the destination", prefix, readAll(dst));
        }
    }

    @Test
    public void invalidRangesAreRejectedWithoutChangingSupportOrDestination() throws IOException
    {
        File src = write("reflink-invalid-src", random(SOURCE_LENGTH));
        File dst = FileUtils.createTempFile("reflink-invalid-dst", "1");
        dst.deleteOnExit();

        try (FileChannel in = src.newReadChannel();
             FileChannel out = dst.newWriteChannel(File.WriteMode.OVERWRITE))
        {
            File directory = dst.parent();
            boolean possibleBefore = Reflink.isPossibleIn(directory);

            assertThatThrownBy(() -> Reflink.tryCloneRange(in, ALIGNMENT + 1, out, 0, ALIGNMENT, directory))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("srcOffset");
            assertThatThrownBy(() -> Reflink.tryCloneRange(in, 0, out, 1, ALIGNMENT, directory))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("dstOffset");
            assertThatThrownBy(() -> Reflink.tryCloneRange(in, 0, out, 0, ALIGNMENT - 1, directory))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("length");
            assertThatThrownBy(() -> Reflink.tryCloneRange(in, 0, out, 0, 0, directory))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("positive");
            assertThatThrownBy(() -> Reflink.tryCloneRange(in, -ALIGNMENT, out, 0, ALIGNMENT, directory))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("srcOffset");
            assertThatThrownBy(() -> Reflink.tryCloneRange(in, SOURCE_LENGTH, out, 0, ALIGNMENT, directory))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("past the source");

            assertEquals(0, dst.length());
            assertEquals("caller errors must not disable reflinking", possibleBefore,
                         Reflink.isPossibleIn(directory));
        }
    }

    @Test
    public void cachedUnsupportedFilesystemUsesCleanFallback() throws IOException
    {
        byte[] existing = random((int) ALIGNMENT, 2L);
        File src = write("reflink-cached-src", random(SOURCE_LENGTH));
        File dst = write("reflink-cached-dst", existing);
        File directory = dst.parent();
        File sibling = new File(java.nio.file.Files.createTempDirectory("reflink-cache-sibling"));
        sibling.deleteRecursiveOnExit();

        rememberUnsupported(directory);
        assertEquals(Integer.valueOf(EOPNOTSUPP), Reflink.unsupportedErrno(directory));
        assertEquals("directories on one filesystem must share the negative cache",
                     Integer.valueOf(EOPNOTSUPP), Reflink.unsupportedErrno(sibling));
        assertFalse(Reflink.isPossibleIn(directory));

        try (FileChannel in = src.newReadChannel();
             FileChannel out = dst.newReadWriteChannel())
        {
            assertFalse(Reflink.tryCloneRange(in, 0, out, ALIGNMENT, ALIGNMENT, directory));
        }

        assertArrayEquals("cached fallback must not extend or overwrite the destination", existing, readAll(dst));

        Reflink.resetSupportCache();
        assertNull(Reflink.unsupportedErrno(directory));
        assertNull(Reflink.unsupportedErrno(sibling));
    }

    @Test
    public void successfulReflinkIsCopyOnWrite() throws IOException
    {
        byte[] source = random(SOURCE_LENGTH);
        File src = write("reflink-cow-src", source);
        File dst = FileUtils.createTempFile("reflink-cow-dst", "1");
        dst.deleteOnExit();

        try (FileChannel in = src.newReadChannel();
             FileChannel out = dst.newWriteChannel(File.WriteMode.OVERWRITE))
        {
            assumeTrue("no reflink support on the test filesystem",
                       Reflink.tryCloneRange(in, 0, out, 0, 2 * ALIGNMENT, dst.parent()));

            out.position(0);
            ByteBuffer mutation = ByteBuffer.wrap(new byte[]{ (byte) ~source[0], (byte) ~source[1] });
            while (mutation.hasRemaining())
                out.write(mutation);
            out.force(true);
        }

        assertArrayEquals("writing the clone must not change its source", source, readAll(src));

        byte[] expected = Arrays.copyOf(source, (int) (2 * ALIGNMENT));
        expected[0] = (byte) ~expected[0];
        expected[1] = (byte) ~expected[1];
        assertArrayEquals(expected, readAll(dst));
        assertTrue("a successful clone must leave the filesystem enabled", Reflink.isPossibleIn(dst.parent()));
    }

    private static void rememberUnsupported(File directory)
    {
        String key = (String) invoke("cacheKey", new Class<?>[]{ File.class }, directory);
        invoke("noteUnsupported", new Class<?>[]{ String.class, File.class, int.class, String.class },
               key, directory, EOPNOTSUPP, "EOPNOTSUPP");
    }

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
            throw new AssertionError(e.getCause());
        }
        catch (ReflectiveOperationException e)
        {
            throw new AssertionError("Reflink." + name + " is unavailable", e);
        }
    }

    private static byte[] random(int length)
    {
        return random(length, 0L);
    }

    private static byte[] random(int length, long salt)
    {
        byte[] bytes = new byte[length];
        new Random(20260824L ^ salt).nextBytes(bytes);
        return bytes;
    }

    private static File write(String name, byte[] bytes) throws IOException
    {
        File file = FileUtils.createTempFile(name, "1");
        file.deleteOnExit();
        try (SequentialWriter writer = new SequentialWriter(file))
        {
            writer.write(bytes);
            writer.finish();
        }
        assertEquals(bytes.length, file.length());
        return file;
    }

    private static byte[] readAll(File file) throws IOException
    {
        return java.nio.file.Files.readAllBytes(file.toPath());
    }
}
