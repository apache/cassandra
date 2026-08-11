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
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Random;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

/**
 * {@link Reflink} can only actually share extents on xfs formatted with {@code -m reflink=1} or on btrfs, which
 * is not the filesystem any of this runs on locally. So what is tested here is the CONTRACT, which holds either
 * way: an attempt either shares the whole range or reports that it could not and leaves the destination alone,
 * and the argument validation that stands between the caller and a silent {@code EINVAL} is unconditional.
 * <p>
 * The assertions that need real support are guarded with {@code assumeTrue} and skip everywhere else; they are
 * what proves the ioctl call itself is right, so this test is worth running on an xfs scratch mount --
 * {@code -Djava.io.tmpdir=/mnt/xfs-scratch} is enough, since that is where the files below are created.
 */
public class ReflinkTest
{
    private static final long A = Reflink.RANGE_ALIGNMENT;
    private static final int SOURCE_LENGTH = (int) (4 * A);

    @BeforeClass
    public static void setupDD()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void forgetPreviousAnswers()
    {
        // The negative cache is JVM-global and keyed by directory, and every test here uses the same one.
        Reflink.resetSupportCache();
    }

    /**
     * The whole range is shared, or nothing is: there is no partial outcome to unwind. When it works the
     * destination must be byte-identical to the source range; when it does not, the destination must still be
     * untouched, because the caller's fallback assumes it is starting from an empty file.
     */
    @Test
    public void clonesTheWholeRangeOrLeavesTheDestinationEmpty() throws IOException
    {
        byte[] source = random(SOURCE_LENGTH);
        File src = write("reflink-src", source);
        File dst = FileUtils.createTempFile("reflink-dst", "1");
        dst.deleteOnExit();

        boolean cloned;
        try (FileChannel in = src.newReadChannel();
             FileChannel out = dst.newWriteChannel(File.WriteMode.OVERWRITE))
        {
            cloned = Reflink.tryCloneRange(in, A, out, 0, 2 * A, dst.parent());
            out.force(true);
        }

        if (cloned)
        {
            assertEquals("a clone must set the destination's length", 2 * A, dst.length());
            assertArrayEquals("shared bytes must be the source's bytes",
                              Arrays.copyOfRange(source, (int) A, (int) (3 * A)), readAll(dst));
            assertTrue("success must not poison the directory", Reflink.isPossibleIn(dst.parent()));
        }
        else
        {
            assertEquals("a refusal must write nothing at all", 0, dst.length());
            assertFalse("a refusal must be remembered for the directory", Reflink.isPossibleIn(dst.parent()));
        }
    }

    /**
     * Shared extents are copy-on-write, not a shared inode: writing through one file must not change the other.
     * This is the property that makes it safe to clone out of an sstable that is still being read.
     */
    @Test
    public void sharedExtentsAreCopyOnWrite() throws IOException
    {
        byte[] source = random(SOURCE_LENGTH);
        File src = write("reflink-cow-src", source);
        File dst = FileUtils.createTempFile("reflink-cow-dst", "1");
        dst.deleteOnExit();

        try (FileChannel in = src.newReadChannel();
             FileChannel out = dst.newWriteChannel(File.WriteMode.OVERWRITE))
        {
            assumeTrue("no filesystem support for sharing extents",
                       Reflink.tryCloneRange(in, 0, out, 0, 2 * A, dst.parent()));
            out.position(0);
            out.write(ByteBuffer.wrap(new byte[]{ (byte) ~source[0], (byte) ~source[1] }));
            out.force(true);
        }

        assertArrayEquals("the source must not have been modified through the clone", source, readAll(src));
        byte[] clone = readAll(dst);
        assertEquals((byte) ~source[0], clone[0]);
        assertEquals("and only the written bytes may differ", source[2], clone[2]);
    }

    /**
     * Misalignment is a caller bug -- the kernel answers it with a bare EINVAL, which would otherwise be
     * indistinguishable from "this filesystem cannot do it" and get the directory written off for the lifetime
     * of the process.
     */
    @Test
    public void unalignedArgumentsAreRejectedRatherThanRetriedAsACopy() throws IOException
    {
        File src = write("reflink-align-src", random(SOURCE_LENGTH));
        File dst = FileUtils.createTempFile("reflink-align-dst", "1");
        dst.deleteOnExit();

        try (FileChannel in = src.newReadChannel();
             FileChannel out = dst.newWriteChannel(File.WriteMode.OVERWRITE))
        {
            File dir = dst.parent();
            assertThatThrownBy(() -> Reflink.tryCloneRange(in, A + 1, out, 0, A, dir))
            .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("srcOffset");
            assertThatThrownBy(() -> Reflink.tryCloneRange(in, 0, out, 4096, A, dir))
            .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("dstOffset");
            assertThatThrownBy(() -> Reflink.tryCloneRange(in, 0, out, 0, A - 1, dir))
            .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("length");
            assertThatThrownBy(() -> Reflink.tryCloneRange(in, 0, out, 0, 0, dir))
            .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("positive");

            assertEquals("nothing may have been written", 0, dst.length());
        }
    }

    /** The alignment is what every caller has to arrange, so it must be a power of two they can mask with. */
    @Test
    public void alignmentIsAPowerOfTwoAndAtLeastAPage()
    {
        assertEquals(0, Reflink.RANGE_ALIGNMENT & (Reflink.RANGE_ALIGNMENT - 1));
        assertTrue("must be at least the largest page size Linux uses", Reflink.RANGE_ALIGNMENT >= 64 * 1024);
    }

    private static byte[] random(int length)
    {
        byte[] bytes = new byte[length];
        new Random(20260729L).nextBytes(bytes);
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
