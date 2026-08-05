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
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;
import java.util.Random;

import com.google.common.base.Throwables;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.utils.NativeLibrary;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

/**
 * {@link Reflink} can only actually share extents on xfs formatted with {@code -m reflink=1} or on btrfs, which
 * is not the filesystem any of this runs on locally. So what is tested here is the CONTRACT, which holds either
 * way: an attempt either shares the whole range or reports that it could not and leaves the destination at the
 * length it had on entry, and the argument validation that stands between the caller and a silent {@code EINVAL}
 * is unconditional.
 * <p>
 * The assertions that need real support are guarded with {@code assumeTrue} and skip everywhere else; they are
 * what proves the ioctl call itself is right, so this test is worth running on an xfs scratch mount --
 * {@code -Djava.io.tmpdir=/mnt/xfs-scratch} is enough, since that is where the files below are created.
 * <p>
 * What no assertion here can reach, and what a loopback {@code mkfs.xfs -m reflink=1} target in CI would buy:
 * <ul>
 * <li>The FIELD ORDER of {@code struct file_clone_range} beyond {@code src_fd}. A filesystem that cannot share
 *     extents answers {@code EOPNOTSUPP} from {@code vfs_clone_file_range} before the kernel has looked at a
 *     single offset, so {@code src_offset}, {@code src_length} and {@code dest_offset} could be in any order and
 *     every assertion below would still pass -- while every split child on a real xfs took its bytes from the
 *     wrong place. {@code src_fd} alone is pinned, because the kernel does read it first
 *     ({@code fdget} in {@code ioctl_file_clone}) and answers {@code EBADF} rather than {@code EOPNOTSUPP} if it
 *     finds something else there; see {@link #ficloneRangeIsTheRequestNumberTheKernelRecognises}.</li>
 * <li>An actual partial share, and therefore the truncation that undoes it. It takes a clone the kernel clamps
 *     ({@code RLIMIT_FSIZE}, {@code s_maxbytes}, or a source that shrank under it) on a filesystem that shares
 *     extents at all. {@link #undoPartialShareTruncatesBackAndNothingElse} drives that method directly instead.</li>
 * <li>Copy-on-write itself, and that the destination really holds the source's bytes afterwards -- both
 *     {@code assumeTrue}d out.</li>
 * </ul>
 */
public class ReflinkTest
{
    private static final long A = Reflink.RANGE_ALIGNMENT;
    private static final int SOURCE_LENGTH = (int) (4 * A);

    // Linux asm-generic, as Reflink's own constants are asserted to be by errnosAreClassifiedByWhatTheyRuleOut.
    private static final int EPERM = 1;
    private static final int EBADF = 9;
    private static final int EXDEV = 18;
    private static final int EINVAL = 22;
    private static final int ENOTTY = 25;
    private static final int ENOSYS = 38;
    private static final int EOPNOTSUPP = 95;

    @BeforeClass
    public static void setupDD()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void forgetPreviousAnswers()
    {
        // The negative cache is JVM-global and keyed by the FILESYSTEM the destination sits on, so every test
        // here shares one entry -- they all write into java.io.tmpdir -- and the JVM-wide "no reachable file
        // descriptor" answer is shared by all of them regardless of where they write.
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
            // Every refusal reachable here is remembered: the destination is empty and the arguments are aligned
            // and inside the source, so the kernel has no bad-argument EINVAL to give, source and destination are
            // in one directory and so on one filesystem (no EXDEV), and neither file is immutable (no EPERM).
            // That leaves the answers of a filesystem that cannot do this at all, which are cached -- for the
            // FILESYSTEM, so a second directory on the same mount inherits it, see
            // oneAnswerIsRememberedPerFilesystemNotPerDirectory.
            assertFalse("a refusal must be remembered for the filesystem", Reflink.isPossibleIn(dst.parent()));
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

            // A negative offset passes the alignment mask on its own (-A & (A-1) == 0), so it is rejected
            // explicitly rather than reaching the kernel as a huge u64.
            assertThatThrownBy(() -> Reflink.tryCloneRange(in, -A, out, 0, A, dir))
            .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("srcOffset");
            assertThatThrownBy(() -> Reflink.tryCloneRange(in, 0, out, -A, A, dir))
            .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("dstOffset");

            assertEquals("nothing may have been written", 0, dst.length());
        }
    }

    /**
     * A source range running past the source's end is a CALLER bug, and it has to be reported as one.
     *
     * <p>The kernel answers it with EINVAL, which is indistinguishable from every other bad-argument EINVAL. When
     * EINVAL was classified as a filesystem limitation, one such call permanently switched extent sharing off for
     * the whole data directory -- on a filesystem that supports it -- and said so at INFO as though the filesystem
     * were at fault. So the precondition is checked here, before the ioctl, and the negative cache is left alone.
     */
    @Test
    public void aSourceRangePastTheEndIsACallerBugAndDoesNotDisableSharing() throws IOException
    {
        File src = write("reflink-past-end-src", random(SOURCE_LENGTH));
        File dst = FileUtils.createTempFile("reflink-past-end-dst", "1");
        dst.deleteOnExit();

        try (FileChannel in = src.newReadChannel();
             FileChannel out = dst.newWriteChannel(File.WriteMode.OVERWRITE))
        {
            File dir = dst.parent();
            Reflink.resetSupportCache();
            // Whether sharing is possible at all is platform dependent; what matters is that a caller bug does not
            // CHANGE the answer, which is what caching an EINVAL used to do.
            boolean possibleBefore = Reflink.isPossibleIn(dir);

            long pastEnd = (SOURCE_LENGTH / A) * A;   // aligned, but there are fewer than A bytes left
            assertThatThrownBy(() -> Reflink.tryCloneRange(in, pastEnd, out, 0, A, dir))
            .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("past the source");

            assertEquals("nothing may have been written", 0, dst.length());
            assertEquals("a caller bug must not be remembered as a filesystem limitation",
                         possibleBefore, Reflink.isPossibleIn(dir));
        }
    }

    /**
     * The request number and the struct it advertises, which are the parts of the ABI nothing else checks.
     *
     * <p>The arithmetic half runs everywhere: {@code FICLONERANGE} is re-derived from the kernel's own macros, so
     * an edit to either literal in {@link Reflink} has to be an edit here too.
     *
     * <p>The half that needs a kernel distinguishes a well formed request from an unrecognised one, which is
     * otherwise indistinguishable: {@code FICLONERANGE} is handled in {@code do_vfs_ioctl} before the filesystem
     * is consulted, so even ext4 answers {@code EOPNOTSUPP} (it has no {@code remap_file_range}) rather than
     * {@code ENOTTY} (no ioctl case matched). Both return false and both poison the filesystem, so only the
     * remembered errno shows the difference -- and a mistyped request would look exactly like ext4 while silently
     * turning every clone on every reflink-capable node into a full copy.
     *
     * <p>The same errno is the only reachable check on {@code src_fd} being the struct's FIRST field:
     * {@code ioctl_file_clone} resolves {@code src_fd} through {@code fdget} and answers {@code EBADF} before it
     * calls {@code vfs_clone_file_range}, so a struct whose first eight bytes held {@code src_offset} instead
     * would fail with {@code EBADF} -- which is not a filesystem limitation, is therefore NOT remembered, and so
     * shows up below as a null where {@code EOPNOTSUPP} is required. The other three fields are not reachable at
     * all without a filesystem that shares extents; see the class javadoc.
     */
    @Test
    public void ficloneRangeIsTheRequestNumberTheKernelRecognises() throws IOException
    {
        // asm-generic/ioctl.h: _IOC(dir,type,nr,size) == (dir<<30)|(size<<16)|(type<<8)|nr, _IOC_WRITE == 1
        // linux/fs.h:          FICLONERANGE == _IOW(0x94, 13, struct file_clone_range)
        long ioc = (1L << 30) | (32L << 16) | (0x94L << 8) | 13L;
        assertEquals("the asm-generic _IOW encoding of FICLONERANGE", 0x4020940DL, ioc);
        assertEquals("Reflink must send exactly that request", ioc, constant("FICLONERANGE"));

        // struct file_clone_range { __s64 src_fd; __u64 src_offset; __u64 src_length; __u64 dest_offset; }
        long structSize = constant("FILE_CLONE_RANGE_SIZE");
        assertEquals("four naturally aligned 8-byte fields, no padding on any ABI", 4L * Long.BYTES, structSize);
        assertEquals("the size the request advertises has to be the size of the struct actually sent; if the two"
                     + " drift the kernel matches no case and answers ENOTTY",
                     structSize, (constant("FICLONERANGE") >> 16) & 0x3FFF);

        byte[] source = random(SOURCE_LENGTH);
        File src = write("reflink-abi-src", source);
        File dst = FileUtils.createTempFile("reflink-abi-dst", "1");
        dst.deleteOnExit();
        File dir = dst.parent();

        Reflink.resetSupportCache();
        assumeTrue("ioctl(2) is not linked here, so no request reaches a kernel", Reflink.isPossibleIn(dir));

        boolean shared;
        try (FileChannel in = src.newReadChannel();
             FileChannel out = dst.newWriteChannel(File.WriteMode.OVERWRITE))
        {
            shared = Reflink.tryCloneRange(in, A, out, 3 * A, 2 * A, dir);
        }

        Integer errno = Reflink.unsupportedErrno(dir);

        if (shared)
        {
            assertNull("a success must not be remembered as a failure", errno);
            assertEquals("a clone grows the destination to dstOffset + length", 5 * A, dst.length());
            assertArrayEquals("the shared range must hold the source's bytes",
                              Arrays.copyOfRange(source, (int) A, (int) (3 * A)),
                              Arrays.copyOfRange(readAll(dst), (int) (3 * A), (int) (5 * A)));
            return;
        }

        assertEquals("a refused clone must leave the destination at the length it had on entry", 0, dst.length());

        // Only the three errnos isFilesystemLimitation accepts are remembered, and exactly one of them is a
        // legitimate answer to this call, so this is an equality rather than a set membership:
        //  - ENOTTY  would mean no ioctl case matched, i.e. FICLONERANGE is not the request being sent. Sharing
        //            could never work anywhere and would look exactly like ext4.
        //  - ENOSYS  is only ever written by the UnsatisfiedLinkError path, which cannot be reached past the
        //            isPossibleIn assumption above.
        //  - null    means the ioctl failed with something that is NOT a filesystem limitation. Nothing else here
        //            can produce one: the arguments are aligned and inside the source (no EINVAL), source and
        //            destination are both in java.io.tmpdir and so on one filesystem (no EXDEV), neither file is
        //            immutable or append-only (no EPERM), and getfd answering -1 would have taken the JVM-wide
        //            path instead of reaching the kernel (see aChannelWithNoReachableDescriptorIsAJvmWideAnswer).
        //            What is left is EBADF, i.e. src_fd is not the struct's first field.
        assertNotNull("the ioctl failed with an errno that is not a filesystem limitation, and the only one this"
                      + " call can produce is EBADF -- src_fd is not the first field of struct file_clone_range,"
                      + " so the kernel resolved an offset as a descriptor",
                      errno);
        assertEquals("expected EOPNOTSUPP (" + EOPNOTSUPP + ") from a filesystem with no refcount btree; got "
                     + errno + ", and ENOTTY (" + ENOTTY + ") in particular means the request number is wrong",
                     Integer.valueOf(EOPNOTSUPP), errno);
    }

    /**
     * A well formed request that fails leaves the destination exactly the length it was, because that is what the
     * caller's fallback copy assumes when it decides where to start writing. The refusal is FORCED here rather than
     * hoped for, so this runs and means the same thing on every filesystem -- and it covers the whole of the
     * "already known not to work" path, which is every clone after the first on a node that cannot share extents.
     * {@link #undoPartialShareTruncatesBackAndNothingElse} covers the path that returns false after the ioctl.
     */
    @Test
    public void aRefusalLeavesTheDestinationAtTheLengthItHadOnEntry() throws IOException
    {
        byte[] existing = random((int) (2 * A));
        File src = write("reflink-entry-length-src", random(SOURCE_LENGTH));
        File dst = write("reflink-entry-length-dst", existing);
        File dir = dst.parent();

        // Remembering EOPNOTSUPP is what a filesystem with no refcount btree does on its first attempt, so this
        // is the state every later clone on such a node runs in.
        noteUnsupported(cacheKey(dir), dir, EOPNOTSUPP, "EOPNOTSUPP");
        assertFalse(Reflink.isPossibleIn(dir));

        try (FileChannel in = src.newReadChannel();
             FileChannel out = dst.newReadWriteChannel())
        {
            assertFalse("a filesystem already known not to share extents must refuse",
                        Reflink.tryCloneRange(in, 0, out, 2 * A, A, dir));
        }

        assertEquals("a refusal must not have truncated, extended or zero-filled the destination",
                     2 * A, dst.length());
        assertArrayEquals("...nor changed a byte of it", existing, readAll(dst));
    }

    /**
     * {@code undoPartialShare} is what makes a false return mean "nothing was written" even though the ioctl is
     * all-or-nothing only on the way IN: {@code ioctl_file_clone} turns a remap it clamped short into
     * {@code EINVAL} after the extents it did share are already in the destination. Reaching that needs a
     * filesystem that shares extents plus a clamp ({@code RLIMIT_FSIZE}, {@code s_maxbytes}, a source that shrank
     * under the call), so it is driven directly here -- and it is the one place in this class where getting it
     * wrong loses data rather than performance: a caller that trusted "nothing was written" and copied only the
     * tail would publish a file whose head is the source's PREVIOUS contents.
     */
    @Test
    public void undoPartialShareTruncatesBackAndNothingElse() throws IOException
    {
        byte[] content = random((int) (3 * A));
        File dst = write("reflink-undo-dst", content);
        File dir = dst.parent();

        // A share that got as far as 3A into a destination that was A long: the 2A it did write must go.
        try (FileChannel out = dst.newReadWriteChannel())
        {
            undoPartialShare(out, A, dir);
        }
        assertEquals("the partially shared bytes must be discarded", A, dst.length());
        assertArrayEquals("and the bytes that were there on entry must survive",
                          Arrays.copyOfRange(content, 0, (int) A), readAll(dst));

        // The ordinary case: the ioctl failed before writing anything, so there is nothing to undo and in
        // particular the destination must not be truncated to the length of a clone that never happened.
        try (FileChannel out = dst.newReadWriteChannel())
        {
            undoPartialShare(out, A, dir);
        }
        assertEquals(A, dst.length());

        // And a destination SHORTER than its recorded entry length -- which nothing can produce, but which a
        // truncate() would answer by extending the file with a hole -- must be left alone.
        try (FileChannel out = dst.newReadWriteChannel())
        {
            undoPartialShare(out, 3 * A, dir);
        }
        assertEquals("undoing must never make the destination longer", A, dst.length());
    }

    /**
     * A truncation that cannot be done must throw rather than answer false, because false is a promise the caller
     * acts on: it copies the range over a destination it believes to be untouched, and a head of stale source
     * bytes under a correctly copied tail is silent corruption that no digest catches (the digest is computed
     * from what was written).
     */
    @Test
    public void undoPartialShareThrowsRatherThanLieAboutTheDestination() throws IOException
    {
        File dst = write("reflink-undo-fail-dst", random((int) (3 * A)));
        FileChannel out = dst.newReadWriteChannel();
        out.close();

        assertThatThrownBy(() -> undoPartialShare(out, A, dst.parent()))
        .isInstanceOf(FSWriteError.class)
        .hasMessageContaining("undo a partially shared range")
        .hasCauseInstanceOf(IOException.class);
        assertEquals("and nothing may have been truncated", 3 * A, dst.length());
    }

    /**
     * The negative cache is keyed by the destination's FILESYSTEM, not by its directory, because every errno it
     * remembers is a property of that filesystem alone. Two {@code data_file_directories} on one mount therefore
     * share one answer: under the old per-directory keying each of them paid for its own failed ioctl and logged
     * its own INFO, on every mount, for the life of the process.
     */
    @Test
    public void oneAnswerIsRememberedPerFilesystemNotPerDirectory() throws IOException
    {
        File a = scratchDirectory("reflink-fs-a");
        File b = scratchDirectory("reflink-fs-b");
        assertNotEquals("the two scratch directories must really be different", a.path(), b.path());

        String key = cacheKey(a);
        assertEquals("two directories on one mount must resolve to one key", key, cacheKey(b));
        assertNotEquals("the key must not be the directory, or one mount cannot answer for another", a.path(), key);
        assertNotEquals(b.path(), key);

        noteUnsupported(key, a, EOPNOTSUPP, "EOPNOTSUPP");

        assertEquals(Integer.valueOf(EOPNOTSUPP), Reflink.unsupportedErrno(a));
        assertEquals("the sibling directory must inherit the answer rather than pay for its own failed ioctl",
                     Integer.valueOf(EOPNOTSUPP), Reflink.unsupportedErrno(b));
        assertFalse(Reflink.isPossibleIn(a));
        assertFalse(Reflink.isPossibleIn(b));

        Reflink.resetSupportCache();
        assertNull(Reflink.unsupportedErrno(a));
        assertNull(Reflink.unsupportedErrno(b));
    }

    /**
     * Which errnos are remembered, and which are answered again on every call. Remembering lasts the life of the
     * process, so remembering one too many switches extent sharing off for a whole filesystem that supports it --
     * and there is no observable difference between that and a filesystem that really cannot do it, which is why
     * this is asserted against the classifier directly rather than through an outcome.
     */
    @Test
    public void errnosAreClassifiedByWhatTheyRuleOut() throws IOException
    {
        // The numbers first: these are Linux asm-generic values and Reflink relies on them being that, which is
        // why LINKED gates on a real Linux test rather than on NativeLibrary.osType (which answers LINUX for
        // every OS it does not recognise, and EOPNOTSUPP is 45 on BSD).
        assertEquals(EPERM, constant("EPERM"));
        assertEquals(EBADF, constant("EBADF"));
        assertEquals(EXDEV, constant("EXDEV"));
        assertEquals(EINVAL, constant("EINVAL"));
        assertEquals(ENOTTY, constant("ENOTTY"));
        assertEquals(ENOSYS, constant("ENOSYS"));
        assertEquals(EOPNOTSUPP, constant("EOPNOTSUPP"));

        assertTrue("EOPNOTSUPP: the filesystem has no refcount btree", isFilesystemLimitation(EOPNOTSUPP));
        assertTrue("ENOTTY: the kernel does not implement FICLONERANGE", isFilesystemLimitation(ENOTTY));
        assertTrue("ENOSYS: ioctl(2) is not linked", isFilesystemLimitation(ENOSYS));

        assertFalse("EXDEV is a property of the source/destination PAIR, not of the destination's filesystem:"
                    + " remembering it would switch sharing off for a mount that shares extents perfectly well,"
                    + " because one source once was not co-located with it",
                    isFilesystemLimitation(EXDEV));
        assertFalse("EINVAL is the kernel's answer to every bad argument, not just to a block size larger than"
                    + " RANGE_ALIGNMENT", isFilesystemLimitation(EINVAL));
        assertFalse("EPERM is per file -- an immutable or append-only destination", isFilesystemLimitation(EPERM));
        assertFalse("EBADF is a property of the JVM, and has its own process-wide answer",
                    isFilesystemLimitation(EBADF));

        // And the ones that are not remembered must not be reported as the filesystem's fault either: EXDEV in
        // particular names both files, so an operator sent to check the destination's mkfs flags learns nothing.
        assertThat(strerror(EXDEV)).contains("different filesystems");
    }

    /**
     * {@code NativeLibrary.getfd} answering -1 means {@code sun.nio.ch.FileChannelImpl.fd} could not be read
     * reflectively -- a JVM started without {@code --add-opens java.base/sun.nio.ch=ALL-UNNAMED}, or a channel
     * that is not from the default filesystem provider. That is a property of the JVM, so it switches sharing off
     * for the whole PROCESS and says which, instead of being written into the per-filesystem cache as a fake
     * EBADF: that named each data directory in turn and blamed its filesystem, which is the wrong thing to send
     * an operator to check while sharing is silently off cluster-wide.
     */
    @Test
    public void anUnreachableDescriptorIsAJvmWideAnswerNotAFilesystemOne() throws IOException
    {
        File a = scratchDirectory("reflink-nofd-a");
        File b = scratchDirectory("reflink-nofd-b");
        boolean possibleBefore = Reflink.isPossibleIn(a);

        noteDescriptorsUnreachable();

        assertNull("an unreachable descriptor must not be remembered as a filesystem limitation",
                   Reflink.unsupportedErrno(a));
        assertNull(Reflink.unsupportedErrno(b));
        assertFalse("but it must still switch sharing off", Reflink.isPossibleIn(a));
        assertFalse("...for every filesystem, not just the one that happened to be attempted first",
                    Reflink.isPossibleIn(b));

        Reflink.resetSupportCache();
        assertEquals("resetSupportCache must clear the process-wide answer too, or one test poisons every later"
                     + " one in the same JVM", possibleBefore, Reflink.isPossibleIn(a));
    }

    /**
     * ...and that answer is really reached from a channel whose descriptor cannot be read, rather than only from
     * the method that records it. A {@link FileChannel} that is not a {@code sun.nio.ch.FileChannelImpl} makes
     * {@code getfd} take exactly the reflection failure branch a missing {@code --add-opens} takes.
     */
    @Test
    public void aChannelWithNoReachableDescriptorIsAJvmWideAnswer() throws IOException
    {
        File src = write("reflink-opaque-src", random(SOURCE_LENGTH));
        File dst = FileUtils.createTempFile("reflink-opaque-dst", "1");
        dst.deleteOnExit();
        File dir = dst.parent();
        assumeTrue("ioctl(2) is not linked here, so no descriptor is ever looked up", Reflink.isPossibleIn(dir));

        try (FileChannel real = src.newReadChannel();
             FileChannel opaque = new OpaqueFileChannel(real);
             FileChannel out = dst.newWriteChannel(File.WriteMode.OVERWRITE))
        {
            assertEquals("the point of this channel is that getfd cannot read it", -1,
                         NativeLibrary.getfd(opaque));
            assertFalse(Reflink.tryCloneRange(opaque, 0, out, 0, 2 * A, dir));
        }

        assertEquals("nothing may have been written", 0, dst.length());
        assertNull("a JVM without --add-opens is not the filesystem's fault", Reflink.unsupportedErrno(dir));
        assertFalse("but sharing is off all the same", Reflink.isPossibleIn(dir));
    }

    /**
     * The alignment is what every caller has to arrange before there is a destination to ask for its block size,
     * so it has to hold for every filesystem that can share extents at all: a multiple of every block size xfs or
     * btrfs can be formatted with, both of which top out at 64 KiB. Anything it is not a multiple of would fail
     * with {@code EINVAL} -- which is deliberately not remembered, so every range would cost a failed ioctl and a
     * WARN before being copied anyway.
     *
     * <p>It also has to be a power of two, because {@code requireAligned} and every caller laying out a
     * destination test it with a mask rather than a modulo.
     *
     * <p>This is not the page size: a block is no longer bounded by a page (Linux 6.12 mounts xfs with a block
     * larger than one), and some builds run with pages larger than 64 KiB, so neither bound would justify this
     * constant.
     */
    @Test
    public void alignmentIsAMultipleOfEveryReflinkCapableBlockSize()
    {
        assertEquals("must be a power of two: requireAligned masks rather than divides",
                     0, Reflink.RANGE_ALIGNMENT & (Reflink.RANGE_ALIGNMENT - 1));

        for (long blockSize = 512; blockSize <= 64 << 10; blockSize <<= 1)
            assertEquals("a filesystem with a " + blockSize + " byte block would answer EINVAL to every range"
                         + " aligned only to " + Reflink.RANGE_ALIGNMENT,
                         0, Reflink.RANGE_ALIGNMENT % blockSize);

        assertTrue("64 KiB is the largest block size xfs or btrfs can be formatted with, so nothing below it can"
                   + " cover every one of them", Reflink.RANGE_ALIGNMENT >= 64 << 10);
    }

    /** Reads a private compile-time constant out of {@link Reflink} so a change to it fails this test. */
    private static long constant(String name) throws IOException
    {
        try
        {
            java.lang.reflect.Field f = Reflink.class.getDeclaredField(name);
            f.setAccessible(true);
            return f.getLong(null);
        }
        catch (ReflectiveOperationException e)
        {
            throw new IOException("Reflink." + name + " is gone; the ABI check above no longer checks anything", e);
        }
    }

    /**
     * Calls one of {@link Reflink}'s private statics. The cache keying, the process-wide "no reachable descriptor"
     * answer and the truncation that undoes a partial share are each reachable in production only through a real
     * {@code FICLONERANGE} failure, and two of the three only on a filesystem that shares extents, so they are
     * driven directly rather than left untested. A rename fails this loudly instead of quietly stopping the tests
     * that use it from testing anything.
     */
    private static Object invoke(String name, Class<?>[] signature, Object... args)
    {
        try
        {
            Method method = Reflink.class.getDeclaredMethod(name, signature);
            method.setAccessible(true);
            return method.invoke(null, args);
        }
        catch (InvocationTargetException e)
        {
            // Unchanged, so a caller can assert on what Reflink itself threw
            Throwables.throwIfUnchecked(e.getCause());
            throw new AssertionError(e.getCause());
        }
        catch (ReflectiveOperationException e)
        {
            throw new AssertionError("Reflink." + name + " is gone; whatever called it no longer tests anything", e);
        }
    }

    private static String cacheKey(File directory)
    {
        return (String) invoke("cacheKey", new Class<?>[]{ File.class }, directory);
    }

    private static void noteUnsupported(String key, File directory, int errno, String reason)
    {
        invoke("noteUnsupported", new Class<?>[]{ String.class, File.class, int.class, String.class },
               key, directory, errno, reason);
    }

    private static void noteDescriptorsUnreachable()
    {
        invoke("noteDescriptorsUnreachable", new Class<?>[0]);
    }

    private static void undoPartialShare(FileChannel dst, long lengthOnEntry, File directory)
    {
        invoke("undoPartialShare", new Class<?>[]{ FileChannel.class, long.class, File.class },
               dst, lengthOnEntry, directory);
    }

    private static boolean isFilesystemLimitation(int errno)
    {
        return (Boolean) invoke("isFilesystemLimitation", new Class<?>[]{ int.class }, errno);
    }

    private static String strerror(int errno)
    {
        return (String) invoke("strerror", new Class<?>[]{ int.class }, errno);
    }

    /** A directory of its own, on whatever filesystem java.io.tmpdir is on. */
    private static File scratchDirectory(String name) throws IOException
    {
        File directory = new File(java.nio.file.Files.createTempDirectory(name));
        directory.deleteRecursiveOnExit();
        return directory;
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

    /**
     * A {@link FileChannel} that behaves exactly like the one it wraps but is not a
     * {@code sun.nio.ch.FileChannelImpl}, which is the whole point: {@code NativeLibrary.getfd} reads the {@code fd}
     * field off that class reflectively and answers -1 for anything else -- the same -1 it answers for every channel
     * in a JVM started without {@code --add-opens java.base/sun.nio.ch=ALL-UNNAMED}, and the same -1 a channel from
     * a non-default {@code FileSystemProvider} produces.
     */
    private static final class OpaqueFileChannel extends FileChannel
    {
        private final FileChannel delegate;

        private OpaqueFileChannel(FileChannel delegate)
        {
            this.delegate = delegate;
        }

        public int read(ByteBuffer dst) throws IOException
        {
            return delegate.read(dst);
        }

        public long read(ByteBuffer[] dsts, int offset, int length) throws IOException
        {
            return delegate.read(dsts, offset, length);
        }

        public int write(ByteBuffer src) throws IOException
        {
            return delegate.write(src);
        }

        public long write(ByteBuffer[] srcs, int offset, int length) throws IOException
        {
            return delegate.write(srcs, offset, length);
        }

        public long position() throws IOException
        {
            return delegate.position();
        }

        public FileChannel position(long newPosition) throws IOException
        {
            delegate.position(newPosition);
            return this;
        }

        public long size() throws IOException
        {
            return delegate.size();
        }

        public FileChannel truncate(long size) throws IOException
        {
            delegate.truncate(size);
            return this;
        }

        public void force(boolean metaData) throws IOException
        {
            delegate.force(metaData);
        }

        public long transferTo(long position, long count, WritableByteChannel target) throws IOException
        {
            return delegate.transferTo(position, count, target);
        }

        public long transferFrom(ReadableByteChannel src, long position, long count) throws IOException
        {
            return delegate.transferFrom(src, position, count);
        }

        public int read(ByteBuffer dst, long position) throws IOException
        {
            return delegate.read(dst, position);
        }

        public int write(ByteBuffer src, long position) throws IOException
        {
            return delegate.write(src, position);
        }

        public MappedByteBuffer map(MapMode mode, long position, long size) throws IOException
        {
            return delegate.map(mode, position, size);
        }

        public FileLock lock(long position, long size, boolean shared) throws IOException
        {
            return delegate.lock(position, size, shared);
        }

        public FileLock tryLock(long position, long size, boolean shared) throws IOException
        {
            return delegate.tryLock(position, size, shared);
        }

        protected void implCloseChannel() throws IOException
        {
            delegate.close();
        }
    }
}
