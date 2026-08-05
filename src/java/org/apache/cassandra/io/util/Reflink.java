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
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.annotations.VisibleForTesting;
import com.sun.jna.LastErrorException;
import com.sun.jna.Memory;
import com.sun.jna.Native;
import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.NativeLibrary;

/**
 * Byte-range extent sharing between two files on the same filesystem -- a "reflink" -- via the Linux
 * {@code FICLONERANGE} ioctl.
 * <p>
 * The ioctl points a range of the destination at the <em>same physical extents</em> as a range of the source and
 * bumps their reference count: nothing is read or written, no page cache is filled, and the cost tracks extents, not
 * bytes. The files stay independent (a shared block is copied on the first write to it, freed at the last reference),
 * so cloning out of an sstable about to be unlinked hands it the extents instead of copying them and then freeing the
 * originals. {@code FileChannel.transferTo} is no substitute: it is {@code sendfile} on JDK 11 and
 * {@code copy_file_range} on JDK 21+, which reflinks only when source offset, destination offset and length are all
 * already block aligned and silently copies otherwise without saying which it did, whereas this ioctl either shares
 * the extents or reports why it could not -- what a caller that pads its destination to make sharing possible needs.
 * <p>
 * Alignment: {@code srcOffset}, {@code dstOffset} and {@code length} must all be multiples of the filesystem block
 * size (a partial last block only if the source range ends at the source's EOF, not worth relying on); unaligned
 * arguments fail with {@code EINVAL} rather than being rounded, and the aligned interior is not cloned for you.
 * {@link #RANGE_ALIGNMENT} is a constant 64 KiB rather than the destination's own block size because a caller has to
 * lay its destination out before there is a destination to ask, and because 64 KiB is a multiple of every block size
 * xfs or btrfs can be formatted with -- 64 KiB is the maximum for both -- for at most 64 KiB of slack per cloned
 * range. It is NOT the page size in disguise: a block is no longer bounded by a page (Linux 6.12 mounts xfs with a
 * block larger than one) and 64 KiB is not the largest page size Linux runs with either (some builds use 256 KiB
 * pages). A filesystem that wanted coarser alignment than this would answer {@code EINVAL}, which is not remembered,
 * so every attempt would cost one failed ioctl and a WARN before the range was copied anyway.
 * <p>
 * Availability: sharing needs a refcount btree -- xfs formatted with {@code -m reflink=1} (the mkfs default since
 * xfsprogs 5.1), btrfs, bcachefs or OCFS2 -- and one mount, else {@code EOPNOTSUPP}/{@code ENOTTY}/{@code EXDEV}.
 * {@code statfs} type names the filesystem but not whether reflink was enabled at mkfs time, so support is discovered
 * by trying and remembered per FILESYSTEM in {@link #unsupported} -- see {@link #isFilesystemLimitation}, which is
 * also why {@code EXDEV} is not one of the answers remembered there.
 * <p>
 * Shared extents are cheap on disk but not in RAM: the page cache is per inode, so bytes read through both files are
 * cached twice. That only costs when both stay live and hot, not when the clone replaces its source.
 */
public final class Reflink
{
    private static final Logger logger = LoggerFactory.getLogger(Reflink.class);

    /**
     * {@code FICLONERANGE == _IOW(0x94, 13, struct file_clone_range)}, i.e.
     * {@code (1 << 30) | (32 << 16) | (0x94 << 8) | 13}. The 32 is {@code sizeof(struct file_clone_range)}.
     * <p>
     * This is the {@code asm-generic} encoding, i.e. every architecture Cassandra is built and tested on. PowerPC,
     * MIPS, SPARC and Alpha override {@code <asm/ioctl.h>} ({@code _IOC_DIRSHIFT == 29}, {@code _IOC_WRITE == 4}) and
     * would need {@code 0x8020940D}. Nothing gates that, and {@code Architecture} in particular does not: it has no
     * notion of an unsupported platform, its one set ({@code UNALIGNED_ARCH}) explicitly INCLUDES {@code ppc64le} and
     * {@code s390x}, and MIPS, SPARC and Alpha appear in it nowhere. It is safe there all the same, if by consequence
     * rather than by design: this request decodes to nothing those kernels implement, so it answers {@code ENOTTY},
     * which is indistinguishable from a filesystem without the feature and is remembered as one, and every range is
     * copied.
     */
    private static final long FICLONERANGE = 0x4020940DL;

    private static final int FILE_CLONE_RANGE_SIZE = 32;

    /** Every offset and length handed to {@link #tryCloneRange} must be a multiple of this; see the class javadoc. */
    public static final long RANGE_ALIGNMENT = 64 << 10;

    // errno values that mean "this filesystem/kernel/mount pairing can never share extents", as opposed to "these
    // particular arguments were wrong". Linux asm-generic values, identical on every architecture Cassandra runs on
    // but NOT on other kernels -- EOPNOTSUPP is 45 on BSD -- which is why LINKED gates on a real Linux test.
    private static final int EPERM = 1;
    private static final int EBADF = 9;
    private static final int EXDEV = 18;
    private static final int EINVAL = 22;
    private static final int ENOTTY = 25;
    private static final int ENOSYS = 38;
    private static final int EOPNOTSUPP = 95;

    // Filesystems that have already answered "no", keyed by the destination's FileStore rather than by its directory:
    // every errno remembered here (see isFilesystemLimitation) is a property of the destination's filesystem alone, so
    // two data directories on one mount share an answer while an ext4 one still cannot disable an xfs one. Bounded by
    // the number of mounts. See cacheKey.
    private static final Map<String, Integer> unsupported = new ConcurrentHashMap<>();

    // Whether FileChannel's descriptor has been found unreachable, which is a property of the JVM and not of any
    // filesystem, so it is remembered for the process instead. See noteDescriptorsUnreachable.
    private static final AtomicBoolean descriptorsUnreachable = new AtomicBoolean();

    private static final boolean LINKED;

    static
    {
        boolean linked = false;
        try
        {
            // Registered here rather than in NativeLibraryLinux: Native.register links every native method of the
            // class at once, so a failure would take mlockall/fadvise/fcntl with it. ioctl(2) is in every libc,
            // unlike copy_file_range (glibc 2.27+), so this is belt and braces.
            //
            // FBUtilities.isLinux and not NativeLibrary.osType: getOsType() answers LINUX for every OS it does not
            // recognise (only mac and aix are picked out), so it would let this run on FreeBSD or illumos, where
            // neither the request number above nor the errno numbers below are right -- EOPNOTSUPP is 45 there, so
            // isFilesystemLimitation would match nothing and the node would log a WARN per child per split forever
            // instead of one INFO.
            if (FBUtilities.isLinux)
            {
                Native.register(com.sun.jna.NativeLibrary.getInstance("c", Collections.emptyMap()));
                linked = true;
            }
        }
        catch (Throwable t)
        {
            logger.debug("Could not link ioctl(2); byte-range extent sharing is unavailable", t);
        }
        LINKED = linked;
    }

    private Reflink()
    {
    }

    private static native int ioctl(int fd, NativeLong request, Pointer argp) throws LastErrorException;

    /**
     * Whether {@link #tryCloneRange} is worth attempting in {@code directory}, so that callers pay to lay their
     * destination out at {@link #RANGE_ALIGNMENT} only when it can buy something. Optimistic: an untried filesystem
     * answers true, so the first caller pays for one failed ioctl.
     */
    public static boolean isPossibleIn(File directory)
    {
        return isPossibleFor(cacheKey(directory));
    }

    private static boolean isPossibleFor(String key)
    {
        return LINKED && !descriptorsUnreachable.get() && !unsupported.containsKey(key);
    }

    /**
     * Share {@code length} bytes of {@code src} at {@code srcOffset} into {@code dst} at {@code dstOffset}, moving no
     * data. All three offsets and lengths must be multiples of {@link #RANGE_ALIGNMENT} and {@code srcOffset + length}
     * must not exceed the source's length. The destination grows to at least {@code dstOffset + length} but its file
     * position is NOT moved, so a caller that writes the tail conventionally must position the channel itself. Neither
     * channel is closed, flushed or synced: a clone is a metadata change like any other write and needs {@code force()}
     * to survive a crash.
     *
     * @param directory the directory the destination lives in, used only to find the filesystem the negative cache is
     *        keyed by
     * @return true if the whole range is now shared; false if it could not be, with the destination truncated back to
     *         the length it had on entry -- so for a clone that only EXTENDS its destination
     *         ({@code dstOffset >= dst.size()}, which is every caller there is) a false return really has written
     *         nothing at all, and the caller must copy the bytes itself. A clone into the MIDDLE of an existing file
     *         cannot be given that guarantee; {@link #undoPartialShare} has why.
     * @throws IllegalArgumentException if the arguments are unaligned or the source range runs past the source's end
     *         -- a caller bug rather than a filesystem limitation, so it must not be answered by copying
     * @throws FSWriteError if the destination could not be truncated back, since returning false would then be a lie
     */
    public static boolean tryCloneRange(FileChannel src, long srcOffset,
                                        FileChannel dst, long dstOffset,
                                        long length, File directory)
    {
        if (length <= 0)
            throw new IllegalArgumentException("length must be positive, got " + length);
        requireAligned("srcOffset", srcOffset);
        requireAligned("dstOffset", dstOffset);
        requireAligned("length", length);

        // Checked here, not left to the kernel's ambiguous EINVAL; before the support check so caller bugs always
        // surface.
        try
        {
            long srcLength = src.size();
            if (srcOffset + length > srcLength)
                throw new IllegalArgumentException("source range [" + srcOffset + ", " + (srcOffset + length) +
                                                   ") runs past the source's length of " + srcLength);
        }
        catch (IOException e)
        {
            logger.warn("Could not size the clone source; copying instead", e);
            return false;
        }

        String key = cacheKey(directory);
        if (!isPossibleFor(key))
            return false;

        int srcFd = NativeLibrary.getfd(src);
        int dstFd = NativeLibrary.getfd(dst);
        if (srcFd < 0 || dstFd < 0)
        {
            noteDescriptorsUnreachable();
            return false;
        }

        // Read as late as possible, so that the length the failure path restores is the one the ioctl itself saw.
        long dstLengthOnEntry;
        try
        {
            dstLengthOnEntry = dst.size();
        }
        catch (IOException e)
        {
            logger.warn("Could not size the clone destination; copying instead", e);
            return false;
        }

        // struct file_clone_range by hand rather than via a JNA Structure: no padding on any ABI, native byte order.
        try (Memory arg = new Memory(FILE_CLONE_RANGE_SIZE))
        {
            arg.setLong(0, srcFd);        // __s64 src_fd
            arg.setLong(8, srcOffset);    // __u64 src_offset
            arg.setLong(16, length);      // __u64 src_length
            arg.setLong(24, dstOffset);   // __u64 dest_offset

            // All-or-nothing on the way IN, so no short-clone loop is needed: 0 means every byte was shared. NOT on
            // the way out -- a failure can still have shared part of the range, which undoPartialShare undoes so that
            // false keeps meaning "nothing was written". rc is checked as well as the exception because JNA raises
            // LastErrorException off errno, not off rc.
            int rc = ioctl(dstFd, new NativeLong(FICLONERANGE), arg);
            if (rc != 0)
            {
                logger.warn("FICLONERANGE of {} bytes at {} returned {} without setting errno; copying instead",
                            length, srcOffset, rc);
                undoPartialShare(dst, dstLengthOnEntry, directory);
                return false;
            }
            logger.trace("Shared {} bytes at {} into {} at {}", length, srcOffset, directory, dstOffset);
            return true;
        }
        catch (LastErrorException e)
        {
            // The errno is reported before the destination is restored, so that it is on record even if restoring it
            // fails and throws.
            int errno = e.getErrorCode();
            if (isFilesystemLimitation(errno))
                noteUnsupported(key, directory, errno, strerror(errno));
            else
                logger.warn("FICLONERANGE of {} bytes at {} failed with errno {} ({}); copying instead",
                            length, srcOffset, errno, strerror(errno));
            undoPartialShare(dst, dstLengthOnEntry, directory);
            return false;
        }
        catch (UnsatisfiedLinkError e)
        {
            noteUnsupported(key, directory, ENOSYS, "ioctl(2) is not linked");
            return false;
        }
    }

    /**
     * Truncate {@code dst} back to the length it had before a refused clone, so that a {@code false} from
     * {@link #tryCloneRange} means the destination was not written.
     * <p>
     * The ioctl is all-or-nothing on the way in but not on the way out. {@code ioctl_file_clone} turns a SHORT remap
     * into {@code EINVAL} <em>after</em> the extents it did share are already in the destination
     * ({@code cloned = vfs_clone_file_range(...); else if (olen && cloned != olen) ret = -EINVAL;}), and the length it
     * remaps is silently clamped on the way there: to {@code RLIMIT_FSIZE} and {@code s_maxbytes} by
     * {@code generic_remap_checks}/{@code generic_write_check_limits}, and to the source's {@code i_size} -- of which
     * the {@code src.size()} check in {@link #tryCloneRange} is only a snapshot -- by
     * {@code generic_remap_file_range_prep}. So a clone of a large run under a systemd unit with {@code LimitFSIZE=}
     * fails with the head of the range already remapped, and a caller that trusted "nothing was written" and copied
     * only the tail would publish a file whose head is stale data from the source's previous contents.
     * <p>
     * Truncation undoes that for a clone that only EXTENDS the destination, which is the only shape it can undo: one
     * into the middle of an existing file has already replaced those bytes and no length restores them. That is
     * documented on {@link #tryCloneRange} rather than rejected, there being no such caller.
     *
     * @throws FSWriteError if the truncation itself fails, because returning false would then be a lie
     */
    private static void undoPartialShare(FileChannel dst, long lengthOnEntry, File directory)
    {
        try
        {
            long length = dst.size();
            if (length <= lengthOnEntry)
                return;

            logger.warn("A refused FICLONERANGE had already shared {} bytes into a destination in {}; discarding them"
                        + " so the range can be copied from scratch", length - lengthOnEntry, directory);
            dst.truncate(lengthOnEntry);
        }
        catch (IOException e)
        {
            throw new FSWriteError("could not undo a partially shared range", e, directory);
        }
    }

    /** Forget every remembered negative answer. For tests, which remount filesystems under one path; nodes do not. */
    public static void resetSupportCache()
    {
        unsupported.clear();
        descriptorsUnreachable.set(false);
    }

    /**
     * The errno remembered for {@code directory}'s filesystem, or null; {@link #tryCloneRange} returns false whichever
     * it was.
     */
    @VisibleForTesting
    static Integer unsupportedErrno(File directory)
    {
        return unsupported.get(cacheKey(directory));
    }

    /**
     * The key {@link #unsupported} is under for {@code directory}: the FILESYSTEM it sits on, since that, and not the
     * directory, is what can or cannot share extents. Falls back to the path when the filesystem cannot be identified,
     * which only loses the sharing of one answer between two directories on one mount; either key leaves the cache
     * bounded by the handful of data directories a node has.
     */
    private static String cacheKey(File directory)
    {
        try
        {
            return Files.getFileStore(directory.toPath()).toString();
        }
        catch (IOException | RuntimeException e)
        {
            logger.debug("Could not identify the filesystem of {}; remembering extent sharing per directory instead",
                         directory, e);
            return directory.path();
        }
    }

    /**
     * The errnos that mean the DESTINATION's filesystem itself cannot do this, so no future call for any directory on
     * it can succeed either -- and therefore the only ones safe to remember, since remembering lasts the life of the
     * process.
     * <p>
     * {@code EINVAL} and {@code EPERM} are deliberately NOT here: EINVAL is the kernel's answer to every bad argument
     * (an unaligned offset, ranges overlapping within one file, a non-regular source, a remap it clamped and so
     * completed short), not just to a block size larger than {@link #RANGE_ALIGNMENT}, and EPERM is per-file (an
     * immutable or append-only destination), not per-filesystem. Remembering either would switch sharing off for a
     * whole filesystem that supports it, over one caller's arithmetic slip; they go to WARN on every occurrence
     * instead.
     * <p>
     * {@code EXDEV} is not here either, and that one is about the KEY. It says the source and the destination are on
     * different filesystems, which is a property of the PAIR, not of the destination: with two
     * {@code data_file_directories} on separate reflink-capable mounts, remembering it would permanently switch
     * sharing off for a destination that shares extents perfectly well because one source once was not co-located with
     * it. Nothing in this API requires them to be co-located -- only today's caller, which always places a child in
     * its parent's directory -- so a cross-filesystem attempt is refused per call and goes to WARN as well.
     */
    private static boolean isFilesystemLimitation(int errno)
    {
        return errno == EOPNOTSUPP || errno == ENOTTY || errno == ENOSYS;
    }

    /** Remember that the filesystem behind {@code key} cannot share extents. INFO once per filesystem per distinct
     * errno: a missing feature is an ordinary deployment, not a fault, but a second, different reason for it is worth
     * seeing. */
    private static void noteUnsupported(String key, File directory, int errno, String reason)
    {
        Integer previous = unsupported.put(key, errno);
        if (previous == null || previous != errno)
            logger.info("Byte-range extent sharing (reflink) is unavailable on the filesystem holding {}: {} ({})." +
                        " Ranges will be copied instead.", directory, reason, errno);
    }

    /**
     * Remember that {@link NativeLibrary#getfd} could not read {@code sun.nio.ch.FileChannelImpl.fd}, which it answers
     * -1 for only when reflection on that field fails: because the JVM was started without
     * {@code --add-opens java.base/sun.nio.ch=ALL-UNNAMED}, which {@code conf/jvm17-server.options} and
     * {@code conf/jvm21-server.options} pass but an embedded or tool launcher need not, or because the channel does not
     * come from the default {@code FileSystemProvider} at all. Either way it is a property of the JVM rather than of
     * any filesystem, so it is remembered once for the process and says which. Going through
     * {@link #noteUnsupported} would instead name each data directory in turn and blame its filesystem -- the wrong
     * thing to send an operator to check while sharing is silently off cluster-wide.
     */
    private static void noteDescriptorsUnreachable()
    {
        if (descriptorsUnreachable.compareAndSet(false, true))
            logger.warn("Byte-range extent sharing (reflink) is unavailable in this JVM rather than on these" +
                        " filesystems: FileChannel's file descriptor cannot be read, which needs --add-opens" +
                        " java.base/sun.nio.ch=ALL-UNNAMED (see conf/jvm17-server.options) and a channel from the" +
                        " default filesystem provider. Ranges will be copied instead.");
    }

    private static void requireAligned(String what, long value)
    {
        // Negatives are rejected explicitly: the mask test alone passes for e.g. -65536, and the kernel's EINVAL on the
        // resulting u64 is (rightly) not remembered, just a confusing way to find a sign error.
        if (value < 0 || (value & (RANGE_ALIGNMENT - 1)) != 0)
            throw new IllegalArgumentException(what + " must be a non-negative multiple of " + RANGE_ALIGNMENT +
                                               ", got " + value);
    }

    private static String strerror(int errno)
    {
        switch (errno)
        {
            case EPERM: return "EPERM";
            case EBADF: return "EBADF";
            case EXDEV: return "EXDEV: source and destination are on different filesystems";
            case EINVAL: return "EINVAL";
            case ENOTTY: return "ENOTTY: the filesystem does not implement FICLONERANGE";
            case ENOSYS: return "ENOSYS";
            case EOPNOTSUPP: return "EOPNOTSUPP: the filesystem cannot share extents";
            default: return "errno " + errno;
        }
    }
}
