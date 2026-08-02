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

import java.nio.channels.FileChannel;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.sun.jna.LastErrorException;
import com.sun.jna.Memory;
import com.sun.jna.Native;
import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.utils.NativeLibrary;

/**
 * Byte-range extent sharing between two files on the same filesystem -- a "reflink" -- via the Linux
 * {@code FICLONERANGE} ioctl.
 *
 * <h2>What it actually does</h2>
 * {@code FICLONERANGE} makes a range of the destination file point at the <em>same physical extents</em> as a
 * range of the source file and bumps their reference count. No data is read, no data is written, no page cache
 * is populated, and the cost is a refcount-btree update proportional to the number of extents rather than to
 * the number of bytes. The two files stay fully independent afterwards: either can be truncated, overwritten
 * or unlinked, and the filesystem copies the affected blocks on the first write to a shared one. Blocks are
 * freed only when the last reference goes away, so cloning a range out of an sstable that is about to be
 * unlinked hands its extents to the clone instead of copying them and then freeing the originals.
 * <p>
 * This is the only one of the three ways to move bytes that does not move bytes:
 * <ul>
 *   <li>a userspace read/write loop copies through a heap or direct buffer;</li>
 *   <li>{@code FileChannel.transferTo} is {@code sendfile} on JDK 11 and {@code copy_file_range} on JDK 21+,
 *       which keeps the copy inside the kernel but still allocates and writes every destination block --
 *       {@code copy_file_range} <em>will</em> reflink instead, but only when the source offset, the destination
 *       offset and the length are all already block aligned, and it silently falls back to a full copy when
 *       they are not, with no way to find out which happened;</li>
 *   <li>{@code FICLONERANGE} shares the extents, and tells you when it could not.</li>
 * </ul>
 * That last property is why this goes through JNA rather than leaning on {@code transferTo}: a caller that
 * pads its destination specifically to make sharing possible needs to know whether the padding bought
 * anything, and the ioctl either succeeds or reports why not.
 *
 * <h2>Alignment: the requirement that shapes every caller</h2>
 * The kernel demands that {@code srcOffset}, {@code dstOffset} and {@code length} all be multiples of the
 * filesystem block size (the last block may be partial only when the source range ends exactly at the source's
 * EOF, which is not a case worth relying on). Unaligned arguments fail with {@code EINVAL}; they are not
 * quietly rounded, and the aligned interior is not cloned for you.
 * <p>
 * {@link #RANGE_ALIGNMENT} is 64 KiB rather than a value read out of {@code statvfs}, because a filesystem
 * block can never exceed the page size and 64 KiB is the largest page size Linux runs with. Any offset that is
 * a multiple of 64 KiB is therefore a multiple of the block size on every filesystem this can be asked about,
 * which removes an entire native call and its structure-layout hazard in exchange for at most 64 KiB of slack
 * per cloned range.
 *
 * <h2>Availability, and how it is discovered</h2>
 * Extent sharing needs a filesystem that has a refcount btree: xfs formatted with {@code -m reflink=1}
 * (the mkfs default since xfsprogs 5.1), btrfs, bcachefs, and OCFS2. On ext4, tmpfs, NFS and everything else
 * the ioctl fails with {@code EOPNOTSUPP} or {@code ENOTTY}; across two mounts it fails with {@code EXDEV}.
 * There is no cheap way to ask in advance -- {@code statfs} type tells you the filesystem but not whether
 * reflink was enabled at mkfs time -- so support is discovered by trying, and a failure that means "this
 * filesystem cannot do it" is remembered per directory in {@link #unsupported} so the next caller does not pay
 * for another failing syscall. Every distinct errno is logged once per directory at WARN, because a failure
 * for any reason other than the filesystem lacking the feature is a bug in the caller's arithmetic and must
 * not be swallowed silently.
 *
 * <h2>One caveat worth stating explicitly</h2>
 * Shared extents are cheap on disk but not in RAM: the page cache is indexed per inode, so bytes read through
 * both files are cached twice even though they are one copy on disk. That is only a real cost when both files
 * stay live and hot; when the clone exists to replace its source, the source's cache is dropped along with it.
 */
public final class Reflink
{
    private static final Logger logger = LoggerFactory.getLogger(Reflink.class);

    /**
     * {@code FICLONERANGE == _IOW(0x94, 13, struct file_clone_range)}, i.e.
     * {@code (1 << 30) | (32 << 16) | (0x94 << 8) | 13}. The 32 is {@code sizeof(struct file_clone_range)};
     * the request number is part of the kernel ABI and is identical on every Linux architecture.
     */
    private static final long FICLONERANGE = 0x4020940DL;

    /** {@code sizeof(struct file_clone_range)}: four 8-byte fields, naturally aligned, no padding. */
    private static final int FILE_CLONE_RANGE_SIZE = 32;

    /**
     * Every offset and length handed to {@link #tryCloneRange} must be a multiple of this. See the class
     * javadoc: it is the largest possible filesystem block size, not the actual one, so that no native call is
     * needed to discover the actual one.
     */
    public static final long RANGE_ALIGNMENT = 64 << 10;

    // errno values that mean "this filesystem/kernel/mount pairing can never share extents", as opposed to
    // "these particular arguments were wrong". Linux asm-generic values, identical on every architecture
    // Cassandra runs on.
    private static final int EPERM = 1;
    private static final int EBADF = 9;
    private static final int EXDEV = 18;
    private static final int EINVAL = 22;
    private static final int ENOTTY = 25;
    private static final int ENOSYS = 38;
    private static final int EOPNOTSUPP = 95;

    /**
     * Directories whose filesystem has already answered "no". Keyed by directory rather than by mount because
     * a directory is what a caller has in hand and it can only ever be on one filesystem, so this is strictly
     * finer grained than a mount and cannot let one ext4 data directory disable sharing on an xfs one.
     */
    private static final Map<String, Integer> unsupported = new ConcurrentHashMap<>();

    private static final boolean LINKED;

    static
    {
        boolean linked = false;
        try
        {
            // Registered on this class rather than added to NativeLibraryLinux so that a link failure here can
            // never take mlockall/fadvise/fcntl down with it: Native.register links every native method of the
            // class it is given at once. ioctl(2) is in every libc that has ever existed, so this is belt and
            // braces -- unlike copy_file_range, which glibc only exposes from 2.27 and which would fail to
            // link on an older base image.
            if (NativeLibrary.osType == NativeLibrary.OSType.LINUX)
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
     * Whether {@link #tryCloneRange} is worth attempting for a file in {@code directory}: the ioctl is linked,
     * this is Linux, and nothing in {@code directory} has failed with a filesystem-level error yet. Callers use
     * this to decide up front whether to lay their destination out so that sharing is possible at all --
     * {@link #RANGE_ALIGNMENT} usually costs something to arrange, and there is no point paying for it on ext4.
     * <p>
     * Optimistic by construction: an untried directory answers true, so the first caller pays for one failing
     * ioctl and whatever its own alignment cost was.
     */
    public static boolean isPossibleIn(File directory)
    {
        return LINKED && !unsupported.containsKey(directory.path());
    }

    /**
     * Share {@code length} bytes of {@code src} starting at {@code srcOffset} into {@code dst} at
     * {@code dstOffset}, moving no data.
     * <p>
     * All three of {@code srcOffset}, {@code dstOffset} and {@code length} must be multiples of
     * {@link #RANGE_ALIGNMENT}, and {@code srcOffset + length} must not exceed the source's length. The
     * destination's length grows to at least {@code dstOffset + length}; its file position is NOT moved, so a
     * caller that goes on to write the tail conventionally must position the channel itself.
     * <p>
     * Neither channel is closed, flushed or synced. A successful clone is a metadata change like any other
     * write: it still needs {@code force()} before anything may depend on it surviving a crash.
     *
     * @param directory the directory the destination lives in, used only as the key of the negative cache
     * @return true if the range is now shared; false if this filesystem cannot share extents, in which case
     *         nothing at all has been written and the caller must copy the bytes itself
     * @throws IllegalArgumentException if the arguments are not aligned, which is a caller bug rather than a
     *         filesystem limitation and must not be answered by silently copying
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

        if (!isPossibleIn(directory))
            return false;

        int srcFd = NativeLibrary.getfd(src);
        int dstFd = NativeLibrary.getfd(dst);
        if (srcFd < 0 || dstFd < 0)
        {
            // Only happens when sun.nio.ch.FileChannelImpl.fd is not reflectively reachable, i.e. the
            // --add-opens the startup script passes is missing. Nothing to do but copy the bytes.
            noteUnsupported(directory, EBADF, "file descriptors are not reachable from FileChannel");
            return false;
        }

        // struct file_clone_range, filled by hand rather than through a JNA Structure: four fixed-width
        // fields with no padding on any ABI, so there is no layout to get wrong, and Memory is already in
        // native byte order.
        try (Memory arg = new Memory(FILE_CLONE_RANGE_SIZE))
        {
            arg.setLong(0, srcFd);        // __s64 src_fd
            arg.setLong(8, srcOffset);    // __u64 src_offset
            arg.setLong(16, length);      // __u64 src_length
            arg.setLong(24, dstOffset);   // __u64 dest_offset

            // FICLONERANGE is all-or-nothing: it returns 0 having shared every byte, or -1 having shared none.
            // There is no short-clone case to loop over, and length is a u64, so even a terabyte range is one
            // call. The return value is checked as well as the exception, because JNA raises
            // LastErrorException off errno rather than off the return value.
            int rc = ioctl(dstFd, new NativeLong(FICLONERANGE), arg);
            if (rc != 0)
            {
                logger.warn("FICLONERANGE of {} bytes at {} returned {} without setting errno; copying instead",
                            length, srcOffset, rc);
                return false;
            }
            logger.trace("Shared {} bytes at {} into {} at {}", length, srcOffset, directory, dstOffset);
            return true;
        }
        catch (LastErrorException e)
        {
            int errno = e.getErrorCode();
            if (isFilesystemLimitation(errno))
                noteUnsupported(directory, errno, strerror(errno));
            else
                logger.warn("FICLONERANGE of {} bytes at {} failed with errno {} ({}); copying instead",
                            length, srcOffset, errno, strerror(errno));
            return false;
        }
        catch (UnsatisfiedLinkError e)
        {
            noteUnsupported(directory, ENOSYS, "ioctl(2) is not linked");
            return false;
        }
    }

    /**
     * Forget every remembered negative answer. Only for tests, which mount and unmount filesystems under the
     * same paths far more often than a running node does.
     */
    public static void resetSupportCache()
    {
        unsupported.clear();
    }

    /**
     * The errnos that mean the filesystem itself cannot do this, so no future call for the same directory can
     * succeed either. {@code EINVAL} is included with a wince: it also covers bad arguments, but this method
     * validates alignment up front and refuses to run with anything else the kernel could object to, so the
     * remaining source of {@code EINVAL} is a filesystem whose block size somehow exceeds
     * {@link #RANGE_ALIGNMENT}. It is logged either way.
     */
    private static boolean isFilesystemLimitation(int errno)
    {
        return errno == EOPNOTSUPP || errno == ENOTTY || errno == ENOSYS
               || errno == EXDEV || errno == EINVAL || errno == EPERM;
    }

    private static void noteUnsupported(File directory, int errno, String reason)
    {
        Integer previous = unsupported.putIfAbsent(directory.path(), errno);
        if (previous == null)
            logger.info("Byte-range extent sharing (reflink) is unavailable in {}: {} ({}). Ranges will be" +
                        " copied instead.", directory, reason, errno);
    }

    private static void requireAligned(String what, long value)
    {
        if ((value & (RANGE_ALIGNMENT - 1)) != 0)
            throw new IllegalArgumentException(what + " must be a multiple of " + RANGE_ALIGNMENT +
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
