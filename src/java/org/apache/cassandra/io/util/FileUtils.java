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

import java.io.*;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.attribute.FileStoreAttributeView;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.utils.SyncUtil;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.io.FSError;
import org.apache.cassandra.io.FSErrorHandler;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.memory.MemoryUtil;

import static com.google.common.base.Throwables.propagate;
import static org.apache.cassandra.utils.Throwables.maybeFail;
import static org.apache.cassandra.utils.Throwables.merge;

public final class FileUtils
{
    public static final Charset CHARSET = StandardCharsets.UTF_8;

    private static final Logger logger = LoggerFactory.getLogger(FileUtils.class);
    public static final long ONE_KB = 1024;
    public static final long ONE_MB = 1024 * ONE_KB;
    public static final long ONE_GB = 1024 * ONE_MB;
    public static final long ONE_TB = 1024 * ONE_GB;

    private static final DecimalFormat df = new DecimalFormat("#.##");
    private static final AtomicReference<Optional<FSErrorHandler>> fsErrorHandler = new AtomicReference<>(Optional.empty());

    private static Class clsDirectBuffer;
    private static MethodHandle mhDirectBufferCleaner;
    private static MethodHandle mhCleanerClean;

    static
    {
        try
        {
            clsDirectBuffer = Class.forName("sun.nio.ch.DirectBuffer");
            Method mDirectBufferCleaner = clsDirectBuffer.getMethod("cleaner");
            mhDirectBufferCleaner = MethodHandles.lookup().unreflect(mDirectBufferCleaner);
            Method mCleanerClean = mDirectBufferCleaner.getReturnType().getMethod("clean");
            mhCleanerClean = MethodHandles.lookup().unreflect(mCleanerClean);

            ByteBuffer buf = ByteBuffer.allocateDirect(1);
            clean(buf);
        }
        catch (Throwable t)
        {
            logger.error("FATAL: Cannot initialize optimized memory deallocator. Some data, both in-memory and on-disk, may live longer due to garbage collection.");
            JVMStabilityInspector.inspectThrowable(t);
            throw new RuntimeException(t);
        }
    }

    public static void createHardLink(String from, String to)
    {
        createHardLink(new File(from), new File(to));
    }

    public static void createHardLink(File from, File to)
    {
        if (to.exists())
            throw new RuntimeException("Tried to create duplicate hard link to " + to);
        if (!from.exists())
            throw new RuntimeException("Tried to hard link to file that does not exist " + from);

        try
        {
            Files.createLink(to.toPath(), from.toPath());
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, to);
        }
    }

    private static final File tempDir = new File(System.getProperty("java.io.tmpdir"));
    private static final AtomicLong tempFileNum = new AtomicLong();

    public static File getTempDir()
    {
        return tempDir;
    }

    /**
     * Pretty much like {@link File#createTempFile(String, String, File)}, but with
     * the guarantee that the "random" part of the generated file name between
     * {@code prefix} and {@code suffix} is a positive, increasing {@code long} value.
     */
    public static File createTempFile(String prefix, String suffix, File directory)
    {
        try
        {
            // Do not use java.io.File.createTempFile(), because some tests rely on the
            // behavior that the "random" part in the temp file name is a positive 'long'.
            // However, at least since Java 9 the code to generate the "random" part
            // uses an _unsigned_ random long generated like this:
            // Long.toUnsignedString(new java.util.Random.nextLong())

            while (true)
            {
                // The contract of File.createTempFile() says, that it must not return
                // the same file name again. We do that here in a very simple way,
                // that probably doesn't cover all edge cases. Just rely on system
                // wall clock and return strictly increasing values from that.
                long num = tempFileNum.getAndIncrement();

                // We have a positive long here, which is safe to use for example
                // for CommitLogTest.
                String fileName = prefix + Long.toString(num) + suffix;
                File candidate = new File(directory, fileName);
                if (candidate.createNewFile())
                    return candidate;
            }
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, directory);
        }
    }

    public static File createTempFile(String prefix, String suffix)
    {
        return createTempFile(prefix, suffix, tempDir);
    }

    public static File createDeletableTempFile(String prefix, String suffix)
    {
        File f = createTempFile(prefix, suffix, getTempDir());
        f.deleteOnExit();
        return f;
    }

    public static Throwable deleteWithConfirm(String filePath, boolean expect, Throwable accumulate)
    {
        return deleteWithConfirm(new File(filePath), expect, accumulate);
    }

    public static Throwable deleteWithConfirm(File file, boolean expect, Throwable accumulate)
    {
        boolean exists = file.exists();
        assert exists || !expect : "attempted to delete non-existing file " + file.getName();
        try
        {
            if (exists)
                Files.delete(file.toPath());
        }
        catch (Throwable t)
        {
            try
            {
                throw new FSWriteError(t, file);
            }
            catch (Throwable t2)
            {
                accumulate = merge(accumulate, t2);
            }
        }
        return accumulate;
    }

    public static void deleteWithConfirm(String file)
    {
        deleteWithConfirm(new File(file));
    }

    public static void deleteWithConfirm(File file)
    {
        maybeFail(deleteWithConfirm(file, true, null));
    }

    public static void renameWithOutConfirm(String from, String to)
    {
        try
        {
            atomicMoveWithFallback(new File(from).toPath(), new File(to).toPath());
        }
        catch (IOException e)
        {
            if (logger.isTraceEnabled())
                logger.trace("Could not move file "+from+" to "+to, e);
        }
    }

    public static void renameWithConfirm(String from, String to)
    {
        renameWithConfirm(new File(from), new File(to));
    }

    public static void renameWithConfirm(File from, File to)
    {
        assert from.exists();
        if (logger.isTraceEnabled())
            logger.trace("Renaming {} to {}", from.getPath(), to.getPath());
        // this is not FSWE because usually when we see it it's because we didn't close the file before renaming it,
        // and Windows is picky about that.
        try
        {
            atomicMoveWithFallback(from.toPath(), to.toPath());
        }
        catch (IOException e)
        {
            throw new RuntimeException(String.format("Failed to rename %s to %s", from.getPath(), to.getPath()), e);
        }
    }

    /**
     * Move a file atomically, if it fails, it falls back to a non-atomic operation
     * @param from
     * @param to
     * @throws IOException
     */
    private static void atomicMoveWithFallback(Path from, Path to) throws IOException
    {
        try
        {
            Files.move(from, to, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        }
        catch (AtomicMoveNotSupportedException e)
        {
            logger.trace("Could not do an atomic move", e);
            Files.move(from, to, StandardCopyOption.REPLACE_EXISTING);
        }

    }
    public static void truncate(String path, long size)
    {
        try(FileChannel channel = FileChannel.open(Paths.get(path), StandardOpenOption.READ, StandardOpenOption.WRITE))
        {
            channel.truncate(size);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static void closeQuietly(Closeable c)
    {
        try
        {
            if (c != null)
                c.close();
        }
        catch (Exception e)
        {
            logger.warn("Failed closing {}", c, e);
        }
    }

    public static void closeQuietly(AutoCloseable c)
    {
        try
        {
            if (c != null)
                c.close();
        }
        catch (Exception e)
        {
            logger.warn("Failed closing {}", c, e);
        }
    }

    public static void close(Closeable... cs) throws IOException
    {
        close(Arrays.asList(cs));
    }

    public static void close(Iterable<? extends Closeable> cs) throws IOException
    {
        IOException e = null;
        for (Closeable c : cs)
        {
            try
            {
                if (c != null)
                    c.close();
            }
            catch (IOException ex)
            {
                e = ex;
                logger.warn("Failed closing stream {}", c, ex);
            }
        }
        if (e != null)
            throw e;
    }

    public static void closeQuietly(Iterable<? extends AutoCloseable> cs)
    {
        for (AutoCloseable c : cs)
        {
            try
            {
                if (c != null)
                    c.close();
            }
            catch (Exception ex)
            {
                logger.warn("Failed closing {}", c, ex);
            }
        }
    }

    public static String getCanonicalPath(String filename)
    {
        try
        {
            return new File(filename).getCanonicalPath();
        }
        catch (IOException e)
        {
            throw new FSReadError(e, filename);
        }
    }

    public static String getCanonicalPath(File file)
    {
        try
        {
            return file.getCanonicalPath();
        }
        catch (IOException e)
        {
            throw new FSReadError(e, file);
        }
    }

    /** Return true if file is contained in folder */
    public static boolean isContained(File folder, File file)
    {
        String folderPath = getCanonicalPath(folder);
        String filePath = getCanonicalPath(file);

        return filePath.startsWith(folderPath);
    }

    /** Convert absolute path into a path relative to the base path */
    public static String getRelativePath(String basePath, String path)
    {
        try
        {
            return Paths.get(basePath).relativize(Paths.get(path)).toString();
        }
        catch(Exception ex)
        {
            String absDataPath = FileUtils.getCanonicalPath(basePath);
            return Paths.get(absDataPath).relativize(Paths.get(path)).toString();
        }
    }

    public static void clean(ByteBuffer buffer)
    {
        if (buffer == null || !buffer.isDirect())
            return;

        // TODO Once we can get rid of Java 8, it's simpler to call sun.misc.Unsafe.invokeCleaner(ByteBuffer),
        // but need to take care of the attachment handling (i.e. whether 'buf' is a duplicate or slice) - that
        // is different in sun.misc.Unsafe.invokeCleaner and this implementation.

        try
        {
            Object cleaner = mhDirectBufferCleaner.bindTo(buffer).invoke();
            if (cleaner != null)
            {
                // ((DirectBuffer) buf).cleaner().clean();
                mhCleanerClean.bindTo(cleaner).invoke();
            }
        }
        catch (RuntimeException e)
        {
            throw e;
        }
        catch (Throwable e)
        {
            throw new RuntimeException(e);
        }
    }

    public static void createDirectory(String directory)
    {
        createDirectory(new File(directory));
    }

    public static void createDirectory(File directory)
    {
        if (!directory.exists())
        {
            if (!directory.mkdirs())
                throw new FSWriteError(new IOException("Failed to mkdirs " + directory), directory);
        }
    }

    public static boolean delete(String file)
    {
        File f = new File(file);
        return f.delete();
    }

    public static void delete(File... files)
    {
        for ( File file : files )
        {
            file.delete();
        }
    }

    public static void deleteAsync(final String file)
    {
        Runnable runnable = new Runnable()
        {
            public void run()
            {
                deleteWithConfirm(new File(file));
            }
        };
        ScheduledExecutors.nonPeriodicTasks.execute(runnable);
    }

    public static long parseFileSize(String value)
    {
        long result;
        if (!value.matches("\\d+(\\.\\d+)? (GiB|KiB|MiB|TiB|bytes)"))
        {
            throw new IllegalArgumentException(
                String.format("value %s is not a valid human-readable file size", value));
        }
        if (value.endsWith(" TiB"))
        {
            result = Math.round(Double.valueOf(value.replace(" TiB", "")) * ONE_TB);
            return result;
        }
        else if (value.endsWith(" GiB"))
        {
            result = Math.round(Double.valueOf(value.replace(" GiB", "")) * ONE_GB);
            return result;
        }
        else if (value.endsWith(" KiB"))
        {
            result = Math.round(Double.valueOf(value.replace(" KiB", "")) * ONE_KB);
            return result;
        }
        else if (value.endsWith(" MiB"))
        {
            result = Math.round(Double.valueOf(value.replace(" MiB", "")) * ONE_MB);
            return result;
        }
        else if (value.endsWith(" bytes"))
        {
            result = Math.round(Double.valueOf(value.replace(" bytes", "")));
            return result;
        }
        else
        {
            throw new IllegalStateException(String.format("FileUtils.parseFileSize() reached an illegal state parsing %s", value));
        }
    }

    public static String stringifyFileSize(double value)
    {
        double d;
        if ( value >= ONE_TB )
        {
            d = value / ONE_TB;
            String val = df.format(d);
            return val + " TiB";
        }
        else if ( value >= ONE_GB )
        {
            d = value / ONE_GB;
            String val = df.format(d);
            return val + " GiB";
        }
        else if ( value >= ONE_MB )
        {
            d = value / ONE_MB;
            String val = df.format(d);
            return val + " MiB";
        }
        else if ( value >= ONE_KB )
        {
            d = value / ONE_KB;
            String val = df.format(d);
            return val + " KiB";
        }
        else
        {
            String val = df.format(value);
            return val + " bytes";
        }
    }

    /**
     * Deletes all files and subdirectories under "dir".
     * @param dir Directory to be deleted
     * @throws FSWriteError if any part of the tree cannot be deleted
     */
    public static void deleteRecursive(File dir)
    {
        if (dir.isDirectory())
        {
            String[] children = dir.list();
            for (String child : children)
                deleteRecursive(new File(dir, child));
        }

        // The directory is now empty so now it can be smoked
        deleteWithConfirm(dir);
    }

    /**
     * Schedules deletion of all file and subdirectories under "dir" on JVM shutdown.
     * @param dir Directory to be deleted
     */
    public static void deleteRecursiveOnExit(File dir)
    {
        if (dir.isDirectory())
        {
            String[] children = dir.list();
            for (String child : children)
                deleteRecursiveOnExit(new File(dir, child));
        }

        logger.trace("Scheduling deferred deletion of file: {}", dir);
        dir.deleteOnExit();
    }

    public static void handleCorruptSSTable(CorruptSSTableException e)
    {
        fsErrorHandler.get().ifPresent(handler -> handler.handleCorruptSSTable(e));
    }

    public static void handleFSError(FSError e)
    {
        fsErrorHandler.get().ifPresent(handler -> handler.handleFSError(e));
    }

    /**
     * handleFSErrorAndPropagate will invoke the disk failure policy error handler,
     * which may or may not stop the daemon or transports. However, if we don't exit,
     * we still want to propagate the exception to the caller in case they have custom
     * exception handling
     *
     * @param e A filesystem error
     */
    public static void handleFSErrorAndPropagate(FSError e)
    {
        handleFSError(e);
        throw propagate(e);
    }

    /**
     * Get the size of a directory in bytes
     * @param folder The directory for which we need size.
     * @return The size of the directory
     */
    public static long folderSize(File folder)
    {
        final long [] sizeArr = {0L};
        try
        {
            Files.walkFileTree(folder.toPath(), new SimpleFileVisitor<Path>()
            {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                {
                    sizeArr[0] += attrs.size();
                    return FileVisitResult.CONTINUE;
                }
            });
        }
        catch (IOException e)
        {
            logger.error("Error while getting {} folder size. {}", folder, e.getMessage());
        }
        return sizeArr[0];
    }

    public static void copyTo(DataInput in, OutputStream out, int length) throws IOException
    {
        byte[] buffer = new byte[64 * 1024];
        int copiedBytes = 0;

        while (copiedBytes + buffer.length < length)
        {
            in.readFully(buffer);
            out.write(buffer);
            copiedBytes += buffer.length;
        }

        if (copiedBytes < length)
        {
            int left = length - copiedBytes;
            in.readFully(buffer, 0, left);
            out.write(buffer, 0, left);
        }
    }

    public static boolean isSubDirectory(File parent, File child) throws IOException
    {
        parent = parent.getCanonicalFile();
        child = child.getCanonicalFile();

        File toCheck = child;
        while (toCheck != null)
        {
            if (parent.equals(toCheck))
                return true;
            toCheck = toCheck.getParentFile();
        }
        return false;
    }

    public static void append(File file, String ... lines)
    {
        if (file.exists())
            write(file, Arrays.asList(lines), StandardOpenOption.APPEND);
        else
            write(file, Arrays.asList(lines), StandardOpenOption.CREATE);
    }

    public static void appendAndSync(File file, String ... lines)
    {
        if (file.exists())
            write(file, Arrays.asList(lines), StandardOpenOption.APPEND, StandardOpenOption.SYNC);
        else
            write(file, Arrays.asList(lines), StandardOpenOption.CREATE, StandardOpenOption.SYNC);
    }

    public static void replace(File file, String ... lines)
    {
        write(file, Arrays.asList(lines), StandardOpenOption.TRUNCATE_EXISTING);
    }

    /**
     * Write lines to a file adding a newline to the end of each supplied line using the provided open options.
     *
     * If open option sync or dsync is provided this will not open the file with sync or dsync since it might end up syncing
     * many times for a lot of lines. Instead it will write all the lines and sync once at the end. Since the file is
     * never returned there is not much difference from the perspective of the caller.
     * @param file
     * @param lines
     * @param options
     */
    public static void write(File file, List<String> lines, StandardOpenOption ... options)
    {
        Set<StandardOpenOption> optionsSet = new HashSet<>(Arrays.asList(options));
        //Emulate the old FileSystemProvider.newOutputStream behavior for open options.
        if (optionsSet.isEmpty())
        {
            optionsSet.add(StandardOpenOption.CREATE);
            optionsSet.add(StandardOpenOption.TRUNCATE_EXISTING);
        }
        boolean sync = optionsSet.remove(StandardOpenOption.SYNC);
        boolean dsync = optionsSet.remove(StandardOpenOption.DSYNC);
        optionsSet.add(StandardOpenOption.WRITE);

        Path filePath = file.toPath();
        try (FileChannel fc = filePath.getFileSystem().provider().newFileChannel(filePath, optionsSet);
             BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(Channels.newOutputStream(fc), CHARSET.newEncoder())))
        {
            for (CharSequence line: lines) {
                writer.append(line);
                writer.newLine();
            }

            if (sync)
            {
                SyncUtil.force(fc, true);
            }
            else if (dsync)
            {
                SyncUtil.force(fc, false);
            }
        }
        catch (IOException ex)
        {
            throw new RuntimeException(ex);
        }
    }

    public static List<String> readLines(File file)
    {
        try
        {
            return Files.readAllLines(file.toPath(), CHARSET);
        }
        catch (IOException ex)
        {
            if (ex instanceof NoSuchFileException)
                return Collections.emptyList();

            throw new RuntimeException(ex);
        }
    }

    public static void setFSErrorHandler(FSErrorHandler handler)
    {
        fsErrorHandler.getAndSet(Optional.ofNullable(handler));
    }

    /**
     * Returns the size of the specified partition.
     * <p>This method handles large file system by returning {@code Long.MAX_VALUE} if the  size overflow.
     * See <a href='https://bugs.openjdk.java.net/browse/JDK-8179320'>JDK-8179320</a> for more information.</p>
     *
     * @param file the partition
     * @return the size, in bytes, of the partition or {@code 0L} if the abstract pathname does not name a partition
     */
    public static long getTotalSpace(File file)
    {
        return handleLargeFileSystem(file.getTotalSpace());
    }

    /**
     * Returns the number of unallocated bytes on the specified partition.
     * <p>This method handles large file system by returning {@code Long.MAX_VALUE} if the  number of unallocated bytes
     * overflow. See <a href='https://bugs.openjdk.java.net/browse/JDK-8179320'>JDK-8179320</a> for more information</p>
     *
     * @param file the partition
     * @return the number of unallocated bytes on the partition or {@code 0L}
     * if the abstract pathname does not name a partition.
     */
    public static long getFreeSpace(File file)
    {
        return handleLargeFileSystem(file.getFreeSpace());
    }

    /**
     * Returns the number of available bytes on the specified partition.
     * <p>This method handles large file system by returning {@code Long.MAX_VALUE} if the  number of available bytes
     * overflow. See <a href='https://bugs.openjdk.java.net/browse/JDK-8179320'>JDK-8179320</a> for more information</p>
     *
     * @param file the partition
     * @return the number of available bytes on the partition or {@code 0L}
     * if the abstract pathname does not name a partition.
     */
    public static long getUsableSpace(File file)
    {
        return handleLargeFileSystem(file.getUsableSpace());
    }

    /**
     * Returns the {@link FileStore} representing the file store where a file
     * is located. This {@link FileStore} handles large file system by returning {@code Long.MAX_VALUE}
     * from {@code FileStore#getTotalSpace()}, {@code FileStore#getUnallocatedSpace()} and {@code FileStore#getUsableSpace()}
     * it the value is bigger than {@code Long.MAX_VALUE}. See <a href='https://bugs.openjdk.java.net/browse/JDK-8162520'>JDK-8162520</a>
     * for more information.
     *
     * @param path the path to the file
     * @return the file store where the file is stored
     */
    public static FileStore getFileStore(Path path) throws IOException
    {
        return new SafeFileStore(Files.getFileStore(path));
    }

    /**
     * Handle large file system by returning {@code Long.MAX_VALUE} when the size overflows.
     * @param size returned by the Java's FileStore methods
     * @return the size or {@code Long.MAX_VALUE} if the size was bigger than {@code Long.MAX_VALUE}
     */
    private static long handleLargeFileSystem(long size)
    {
        return size < 0 ? Long.MAX_VALUE : size;
    }

    /**
     * Private constructor as the class contains only static methods.
     */
    private FileUtils()
    {
    }

    /**
     * FileStore decorator used to safely handle large file system.
     *
     * <p>Java's FileStore methods (getTotalSpace/getUnallocatedSpace/getUsableSpace) are limited to reporting bytes as
     * signed long (2^63-1), if the filesystem is any bigger, then the size overflows. {@code SafeFileStore} will
     * return {@code Long.MAX_VALUE} if the size overflow.</p>
     *
     * @see https://bugs.openjdk.java.net/browse/JDK-8162520.
     */
    private static final class SafeFileStore extends FileStore
    {
        /**
         * The decorated {@code FileStore}
         */
        private final FileStore fileStore;

        public SafeFileStore(FileStore fileStore)
        {
            this.fileStore = fileStore;
        }

        @Override
        public String name()
        {
            return fileStore.name();
        }

        @Override
        public String type()
        {
            return fileStore.type();
        }

        @Override
        public boolean isReadOnly()
        {
            return fileStore.isReadOnly();
        }

        @Override
        public long getTotalSpace() throws IOException
        {
            return handleLargeFileSystem(fileStore.getTotalSpace());
        }

        @Override
        public long getUsableSpace() throws IOException
        {
            return handleLargeFileSystem(fileStore.getUsableSpace());
        }

        @Override
        public long getUnallocatedSpace() throws IOException
        {
            return handleLargeFileSystem(fileStore.getUnallocatedSpace());
        }

        @Override
        public boolean supportsFileAttributeView(Class<? extends FileAttributeView> type)
        {
            return fileStore.supportsFileAttributeView(type);
        }

        @Override
        public boolean supportsFileAttributeView(String name)
        {
            return fileStore.supportsFileAttributeView(name);
        }

        @Override
        public <V extends FileStoreAttributeView> V getFileStoreAttributeView(Class<V> type)
        {
            return fileStore.getFileStoreAttributeView(type);
        }

        @Override
        public Object getAttribute(String attribute) throws IOException
        {
            return fileStore.getAttribute(attribute);
        }
    }
}
