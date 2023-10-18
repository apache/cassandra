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

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.FSError;
import org.apache.cassandra.io.FSErrorHandler;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.SyncUtil;

import static org.apache.cassandra.config.CassandraRelevantProperties.JAVA_IO_TMPDIR;
import static org.apache.cassandra.utils.Throwables.maybeFail;

public final class FileUtils
{
    public static final Charset CHARSET = StandardCharsets.UTF_8;

    private static final Logger logger = LoggerFactory.getLogger(FileUtils.class);

    public static final long ONE_KIB = 1024;
    public static final long ONE_MIB = 1024 * ONE_KIB;
    public static final long ONE_GIB = 1024 * ONE_MIB;
    public static final long ONE_TIB = 1024 * ONE_GIB;

    private static final DecimalFormat df = new DecimalFormat("#.##");
    private static final AtomicReference<Optional<FSErrorHandler>> fsErrorHandler = new AtomicReference<>(Optional.empty());

    private static final Class clsDirectBuffer;
    private static final MethodHandle mhDirectBufferCleaner;
    private static final MethodHandle mhCleanerClean;

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
        catch (IllegalAccessException e)
        {
            logger.error("FATAL: Cassandra is unable to access required classes. This usually means it has been " +
                "run without the aid of the standard startup scripts or the scripts have been edited. If this was " +
                "intentional, and you are attempting to use Java 11+ you may need to add the --add-exports and " +
                "--add-opens jvm options from either jvm11-server.options or jvm11-client.options", e);
            throw new RuntimeException(e);  // causes ExceptionInInitializerError, will prevent startup
        }
        catch (Throwable t)
        {
            logger.error("FATAL: Cannot initialize optimized memory deallocator.", t);
            JVMStabilityInspector.inspectThrowable(t);
            throw new RuntimeException(t); // causes ExceptionInInitializerError, will prevent startup
        }
    }

    private static final File tempDir = new File(JAVA_IO_TMPDIR.getString());
    private static final AtomicLong tempFileNum = new AtomicLong();

    public static File getTempDir()
    {
        return tempDir;
    }

    /**
     * Pretty much like {@link java.io.File#createTempFile(String, String, java.io.File)}, but with
     * the guarantee that the "random" part of the generated file name between
     * {@code prefix} and {@code suffix} is a positive, increasing {@code long} value.
     */
    public static File createTempFile(String prefix, String suffix, File directory)
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
            String fileName = prefix + num + suffix;
            File candidate = new File(directory, fileName);
            if (candidate.createFileIfNotExists())
                return candidate;
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

    public static void createHardLinkWithConfirm(String from, String to)
    {
        createHardLinkWithConfirm(new File(from), new File(to));
    }

    public static void createHardLinkWithConfirm(File from, File to)
    {
        try
        {
            createHardLink(from, to);
        }
        catch (FSWriteError ex)
        {
            throw ex;
        }
        catch (Throwable t)
        {
            throw new RuntimeException(String.format("Unable to hardlink from %s to %s", from, to), t);
        }
    }

    public static void createHardLinkWithoutConfirm(String from, String to)
    {
        createHardLinkWithoutConfirm(new File(from), new File(to));
    }

    public static void createHardLinkWithoutConfirm(File from, File to)
    {
        try
        {
            createHardLink(from, to);
        }
        catch (FSWriteError fse)
        {
            if (logger.isTraceEnabled())
                logger.trace("Could not hardlink file " + from + " to " + to, fse);
        }
    }

    public static void copyWithOutConfirm(String from, String to)
    {
        copyWithOutConfirm(new File(from), new File(to));
    }

    public static void copyWithOutConfirm(File from, File to)
    {
        try
        {
            Files.copy(from.toPath(), to.toPath());
        }
        catch (IOException e)
        {
            if (logger.isTraceEnabled())
                logger.trace("Could not copy file" + from + " to " + to, e);
        }
    }

    public static void copyWithConfirm(String from, String to)
    {
        copyWithConfirm(new File(from), new File(to));
    }

    public static void copyWithConfirm(File from, File to)
    {
        assert from.exists();
        if (logger.isTraceEnabled())
            logger.trace("Copying {} to {}", from.path(), to.path());

        try
        {
            Files.copy(from.toPath(), to.toPath());
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, "Could not copy file" + from + " to " + to);
        }
    }

    public static void truncate(String path, long size)
    {
        File file = new File(path);
        try (FileChannel channel = file.newReadWriteChannel())
        {
            channel.truncate(size);
        }
        catch (IOException e)
        {
            throw PathUtils.propagateUnchecked(e, file.toPath(), true);
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
        Throwable e = null;
        for (Closeable c : cs)
        {
            try
            {
                if (c != null)
                    c.close();
            }
            catch (Throwable ex)
            {
                if (e == null) e = ex;
                else e.addSuppressed(ex);
                logger.warn("Failed closing stream {}", c, ex);
            }
        }
        maybeFail(e, IOException.class);
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
        return new File(filename).canonicalPath();
    }

    public static String getCanonicalPath(File file)
    {
        return file.canonicalPath();
    }

    /** Return true if file is contained in folder */
    public static boolean isContained(File folder, File file)
    {
        return folder.isAncestorOf(file);
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
            result = Math.round(Double.valueOf(value.replace(" TiB", "")) * ONE_TIB);
            return result;
        }
        else if (value.endsWith(" GiB"))
        {
            result = Math.round(Double.valueOf(value.replace(" GiB", "")) * ONE_GIB);
            return result;
        }
        else if (value.endsWith(" KiB"))
        {
            result = Math.round(Double.valueOf(value.replace(" KiB", "")) * ONE_KIB);
            return result;
        }
        else if (value.endsWith(" MiB"))
        {
            result = Math.round(Double.valueOf(value.replace(" MiB", "")) * ONE_MIB);
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
        if (value >= ONE_TIB)
        {
            d = value / ONE_TIB;
            String val = df.format(d);
            return val + " TiB";
        }
        else if (value >= ONE_GIB)
        {
            d = value / ONE_GIB;
            String val = df.format(d);
            return val + " GiB";
        }
        else if (value >= ONE_MIB)
        {
            d = value / ONE_MIB;
            String val = df.format(d);
            return val + " MiB";
        }
        else if (value >= ONE_KIB)
        {
            d = value / ONE_KIB;
            String val = df.format(d);
            return val + " KiB";
        }
        else
        {
            String val = df.format(value);
            return val + " bytes";
        }
    }

    public static void handleCorruptSSTable(CorruptSSTableException e)
    {
        fsErrorHandler.get().ifPresent(handler -> handler.handleCorruptSSTable(e));
    }

    public static void handleFSError(FSError e)
    {
        fsErrorHandler.get().ifPresent(handler -> handler.handleFSError(e));
    }

    public static void handleStartupFSError(Throwable t)
    {
        fsErrorHandler.get().ifPresent(handler -> handler.handleStartupFSError(t));
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
        JVMStabilityInspector.inspectThrowable(e);
        throw e;
    }

    /**
     * Get the size of a directory in bytes
     * @param folder The directory for which we need size.
     * @return The size of the directory
     */
    public static long folderSize(File folder)
    {
        if (!folder.exists())
            return 0;

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

                @Override
                public FileVisitResult visitFileFailed(Path path, IOException e) throws IOException
                {
                    if (e instanceof NoSuchFileException)
                        return FileVisitResult.CONTINUE;
                    else
                        throw e;
                }
            });
        }
        catch (IOException e)
        {
            logger.error("Error while getting {} folder size. {}", folder, e.getMessage());
        }
        return sizeArr[0];
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
        Set<StandardOpenOption> optionsSet = EnumSet.noneOf(StandardOpenOption.class);
        for (StandardOpenOption option : options)
            optionsSet.add(option);

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
        catch (ClosedChannelException cce)
        {
            throw new RuntimeException(cce);
        }
        catch (IOException ex)
        {
            throw new FSWriteError(ex, file);
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

    /** @deprecated See CASSANDRA-16926 */
    @Deprecated(since = "4.1")
    public static void createDirectory(String directory)
    {
        createDirectory(new File(directory));
    }

    /** @deprecated See CASSANDRA-16926 */
    @Deprecated(since = "4.1")
    public static void createDirectory(File directory)
    {
        PathUtils.createDirectoriesIfNotExists(directory.toPath());
    }

    /** @deprecated See CASSANDRA-16926 */
    @Deprecated(since = "4.1")
    public static boolean delete(String file)
    {
        return new File(file).tryDelete();
    }

    /** @deprecated See CASSANDRA-16926 */
    @Deprecated(since = "4.1")
    public static void delete(File... files)
    {
        for (File file : files)
            file.tryDelete();
    }

    /**
     * Deletes all files and subdirectories under "dir".
     * @param dir Directory to be deleted
     * @throws FSWriteError if any part of the tree cannot be deleted
     *
     * @deprecated See CASSANDRA-16926
     */
    @Deprecated(since = "4.1")
    public static void deleteRecursiveWithThrottle(File dir, RateLimiter rateLimiter)
    {
        dir.deleteRecursive(rateLimiter);
    }

    /**
     * Deletes all files and subdirectories under "dir".
     * @param dir Directory to be deleted
     * @throws FSWriteError if any part of the tree cannot be deleted
     *
     * @deprecated See CASSANDRA-16926
     */
    @Deprecated(since = "4.1")
    public static void deleteRecursive(File dir)
    {
        dir.deleteRecursive();
    }

    /**
     * Schedules deletion of all file and subdirectories under "dir" on JVM shutdown.
     * @param dir Directory to be deleted
     *
     * @deprecated See CASSANDRA-16926
     */
    @Deprecated(since = "4.1")
    public static void deleteRecursiveOnExit(File dir)
    {
        dir.deleteRecursiveOnExit();
    }

    /** @deprecated See CASSANDRA-16926 */
    @Deprecated(since = "4.1")
    public static boolean isSubDirectory(File parent, File child)
    {
        return parent.isAncestorOf(child);
    }

    /** @deprecated See CASSANDRA-16926 */
    @Deprecated(since = "4.1")
    public static Throwable deleteWithConfirm(File file, Throwable accumulate)
    {
        return file.delete(accumulate, null);
    }

    /** @deprecated See CASSANDRA-16926 */
    @Deprecated(since = "4.1")
    public static Throwable deleteWithConfirm(File file, Throwable accumulate, RateLimiter rateLimiter)
    {
        return file.delete(accumulate, rateLimiter);
    }

    /** @deprecated See CASSANDRA-16926 */
    @Deprecated(since = "4.1")
    public static void deleteWithConfirm(String file)
    {
        deleteWithConfirm(new File(file));
    }

    /** @deprecated See CASSANDRA-16926 */
    @Deprecated(since = "4.1")
    public static void deleteWithConfirm(File file)
    {
        file.delete();
    }

    /** @deprecated See CASSANDRA-16926 */
    @Deprecated(since = "4.1")
    public static void renameWithOutConfirm(String from, String to)
    {
        new File(from).tryMove(new File(to));
    }

    /** @deprecated See CASSANDRA-16926 */
    @Deprecated(since = "4.1")
    public static void renameWithConfirm(String from, String to)
    {
        renameWithConfirm(new File(from), new File(to));
    }

    /** @deprecated See CASSANDRA-16926 */
    @Deprecated(since = "4.1")
    public static void renameWithConfirm(File from, File to)
    {
        from.move(to);
    }

    /**
     * Private constructor as the class contains only static methods.
     */
    private FileUtils()
    {
    }

    /**
     * Moves the contents of a directory to another directory.
     * <p>Once a file has been copied to the target directory it will be deleted from the source directory.
     * If a file already exists in the target directory a warning will be logged and the file will not
     * be deleted.</p>
     *
     * @param source the directory containing the files to move
     * @param target the directory where the files must be moved
     */
    public static void moveRecursively(Path source, Path target) throws IOException
    {
        logger.info("Moving {} to {}" , source, target);

        if (Files.isDirectory(source))
        {
            Files.createDirectories(target);

            for (File f : new File(source).tryList())
            {
                String fileName = f.name();
                moveRecursively(source.resolve(fileName), target.resolve(fileName));
            }

            deleteDirectoryIfEmpty(source);
        }
        else
        {
            if (Files.exists(target))
            {
                logger.warn("Cannot move the file {} to {} as the target file already exists." , source, target);
            }
            else
            {
                Files.copy(source, target, StandardCopyOption.COPY_ATTRIBUTES);
                Files.delete(source);
            }
        }
    }

    /**
     * Deletes the specified directory if it is empty
     *
     * @param path the path to the directory
     */
    public static void deleteDirectoryIfEmpty(Path path) throws IOException
    {
        Preconditions.checkArgument(Files.isDirectory(path), String.format("%s is not a directory", path));

        try
        {
            logger.info("Deleting directory {}", path);
            Files.delete(path);
        }
        catch (DirectoryNotEmptyException e)
        {
            try (Stream<Path> paths = Files.list(path))
            {
                String content = paths.map(p -> p.getFileName().toString()).collect(Collectors.joining(", "));

                logger.warn("Cannot delete the directory {} as it is not empty. (Content: {})", path, content);
            }
        }
    }
}