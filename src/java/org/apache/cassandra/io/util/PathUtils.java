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

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOError;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.openhft.chronicle.core.util.ThrowingFunction;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.io.FSError;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.utils.NoSpamLogger;

import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.util.Collections.unmodifiableSet;
import static org.apache.cassandra.config.CassandraRelevantProperties.USE_NIX_RECURSIVE_DELETE;
import static org.apache.cassandra.utils.Throwables.merge;

/**
 * Vernacular: tryX means return false or 0L on any failure; XIfNotY means propagate any exceptions besides those caused by Y
 *
 * This class tries to apply uniform IOException handling, and does not propagate IOException except for NoSuchFileException.
 * Any harmless/application error exceptions are propagated as UncheckedIOException, and anything else as an FSReadError or FSWriteError.
 * Semantically this is a little incoherent throughout the codebase, as we intercept IOException haphazardly and treaat
 * it inconsistently - we should ideally migrate to using {@link #propagate(IOException, Path, boolean)} et al globally.
 */
public final class PathUtils
{
    private static final boolean consistentDirectoryListings = CassandraRelevantProperties.CONSISTENT_DIRECTORY_LISTINGS.getBoolean();

    private static final Set<StandardOpenOption> READ_OPTIONS = unmodifiableSet(EnumSet.of(READ));
    private static final Set<StandardOpenOption> WRITE_OPTIONS = unmodifiableSet(EnumSet.of(WRITE, CREATE, TRUNCATE_EXISTING));
    private static final Set<StandardOpenOption> WRITE_APPEND_OPTIONS = unmodifiableSet(EnumSet.of(WRITE, CREATE, APPEND));
    private static final Set<StandardOpenOption> READ_WRITE_OPTIONS = unmodifiableSet(EnumSet.of(READ, WRITE, CREATE));
    private static final FileAttribute<?>[] NO_ATTRIBUTES = new FileAttribute[0];

    private static final Logger logger = LoggerFactory.getLogger(PathUtils.class);
    private static final NoSpamLogger nospam1m = NoSpamLogger.getLogger(logger, 1, TimeUnit.MINUTES);

    private static Consumer<Path> onDeletion = path -> {};

    public static FileChannel newReadChannel(Path path) throws NoSuchFileException
    {
        return newFileChannel(path, READ_OPTIONS);
    }

    public static FileChannel newReadWriteChannel(Path path) throws NoSuchFileException
    {
        return newFileChannel(path, READ_WRITE_OPTIONS);
    }

    public static FileChannel newWriteOverwriteChannel(Path path) throws NoSuchFileException
    {
        return newFileChannel(path, WRITE_OPTIONS);
    }

    public static FileChannel newWriteAppendChannel(Path path) throws NoSuchFileException
    {
        return newFileChannel(path, WRITE_APPEND_OPTIONS);
    }

    private static FileChannel newFileChannel(Path path, Set<StandardOpenOption> options) throws NoSuchFileException
    {
        try
        {
            return FileChannel.open(path, options, PathUtils.NO_ATTRIBUTES);
        }
        catch (IOException e)
        {
            throw propagateUncheckedOrNoSuchFileException(e, path, options.contains(WRITE));
        }
    }

    public static void setDeletionListener(Consumer<Path> newOnDeletion)
    {
        onDeletion = newOnDeletion;
    }

    public static String filename(Path path)
    {
        return path.getFileName().toString();
    }

    public static <T> T[] list(Path path, Function<Stream<Path>, Stream<T>> transform, IntFunction<T[]> arrayFactory)
    {
        try (Stream<Path> stream = Files.list(path))
        {
            return transform.apply(consistentDirectoryListings ? stream.sorted() : stream)
                    .toArray(arrayFactory);
        }
        catch (NoSuchFileException e)
        {
            return null;
        }
        catch (IOException e)
        {
            throw propagateUnchecked(e, path, false);
        }
    }

    public static <T extends Throwable, V> V[] tryList(Path path, Function<Stream<Path>, Stream<V>> transform, IntFunction<V[]> arrayFactory, ThrowingFunction<IOException, V[], T> orElse) throws T
    {
        try (Stream<Path> stream = Files.list(path))
        {
            return transform.apply(consistentDirectoryListings ? stream.sorted() : stream)
                    .toArray(arrayFactory);
        }
        catch (IOException e)
        {
            return orElse.apply(e);
        }
    }

    public static void forEach(Path path, Consumer<Path> forEach)
    {
        try (Stream<Path> stream = Files.list(path))
        {
            (consistentDirectoryListings ? stream.sorted() : stream).forEach(forEach);
        }
        catch (IOException e)
        {
            throw propagateUnchecked(e, path, false);
        }
    }

    public static void forEachRecursive(Path path, Consumer<Path> forEach)
    {
        Consumer<Path> forEachRecursive = new Consumer<Path>()
        {
            @Override
            public void accept(Path child)
            {
                forEach.accept(child);
                forEach(child, this);
            }
        };
        forEach(path, forEachRecursive);
    }

    public static long tryGetLength(Path path)
    {
        return tryOnPath(path, Files::size);
    }

    public static long tryGetLastModified(Path path)
    {
        return tryOnPath(path, p -> Files.getLastModifiedTime(p).toMillis());
    }

    public static boolean trySetLastModified(Path path, long lastModified)
    {
        try
        {
            Files.setLastModifiedTime(path, FileTime.fromMillis(lastModified));
            return true;
        }
        catch (IOException e)
        {
            return false;
        }
    }

    public static boolean trySetReadable(Path path, boolean readable)
    {
        return trySet(path, PosixFilePermission.OWNER_READ, readable);
    }

    public static boolean trySetWritable(Path path, boolean writeable)
    {
        return trySet(path, PosixFilePermission.OWNER_WRITE, writeable);
    }

    public static boolean trySetExecutable(Path path, boolean executable)
    {
        return trySet(path, PosixFilePermission.OWNER_EXECUTE, executable);
    }

    public static boolean trySet(Path path, PosixFilePermission permission, boolean set)
    {
        try
        {
            PosixFileAttributeView view = path.getFileSystem().provider().getFileAttributeView(path, PosixFileAttributeView.class);
            PosixFileAttributes attributes = view.readAttributes();
            Set<PosixFilePermission> permissions = attributes.permissions();
            if (set == permissions.contains(permission))
                return true;
            if (set) permissions.add(permission);
            else permissions.remove(permission);
            view.setPermissions(permissions);
            return true;
        }
        catch (IOException e)
        {
            return false;
        }
    }

    public static Throwable delete(Path file, Throwable accumulate)
    {
        try
        {
            delete(file);
        }
        catch (FSError t)
        {
            accumulate = merge(accumulate, t);
        }
        return accumulate;
    }

    public static void delete(Path file)
    {
        try
        {
            Files.delete(file);
            onDeletion.accept(file);
        }
        catch (IOException e)
        {
            throw propagateUnchecked(e, file, true);
        }
    }

    public static void deleteIfExists(Path file)
    {
        try
        {
            Files.delete(file);
            onDeletion.accept(file);
        }
        catch (IOException e)
        {
            if (e instanceof FileNotFoundException | e instanceof NoSuchFileException)
                return;

            throw propagateUnchecked(e, file, true);
        }
    }

    public static boolean tryDelete(Path file)
    {
        try
        {
            Files.delete(file);
            onDeletion.accept(file);
            return true;
        }
        catch (IOException e)
        {
            return false;
        }
    }

    public static void delete(Path file, @Nullable RateLimiter rateLimiter)
    {
        if (rateLimiter != null)
        {
            double throttled = rateLimiter.acquire();
            if (throttled > 0.0)
                nospam1m.warn("Throttling file deletion: waited {} seconds to delete {}", throttled, file);
        }
        delete(file);
    }

    public static Throwable delete(Path file, Throwable accumulate, @Nullable RateLimiter rateLimiter)
    {
        try
        {
            delete(file, rateLimiter);
        }
        catch (Throwable t)
        {
            accumulate = merge(accumulate, t);
        }
        return accumulate;
    }

    /**
     * Uses unix `rm -r` to delete a directory recursively.
     * Note that, it will trigger {@link #onDeletion} listener only for the provided path and will not call it for any
     * nested path. This method can be much faster than deleting files and directories recursively by traversing them
     * with Java. Though, we use it only for tests because it provides less information about the problem when something
     * goes wrong.
     *
     * @param path    path to be deleted
     * @param quietly if quietly, additional `-f` flag is added to the `rm` command so that it will not complain in case
     *                the provided path is missing
     */
    private static void deleteRecursiveUsingNixCommand(Path path, boolean quietly)
    {
        String [] cmd = new String[]{ "rm", quietly ? "-rdf" : "-rd", path.toAbsolutePath().toString() };
        try
        {
            if (!quietly && !Files.exists(path))
                throw new NoSuchFileException(path.toString());

            Process p = Runtime.getRuntime().exec(cmd);
            int result = p.waitFor();

            String out, err;
            try (BufferedReader outReader = new BufferedReader(new InputStreamReader(p.getInputStream()));
                 BufferedReader errReader = new BufferedReader(new InputStreamReader(p.getErrorStream())))
            {
                out = outReader.lines().collect(Collectors.joining("\n"));
                err = errReader.lines().collect(Collectors.joining("\n"));
            }

            if (result != 0 && Files.exists(path))
            {
                logger.error("{} returned:\nstdout:\n{}\n\nstderr:\n{}", Arrays.toString(cmd), out, err);
                throw new IOException(String.format("%s returned non-zero exit code: %d%nstdout:%n%s%n%nstderr:%n%s", Arrays.toString(cmd), result, out, err));
            }

            onDeletion.accept(path);
        }
        catch (IOException e)
        {
            throw propagateUnchecked(e, path, true);
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            throw new FSWriteError(e, path);
        }
    }

    /**
     * Deletes all files and subdirectories under "path".
     * @param path file to be deleted
     * @throws FSWriteError if any part of the tree cannot be deleted
     */
    public static void deleteRecursive(Path path)
    {
        if (USE_NIX_RECURSIVE_DELETE.getBoolean() && path.getFileSystem() == java.nio.file.FileSystems.getDefault())
        {
            deleteRecursiveUsingNixCommand(path, false);
            return;
        }

        if (isDirectory(path))
            forEach(path, PathUtils::deleteRecursive);

        // The directory is now empty, so now it can be smoked
        delete(path);
    }

    /**
     * Deletes all files and subdirectories under "path".
     * @param path file to be deleted
     * @throws FSWriteError if any part of the tree cannot be deleted
     */
    public static void deleteRecursive(Path path, RateLimiter rateLimiter)
    {
        if (USE_NIX_RECURSIVE_DELETE.getBoolean() && path.getFileSystem() == java.nio.file.FileSystems.getDefault())
        {
            deleteRecursiveUsingNixCommand(path, false);
            return;
        }

        deleteRecursive(path, rateLimiter, p -> deleteRecursive(p, rateLimiter));
    }

    /**
     * Deletes all files and subdirectories under "path".
     * @param path file to be deleted
     * @throws FSWriteError if any part of the tree cannot be deleted
     */
    private static void deleteRecursive(Path path, RateLimiter rateLimiter, Consumer<Path> deleteRecursive)
    {
        if (isDirectory(path))
            forEach(path, deleteRecursive);

        // The directory is now empty so now it can be smoked
        delete(path, rateLimiter);
    }

    /**
     * Schedules deletion of all file and subdirectories under "dir" on JVM shutdown.
     * @param dir Directory to be deleted
     */
    public synchronized static void deleteRecursiveOnExit(Path dir)
    {
        ON_EXIT.add(dir, true);
    }

    /**
     * Schedules deletion of the file only on JVM shutdown.
     * @param file File to be deleted
     */
    public synchronized static void deleteOnExit(Path file)
    {
        ON_EXIT.add(file, false);
    }

    public static boolean tryRename(Path from, Path to)
    {
        logger.trace("Renaming {} to {}", from, to);
        try
        {
            atomicMoveWithFallback(from, to);
            return true;
        }
        catch (IOException e)
        {
            logger.trace("Could not move file {} to {}", from, to, e);
            return false;
        }
    }

    public static void rename(Path from, Path to)
    {
        logger.trace("Renaming {} to {}", from, to);
        try
        {
            atomicMoveWithFallback(from, to);
        }
        catch (IOException e)
        {
            logger.trace("Could not move file {} to {}", from, to, e);

            // TODO: try to decide if is read or write? for now, have assumed write
            throw propagateUnchecked(String.format("Failed to rename %s to %s", from, to), e, to, true);
        }
    }

    /**
     * Move a file atomically, if it fails, it falls back to a non-atomic operation
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

    // true if can determine exists, false if any exception occurs
    public static boolean exists(Path path)
    {
        return Files.exists(path);
    }

    // true if can determine is a directory, false if any exception occurs
    public static boolean isDirectory(Path path)
    {
        return Files.isDirectory(path);
    }

    // true if can determine is a regular file, false if any exception occurs
    public static boolean isFile(Path path)
    {
        return Files.isRegularFile(path);
    }

    /**
     * @param path create file if not exists
     * @throws IOError if cannot perform the operation
     * @return true if a new file was created
     */
    public static boolean createFileIfNotExists(Path path)
    {
        return ifNotExists(path, Files::createFile);
    }

    /**
     * @param path create directory if not exists
     * @throws IOError if cannot perform the operation
     * @return true if a new directory was created
     */
    public static boolean createDirectoryIfNotExists(Path path)
    {
        return ifNotExists(path, Files::createDirectory);
    }

    /**
     * @param path create directory (and parents) if not exists
     * @throws IOError if cannot perform the operation
     * @return true if a new directory was created
     */
    public static boolean createDirectoriesIfNotExists(Path path)
    {
        return ifNotExists(path, Files::createDirectories);
    }

    /**
     * @param path create directory if not exists and action can be performed
     * @return true if a new directory was created, false otherwise (for any reason)
     */
    public static boolean tryCreateDirectory(Path path)
    {
        return tryConsume(path, Files::createDirectory);
    }

    /**
     * @param path create directory (and parents) if not exists and action can be performed
     * @return true if the new directory was created, false otherwise (for any reason)
     */
    public static boolean tryCreateDirectories(Path path)
    {
        if (exists(path))
            return false;

        tryCreateDirectories(path.toAbsolutePath().getParent());
        return tryCreateDirectory(path);
    }

    /**
     * @return file if exists, otherwise nearest parent that exists; null if nothing in path exists
     */
    public static Path findExistingAncestor(Path file)
    {
        if (!file.equals(file.normalize()))
            throw new IllegalArgumentException("Must be invoked on a path without redundant elements");

        Path parent = file;
        while (parent != null && !Files.exists(parent))
            parent = parent.getParent();
        return parent;
    }

    /**
     * 1) Convert to an absolute path without redundant path elements;
     * 2) If the file exists, resolve any links to the underlying fille;
     * 3) If the file does not exist, find the first ancestor that does and resolve the path from there
     */
    public static Path toCanonicalPath(Path file)
    {
        Preconditions.checkNotNull(file);

        file = file.toAbsolutePath().normalize();
        Path parent = findExistingAncestor(file);

        if (parent == null)
            return file;
        if (parent == file)
            return toRealPath(file);
        return toRealPath(parent).resolve(parent.relativize(file));
    }

    private static Path toRealPath(Path path)
    {
        try
        {
            return path.toRealPath();
        }
        catch (IOException e)
        {
            throw propagateUnchecked(e, path, false);
        }
    }

    /**
     * Return true if file's canonical path is contained in folder's canonical path.
     *
     * Propagates any exceptions encountered finding canonical paths.
     */
    public static boolean isContained(Path folder, Path file)
    {
        Path realFolder = toCanonicalPath(folder), realFile = toCanonicalPath(file);
        return realFile.startsWith(realFolder);
    }

    @VisibleForTesting
    static public void runOnExitThreadsAndClear()
    {
        DeleteOnExit.runOnExitThreadsAndClear();
    }

    static public void clearOnExitThreads()
    {
        DeleteOnExit.clearOnExitThreads();
    }


    private static final class DeleteOnExit implements Runnable
    {
        private boolean isRegistered;
        private final Set<Path> deleteRecursivelyOnExit = new HashSet<>();
        private final Set<Path> deleteOnExit = new HashSet<>();

        private static List<Thread> onExitThreads = new ArrayList<>();

        private static void runOnExitThreadsAndClear()
        {
            List<Thread> toRun;
            synchronized (onExitThreads)
            {
                toRun = new ArrayList<>(onExitThreads);
                onExitThreads.clear();
            }
            Runtime runtime = Runtime.getRuntime();
            toRun.forEach(onExitThread -> {
                try
                {
                    runtime.removeShutdownHook(onExitThread);
                    //noinspection CallToThreadRun
                    onExitThread.run();
                }
                catch (Exception ex)
                {
                    logger.warn("Exception thrown when cleaning up files to delete on exit, continuing.", ex);
                }
            });
        }

        private static void clearOnExitThreads()
        {
            synchronized (onExitThreads)
            {
                Runtime runtime = Runtime.getRuntime();
                onExitThreads.forEach(runtime::removeShutdownHook);
                onExitThreads.clear();
            }
        }

        DeleteOnExit()
        {
            final Thread onExitThread = new Thread(this); // checkstyle: permit this instantiation
            synchronized (onExitThreads)
            {
                onExitThreads.add(onExitThread);
            }
            Runtime.getRuntime().addShutdownHook(onExitThread);
        }

        synchronized void add(Path path, boolean recursive)
        {
            if (!isRegistered)
            {
                isRegistered = true;
            }
            logger.trace("Scheduling deferred {}deletion of file: {}", recursive ? "recursive " : "", path);
            (recursive ? deleteRecursivelyOnExit : deleteOnExit).add(path);
        }

        public void run()
        {
            for (Path path : deleteOnExit)
            {
                try
                {
                    if (exists(path))
                        delete(path);
                }
                catch (Throwable t)
                {
                    logger.warn("Failed to delete {} on exit", path, t);
                }
            }
            for (Path path : deleteRecursivelyOnExit)
            {
                try
                {
                    if (exists(path))
                        deleteRecursive(path);
                }
                catch (Throwable t)
                {
                    logger.warn("Failed to delete {} on exit", path, t);
                }
            }
        }
    }
    private static final DeleteOnExit ON_EXIT = new DeleteOnExit();

    public interface IOConsumer { void accept(Path path) throws IOException; }
    public interface IOToLongFunction<V> { long apply(V path) throws IOException; }

    private static boolean ifNotExists(Path path, IOConsumer consumer)
    {
        try
        {
            consumer.accept(path);
            return true;
        }
        catch (FileAlreadyExistsException fae)
        {
            return false;
        }
        catch (IOException e)
        {
            throw propagateUnchecked(e, path, true);
        }
    }

    private static boolean tryConsume(Path path, IOConsumer function)
    {
        try
        {
            function.accept(path);
            return true;
        }
        catch (IOException e)
        {
            return false;
        }
    }

    private static long tryOnPath(Path path, IOToLongFunction<Path> function)
    {
        try
        {
            return function.apply(path);
        }
        catch (IOException e)
        {
            return 0L;
        }
    }

    private static long tryOnFileStore(Path path, IOToLongFunction<FileStore> function)
    {
        return tryOnFileStore(path, function, ignore -> {});
    }

    private static long tryOnFileStore(Path path, IOToLongFunction<FileStore> function, Consumer<IOException> orElse)
    {
        try
        {
            Path ancestor = findExistingAncestor(path.toAbsolutePath().normalize());
            if (ancestor == null)
            {
                orElse.accept(new NoSuchFileException(path.toString()));
                return 0L;
            }
            return function.apply(Files.getFileStore(ancestor));
        }
        catch (IOException e)
        {
            orElse.accept(e);
            return 0L;
        }
    }

    /**
     * Returns the number of bytes (determined by the provided MethodHandle) on the specified partition.
     * <p>This method handles large file system by returning {@code Long.MAX_VALUE} if the  number of available bytes
     * overflow. See <a href='https://bugs.openjdk.java.net/browse/JDK-8179320'>JDK-8179320</a> for more information</p>
     *
     * @param path the partition (or a file within it)
     */
    public static long tryGetSpace(Path path, IOToLongFunction<FileStore> getSpace)
    {
        return handleLargeFileSystem(tryOnFileStore(path, getSpace));
    }

    public static long tryGetSpace(Path path, IOToLongFunction<FileStore> getSpace, Consumer<IOException> orElse)
    {
        return handleLargeFileSystem(tryOnFileStore(path, getSpace, orElse));
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
    private PathUtils()
    {
    }

    /**
     * propagate an IOException as an FSWriteError, FSReadError or UncheckedIOException
     */
    public static RuntimeException propagateUnchecked(IOException ioe, Path path, boolean write)
    {
        return propagateUnchecked(null, ioe, path, write);
    }

    /**
     * propagate an IOException as an FSWriteError, FSReadError or UncheckedIOException
     */
    public static RuntimeException propagateUnchecked(String message, IOException ioe, Path path, boolean write)
    {
        if (ioe instanceof FileAlreadyExistsException
            || ioe instanceof NoSuchFileException
            || ioe instanceof AtomicMoveNotSupportedException
            || ioe instanceof java.nio.file.DirectoryNotEmptyException
            || ioe instanceof java.nio.file.FileSystemLoopException
            || ioe instanceof java.nio.file.NotDirectoryException
            || ioe instanceof java.nio.file.NotLinkException)
            throw new UncheckedIOException(message, ioe);

        if (write) throw new FSWriteError(message, ioe, path);
        else throw new FSReadError(message, ioe, path);
    }

    /**
     * propagate an IOException as an FSWriteError, FSReadError or UncheckedIOException - except for NoSuchFileException
     */
    public static NoSuchFileException propagateUncheckedOrNoSuchFileException(IOException ioe, Path path, boolean write) throws NoSuchFileException
    {
        if (ioe instanceof NoSuchFileException)
            throw (NoSuchFileException) ioe;

        throw propagateUnchecked(ioe, path, write);
    }

    /**
     * propagate an IOException either as itself or an FSWriteError or FSReadError
     */
    public static <E extends IOException> E propagate(E ioe, Path path, boolean write) throws E
    {
        if (ioe instanceof FileAlreadyExistsException
            || ioe instanceof NoSuchFileException
            || ioe instanceof AtomicMoveNotSupportedException
            || ioe instanceof java.nio.file.DirectoryNotEmptyException
            || ioe instanceof java.nio.file.FileSystemLoopException
            || ioe instanceof java.nio.file.NotDirectoryException
            || ioe instanceof java.nio.file.NotLinkException)
            throw ioe;

        if (write) throw new FSWriteError(ioe, path);
        else throw new FSReadError(ioe, path);
    }
}
