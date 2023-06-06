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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths; // checkstyle: permit this import
import java.util.Objects;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.stream.Stream;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.RateLimiter;

import net.openhft.chronicle.core.util.ThrowingFunction;
import org.apache.cassandra.io.FSWriteError;

import static org.apache.cassandra.io.util.PathUtils.filename;
import static org.apache.cassandra.utils.Throwables.maybeFail;

/**
 * A thin wrapper around java.nio.file.Path to provide more ergonomic functionality.
 *
 * TODO codebase probably should not use tryList, as unexpected exceptions are hidden;
 *      probably want to introduce e.g. listIfExists
 */
public class File implements Comparable<File>
{
    private static FileSystem filesystem = FileSystems.getDefault();

    public enum WriteMode { OVERWRITE, APPEND }

    public static String pathSeparator()
    {
        return filesystem.getSeparator();
    }

    @Nullable final Path path; // nullable to support concept of empty path, that resolves to the working directory if converted to an absolute path

    /**
     * Construct a File representing the child {@code child} of {@code parent}
     */
    public File(String parent, String child)
    {
        this(parent.isEmpty() ? null : filesystem.getPath(parent), child);
    }

    /**
     * Construct a File representing the child {@code child} of {@code parent}
     */
    public File(File parent, String child)
    {
        this(parent.path, child);
    }

    /**
     * Construct a File representing the child {@code child} of {@code parent}
     */
    public File(Path parent, String child)
    {
        // if "empty abstract path" (a la java.io.File) is provided, we should behave as though resolving relative path
        if (child.startsWith(pathSeparator()))
            child = child.substring(pathSeparator().length());
        this.path = parent == null ? filesystem.getPath(child) : parent.resolve(child);
    }

    /**
     * Construct a File representing the provided {@code path}
     */
    public File(String path)
    {
        this(path.isEmpty() ? null : filesystem.getPath(path));
    }

    /**
     * Create a File equivalent to the java.io.File provided
     */
    public File(java.io.File file)
    {
        this(file.getPath().isEmpty() ? null : file.toPath());
    }

    /**
     * Construct a File representing the child {@code child} of {@code parent}
     */
    public File(java.io.File parent, String child)
    {
        this(new File(parent), child);
    }

    /**
     * Convenience constructor equivalent to {@code new File(Paths.get(path))}
     */
    public File(URI path)
    {
        this(Paths.get(path)); //TODO unsafe if uri is file:// as it uses default file system and not File.filesystem
        if (!path.isAbsolute() || path.isOpaque()) throw new IllegalArgumentException();
    }

    /**
     * Unsafe constructor that allows a File to use a differet {@link FileSystem} than {@link File#filesystem}.
     *
     * The main caller of such a method are cases such as JVM Dtest functions that need access to the logging framwork
     * files, which exists on in {@link FileSystems#getDefault()}.
     */
    @VisibleForTesting
    public File(FileSystem fs, String first, String... more)
    {
        this.path = fs.getPath(first, more);
    }

    /**
     * @param path the path to wrap
     */
    public File(Path path)
    {
        if (path != null && path.getFileSystem() != filesystem)
            throw new IllegalArgumentException("Incompatible file system; path FileSystem (" + path.getFileSystem() + ") is not the same reference (" + filesystem + ")");

        this.path = path;
    }

    public static Path getPath(String first, String... more)
    {
        return filesystem.getPath(first, more);
    }

    /**
     * Try to delete the file, returning true iff it was deleted by us. Does not ordinarily throw exceptions.
     */
    public boolean tryDelete()
    {
        return path != null && PathUtils.tryDelete(path);
    }

    /**
     * This file will be deleted, and any exceptions encountered merged with {@code accumulate} to the return value
     */
    public Throwable delete(Throwable accumulate)
    {
        return delete(accumulate, null);
    }

    /**
     * This file will be deleted, obeying the provided rate limiter.
     * Any exceptions encountered will be merged with {@code accumulate} to the return value
     */
    public Throwable delete(Throwable accumulate, RateLimiter rateLimiter)
    {
        return PathUtils.delete(toPathForWrite(), accumulate, rateLimiter);
    }

    /**
     * This file will be deleted, with any failures being reported with an FSError
     * @throws FSWriteError if cannot be deleted
     */
    public void delete()
    {
        maybeFail(delete(null, null));
    }

    /**
     * This file will be deleted, with any failures being reported with an FSError
     * @throws FSWriteError if cannot be deleted
     */
    public void deleteIfExists()
    {
        if (path != null)
            PathUtils.deleteIfExists(path);
    }

    /**
     * This file will be deleted, obeying the provided rate limiter.
     * @throws FSWriteError if cannot be deleted
     */
    public void delete(RateLimiter rateLimiter)
    {
        maybeFail(delete(null, rateLimiter));
    }

    /**
     * Deletes all files and subdirectories under "dir".
     * @throws FSWriteError if any part of the tree cannot be deleted
     */
    public void deleteRecursive(RateLimiter rateLimiter)
    {
        PathUtils.deleteRecursive(toPathForWrite(), rateLimiter);
    }

    /**
     * Deletes all files and subdirectories under "dir".
     * @throws FSWriteError if any part of the tree cannot be deleted
     */
    public void deleteRecursive()
    {
        PathUtils.deleteRecursive(toPathForWrite());
    }

    /**
     * Try to delete the file on process exit.
     */
    public void deleteOnExit()
    {
        if (path != null) PathUtils.deleteOnExit(path);
    }

    /**
     * This file will be deleted on clean shutdown; if it is a directory, its entire contents
     * <i>at the time of shutdown</i> will be deleted
     */
    public void deleteRecursiveOnExit()
    {
        if (path != null)
            PathUtils.deleteRecursiveOnExit(path);
    }

    /**
     * Try to rename the file atomically, if the system supports it.
     * @return true iff successful, false if it fails for any reason.
     */
    public boolean tryMove(File to)
    {
        return path != null && PathUtils.tryRename(path, to.path);
    }

    /**
     * Atomically (if supported) rename/move this file to {@code to}
     * @throws FSWriteError if any part of the tree cannot be deleted
     */
    public void move(File to)
    {
        PathUtils.rename(toPathForRead(), to.toPathForWrite());
    }

    /**
     * @return the length of the file if it exists and if we can read it; 0 otherwise.
     */
    public long length()
    {
        return path == null ? 0L : PathUtils.tryGetLength(path);
    }

    /**
     * @return the last modified time in millis of the path if it exists and we can read it; 0 otherwise.
     */
    public long lastModified()
    {
        return path == null ? 0L : PathUtils.tryGetLastModified(path);
    }

    /**
     * Try to set the last modified time in millis of the path
     * @return true if it exists and we can write it; return false otherwise.
     */
    public boolean trySetLastModified(long value)
    {
        return path != null && PathUtils.trySetLastModified(path, value);
    }

    /**
     * Try to set if the path is readable by its owner
     * @return true if it exists and we can write it; return false otherwise.
     */
    public boolean trySetReadable(boolean value)
    {
        return path != null && PathUtils.trySetReadable(path, value);
    }

    /**
     * Try to set if the path is writable by its owner
     * @return true if it exists and we can write it; return false otherwise.
     */
    public boolean trySetWritable(boolean value)
    {
        return path != null && PathUtils.trySetWritable(path, value);
    }

    /**
     * Try to set if the path is executable by its owner
     * @return true if it exists and we can write it; return false otherwise.
     */
    public boolean trySetExecutable(boolean value)
    {
        return path != null && PathUtils.trySetExecutable(path, value);
    }

    /**
     * @return true if the path exists, false if it does not, or we cannot determine due to some exception
     */
    public boolean exists()
    {
        return path != null && PathUtils.exists(path);
    }

    /**
     * @return true if the path refers to a directory
     */
    public boolean isDirectory()
    {
        return path != null && PathUtils.isDirectory(path);
    }

    /**
     * @return true if the path refers to a regular file
     */
    public boolean isFile()
    {
        return path != null && PathUtils.isFile(path);
    }

    /**
     * @return true if the path can be read by us
     */
    public boolean isReadable()
    {
        return path != null && Files.isReadable(path);
    }

    /**
     * @return true if the path can be written by us
     */
    public boolean isWritable()
    {
        return path != null && Files.isWritable(path);
    }

    /**
     * @return true if the path can be executed by us
     */
    public boolean isExecutable()
    {
        return path != null && Files.isExecutable(path);
    }

    /**
     * Try to create a new regular file at this path.
     * @return true if successful, false if it already exists
     */
    public boolean createFileIfNotExists()
    {
        return PathUtils.createFileIfNotExists(toPathForWrite());
    }

    public boolean createDirectoriesIfNotExists()
    {
        return PathUtils.createDirectoriesIfNotExists(toPathForWrite());
    }

    /**
     * Try to create a directory at this path.
     * Return true if a new directory was created at this path, and false otherwise.
     */
    public boolean tryCreateDirectory()
    {
        return path != null && PathUtils.tryCreateDirectory(path);
    }

    /**
     * Try to create a directory at this path, creating any parent directories as necessary.
     * @return true if a new directory was created at this path, and false otherwise.
     */
    public boolean tryCreateDirectories()
    {
        return path != null && PathUtils.tryCreateDirectories(path);
    }

    /**
     * @return the parent file, or null if none
     */
    public File parent()
    {
        if (path == null) return null;
        Path parent = path.getParent();
        if (parent == null) return null;
        return new File(parent);
    }

    /**
     * @return the parent file's path, or null if none
     */
    public String parentPath()
    {
        File parent = parent();
        return parent == null ? null : parent.toString();
    }

    /**
     * @return true if the path has no relative path elements
     */
    public boolean isAbsolute()
    {
        return path != null && path.isAbsolute();
    }

    public boolean isAncestorOf(File child)
    {
        return PathUtils.isContained(toPath(), child.toPath());
    }

    /**
     * @return a File that represents the same path as this File with any relative path elements resolved.
     *         If this is the empty File, returns the working directory.
     */
    public File toAbsolute()
    {
        return new File(toPath().toAbsolutePath());
    }

    /** {@link #toAbsolute} */
    public String absolutePath()
    {
        return toPath().toAbsolutePath().toString();
    }

    /**
     * @return a File that represents the same path as this File with any relative path elements and links resolved.
     *         If this is the empty File, returns the working directory.
     */
    public File toCanonical()
    {
        Path canonical = PathUtils.toCanonicalPath(toPath());
        return canonical == path ? this : new File(canonical);
    }

    /** {@link #toCanonical} */
    public String canonicalPath()
    {
        return toCanonical().toString();
    }

    /**
     * @return the last path element for this file
     */
    public String name()
    {
        return path == null ? "" : filename(path);
    }

    public void forEach(Consumer<File> forEach)
    {
        PathUtils.forEach(path, path -> forEach.accept(new File(path)));
    }

    public void forEachRecursive(Consumer<File> forEach)
    {
        PathUtils.forEachRecursive(path, path -> forEach.accept(new File(path)));
    }

    private static <V> ThrowingFunction<IOException, V, RuntimeException> nulls() { return ignore -> null; }
    private static <V> ThrowingFunction<IOException, V, IOException> rethrow()
    {
        return fail -> {
            if (fail == null) throw new FileNotFoundException();
            throw fail;
        };
    }
    private static <V> ThrowingFunction<IOException, V, UncheckedIOException> unchecked()
    {
        return fail -> {
            if (fail == null) fail = new FileNotFoundException();
            throw new UncheckedIOException(fail);
        };
    }


    /**
     * @return if a directory, the names of the files within; null otherwise
     */
    public String[] tryListNames()
    {
        return tryListNames(nulls());
    }

    /**
     * @return if a directory, the names of the files within, filtered by the provided predicate; null otherwise
     */
    public String[] tryListNames(BiPredicate<File, String> filter)
    {
        return tryListNames(filter, nulls());
    }

    /**
     * @return if a directory, the files within; null otherwise
     */
    public File[] tryList()
    {
        return tryList(nulls());
    }

    /**
     * @return if a directory, the files within, filtered by the provided predicate; null otherwise
     */
    public File[] tryList(Predicate<File> filter)
    {
        return tryList(filter, nulls());
    }

    /**
     * @return if a directory, the files within, filtered by the provided predicate; null otherwise
     */
    public File[] tryList(BiPredicate<File, String> filter)
    {
        return tryList(filter, nulls());
    }

    /**
     * @return if a directory, the names of the files within; null otherwise
     */
    public String[] listNames() throws IOException
    {
        return tryListNames(rethrow());
    }

    /**
     * @return if a directory, the names of the files within, filtered by the provided predicate; null otherwise
     */
    public String[] listNames(BiPredicate<File, String> filter) throws IOException
    {
        return tryListNames(filter, rethrow());
    }

    /**
     * @return if a directory, the files within; null otherwise
     */
    public File[] list() throws IOException
    {
        return tryList(rethrow());
    }

    /**
     * @return if a directory, the files within, filtered by the provided predicate; null otherwise
     */
    public File[] list(Predicate<File> filter) throws IOException
    {
        return tryList(filter, rethrow());
    }

    /**
     * @return if a directory, the files within, filtered by the provided predicate; null otherwise
     */
    public File[] list(BiPredicate<File, String> filter) throws IOException
    {
        return tryList(filter, rethrow());
    }

    /**
     * @return if a directory, the names of the files within; null otherwise
     */
    public String[] listNamesUnchecked() throws UncheckedIOException
    {
        return tryListNames(unchecked());
    }

    /**
     * @return if a directory, the names of the files within, filtered by the provided predicate; null otherwise
     */
    public String[] listNamesUnchecked(BiPredicate<File, String> filter) throws UncheckedIOException
    {
        return tryListNames(filter, unchecked());
    }

    /**
     * @return if a directory, the files within; null otherwise
     */
    public File[] listUnchecked() throws UncheckedIOException
    {
        return tryList(unchecked());
    }

    /**
     * @return if a directory, the files within, filtered by the provided predicate; null otherwise
     */
    public File[] listUnchecked(Predicate<File> filter) throws UncheckedIOException
    {
        return tryList(filter, unchecked());
    }

    /**
     * @return if a directory, the files within, filtered by the provided predicate; throw an UncheckedIO exception otherwise
     */
    public File[] listUnchecked(BiPredicate<File, String> filter) throws UncheckedIOException
    {
        return tryList(filter, unchecked());
    }

    /**
     * @return if a directory, the names of the files within; null otherwise
     */
    public <T extends Throwable> String[] tryListNames(ThrowingFunction<IOException, String[], T> orElse) throws T
    {
        return tryListNames(path, Function.identity(), orElse);
    }

    /**
     * @return if a directory, the names of the files within, filtered by the provided predicate; null otherwise
     */
    public <T extends Throwable> String[] tryListNames(BiPredicate<File, String> filter, ThrowingFunction<IOException, String[], T> orElse) throws T
    {
        return tryList(path, stream -> stream.map(PathUtils::filename).filter(filename -> filter.test(this, filename)), String[]::new, orElse);
    }

    /**
     * @return if a directory, the files within; null otherwise
     */
    private <T extends Throwable> File[] tryList(ThrowingFunction<IOException, File[], T> orElse) throws T
    {
        return tryList(path, Function.identity(), orElse);
    }

    /**
     * @return if a directory, the files within, filtered by the provided predicate; null otherwise
     */
    private <T extends Throwable> File[] tryList(Predicate<File> filter, ThrowingFunction<IOException, File[], T> orElse) throws T
    {
        return tryList(path, stream -> stream.filter(filter), orElse);
    }

    /**
     * @return if a directory, the files within, filtered by the provided predicate; null otherwise
     */
    private <T extends Throwable> File[] tryList(BiPredicate<File, String> filter, ThrowingFunction<IOException, File[], T> orElse) throws T
    {
        return tryList(path, stream -> stream.filter(file -> filter.test(this, file.name())), orElse);
    }

    private static <T extends Throwable> String[] tryListNames(Path path, Function<Stream<File>, Stream<File>> toFiles, ThrowingFunction<IOException, String[], T> orElse) throws T
    {
        if (path == null)
            return orElse.apply(null);
        return PathUtils.tryList(path, stream -> toFiles.apply(stream.map(File::new)).map(File::name), String[]::new, orElse);
    }

    private static <T extends Throwable, V> V[] tryList(Path path, Function<Stream<Path>, Stream<V>> transformation, IntFunction<V[]> constructor, ThrowingFunction<IOException, V[], T> orElse) throws T
    {
        if (path == null)
            return orElse.apply(null);
        return PathUtils.tryList(path, transformation, constructor, orElse);
    }

    private static <T extends Throwable> File[] tryList(Path path, Function<Stream<File>, Stream<File>> toFiles, ThrowingFunction<IOException, File[], T> orElse) throws T
    {
        if (path == null)
            return orElse.apply(null);
        return PathUtils.tryList(path, stream -> toFiles.apply(stream.map(File::new)), File[]::new, orElse);
    }

    /**
     * @return the path of this file
     */
    public String path()
    {
        return toString();
    }

    /**
     * @return the {@link Path} of this file
     */
    public Path toPath()
    {
        return path == null ? filesystem.getPath("") : path;
    }

    /**
     * @return the path of this file
     */
    @Override
    public String toString()
    {
        return path == null ? "" : path.toString();
    }

    @Override
    public int hashCode()
    {
        return path == null ? 0 : path.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        return obj instanceof File && Objects.equals(path, ((File) obj).path);
    }

    @Override
    public int compareTo(File that)
    {
        if (this.path == null || that.path == null)
            return this.path == null && that.path == null ? 0 : this.path == null ? -1 : 1;
        return this.path.compareTo(that.path);
    }

    public java.io.File toJavaIOFile()
    {
        return path == null ? new java.io.File("") // checkstyle: permit this instantiation
                            : path.toFile(); // checkstyle: permit this invocation
    }

    /**
     * @return a new {@link FileChannel} for reading
     */
    public FileChannel newReadChannel() throws NoSuchFileException
    {
        return PathUtils.newReadChannel(toPathForRead());
    }

    /**
     * @return a new {@link FileChannel} for reading or writing; file will be created if it doesn't exist
     */
    public FileChannel newReadWriteChannel() throws NoSuchFileException
    {
        return PathUtils.newReadWriteChannel(toPathForRead());
    }

    /**
     * @param mode whether or not the channel appends to the underlying file
     * @return a new {@link FileChannel} for writing; file will be created if it doesn't exist
     */
    public FileChannel newWriteChannel(WriteMode mode) throws NoSuchFileException
    {
        switch (mode)
        {
            default: throw new AssertionError();
            case APPEND: return PathUtils.newWriteAppendChannel(toPathForWrite());
            case OVERWRITE: return PathUtils.newWriteOverwriteChannel(toPathForWrite());
        }
    }

    public FileWriter newWriter(WriteMode mode) throws IOException
    {
        return new FileWriter(this, mode);
    }

    public FileOutputStreamPlus newOutputStream(WriteMode mode) throws NoSuchFileException
    {
        return new FileOutputStreamPlus(this, mode);
    }

    public FileInputStreamPlus newInputStream() throws NoSuchFileException
    {
        return new FileInputStreamPlus(this);
    }

    public File withSuffix(String suffix)
    {
        if (path == null)
            throw new IllegalStateException("Cannot suffix an empty path");
        return new File(path.getParent().resolve(path.getFileName().toString() + suffix));
    }

    private Path toPathForWrite()
    {
        if (path == null)
            throw new IllegalStateException("Cannot write to an empty path");
        return path;
    }

    private Path toPathForRead()
    {
        if (path == null)
            throw new IllegalStateException("Cannot read from an empty path");
        return path;
    }

    @VisibleForTesting
    public static FileSystem unsafeGetFilesystem()
    {
        return filesystem;
    }

    public static void unsafeSetFilesystem(FileSystem fs)
    {
        filesystem = fs;
    }
}

