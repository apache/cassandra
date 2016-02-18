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
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import sun.nio.ch.DirectBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.BlacklistedDirectories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.io.FSError;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.JVMStabilityInspector;

import static org.apache.cassandra.utils.Throwables.maybeFail;
import static org.apache.cassandra.utils.Throwables.merge;

public class FileUtils
{
    public static final Charset CHARSET = StandardCharsets.UTF_8;

    private static final Logger logger = LoggerFactory.getLogger(FileUtils.class);
    private static final double KB = 1024d;
    private static final double MB = 1024*1024d;
    private static final double GB = 1024*1024*1024d;
    private static final double TB = 1024*1024*1024*1024d;

    private static final DecimalFormat df = new DecimalFormat("#.##");
    private static final boolean canCleanDirectBuffers;

    static
    {
        boolean canClean = false;
        try
        {
            ByteBuffer buf = ByteBuffer.allocateDirect(1);
            ((DirectBuffer) buf).cleaner().clean();
            canClean = true;
        }
        catch (Throwable t)
        {
            JVMStabilityInspector.inspectThrowable(t);
            logger.info("Cannot initialize un-mmaper.  (Are you using a non-Oracle JVM?)  Compacted data files will not be removed promptly.  Consider using an Oracle JVM or using standard disk access mode");
        }
        canCleanDirectBuffers = canClean;
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

    public static File createTempFile(String prefix, String suffix, File directory)
    {
        try
        {
            return File.createTempFile(prefix, suffix, directory);
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, directory);
        }
    }

    public static File createTempFile(String prefix, String suffix)
    {
        return createTempFile(prefix, suffix, new File(System.getProperty("java.io.tmpdir")));
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
            logger.trace((String.format("Renaming %s to %s", from.getPath(), to.getPath())));
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

    public static boolean isCleanerAvailable()
    {
        return canCleanDirectBuffers;
    }

    public static void clean(ByteBuffer buffer)
    {
        if (buffer == null)
            return;
        if (isCleanerAvailable() && buffer.isDirect())
        {
            DirectBuffer db = (DirectBuffer) buffer;
            if (db.cleaner() != null)
                db.cleaner().clean();
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

    public static String stringifyFileSize(double value)
    {
        double d;
        if ( value >= TB )
        {
            d = value / TB;
            String val = df.format(d);
            return val + " TiB";
        }
        else if ( value >= GB )
        {
            d = value / GB;
            String val = df.format(d);
            return val + " GiB";
        }
        else if ( value >= MB )
        {
            d = value / MB;
            String val = df.format(d);
            return val + " MiB";
        }
        else if ( value >= KB )
        {
            d = value / KB;
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

        logger.trace("Scheduling deferred deletion of file: " + dir);
        dir.deleteOnExit();
    }

    public static void handleCorruptSSTable(CorruptSSTableException e)
    {
        if (!StorageService.instance.isSetupCompleted())
            handleStartupFSError(e);

        JVMStabilityInspector.inspectThrowable(e);
        switch (DatabaseDescriptor.getDiskFailurePolicy())
        {
            case stop_paranoid:
                StorageService.instance.stopTransports();
                break;
        }
    }
    
    public static void handleFSError(FSError e)
    {
        if (!StorageService.instance.isSetupCompleted())
            handleStartupFSError(e);

        JVMStabilityInspector.inspectThrowable(e);
        switch (DatabaseDescriptor.getDiskFailurePolicy())
        {
            case stop_paranoid:
            case stop:
                StorageService.instance.stopTransports();
                break;
            case best_effort:
                // for both read and write errors mark the path as unwritable.
                BlacklistedDirectories.maybeMarkUnwritable(e.path);
                if (e instanceof FSReadError)
                {
                    File directory = BlacklistedDirectories.maybeMarkUnreadable(e.path);
                    if (directory != null)
                        Keyspace.removeUnreadableSSTables(directory);
                }
                break;
            case ignore:
                // already logged, so left nothing to do
                break;
            default:
                throw new IllegalStateException();
        }
    }

    private static void handleStartupFSError(Throwable t)
    {
        switch (DatabaseDescriptor.getDiskFailurePolicy())
        {
            case stop_paranoid:
            case stop:
            case die:
                logger.error("Exiting forcefully due to file system exception on startup, disk failure policy \"{}\"",
                             DatabaseDescriptor.getDiskFailurePolicy(),
                             t);
                JVMStabilityInspector.killCurrentJVM(t, true);
                break;
            default:
                break;
        }
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
            logger.error("Error while getting {} folder size. {}", folder, e);
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

    public static void write(File file, List<String> lines, StandardOpenOption ... options)
    {
        try
        {
            Files.write(file.toPath(),
                        lines,
                        CHARSET,
                        options);
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
}
