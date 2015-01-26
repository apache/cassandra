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
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.text.DecimalFormat;
import java.util.Arrays;

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

public class FileUtils
{
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

    public static void deleteWithConfirm(String file)
    {
        deleteWithConfirm(new File(file));
    }

    public static void deleteWithConfirm(File file)
    {
        assert file.exists() : "attempted to delete non-existing file " + file.getName();
        if (logger.isDebugEnabled())
            logger.debug("Deleting {}", file.getName());
        try
        {
            Files.delete(file.toPath());
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, file);
        }
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
        if (logger.isDebugEnabled())
            logger.debug((String.format("Renaming %s to %s", from.getPath(), to.getPath())));
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
            logger.debug("Could not do an atomic move", e);
            Files.move(from, to, StandardCopyOption.REPLACE_EXISTING);
        }

    }
    public static void truncate(String path, long size)
    {
        RandomAccessFile file;

        try
        {
            file = new RandomAccessFile(path, "rw");
        }
        catch (FileNotFoundException e)
        {
            throw new RuntimeException(e);
        }

        try
        {
            file.getChannel().truncate(size);
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, path);
        }
        finally
        {
            closeQuietly(file);
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

    public static boolean isCleanerAvailable()
    {
        return canCleanDirectBuffers;
    }

    public static void clean(ByteBuffer buffer)
    {
        if (isCleanerAvailable() && buffer.isDirect())
            ((DirectBuffer)buffer).cleaner().clean();
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
            return val + " TB";
        }
        else if ( value >= GB )
        {
            d = value / GB;
            String val = df.format(d);
            return val + " GB";
        }
        else if ( value >= MB )
        {
            d = value / MB;
            String val = df.format(d);
            return val + " MB";
        }
        else if ( value >= KB )
        {
            d = value / KB;
            String val = df.format(d);
            return val + " KB";
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

    public static void skipBytesFully(DataInput in, int bytes) throws IOException
    {
        int n = 0;
        while (n < bytes)
        {
            int skipped = in.skipBytes(bytes - n);
            if (skipped == 0)
                throw new EOFException("EOF after " + n + " bytes out of " + bytes);
            n += skipped;
        }
    }

    public static void handleCorruptSSTable(CorruptSSTableException e)
    {
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

    /**
     * Get the size of a directory in bytes
     * @param directory The directory for which we need size.
     * @return The size of the directory
     */
    public static long folderSize(File directory)
    {
        long length = 0;
        for (File file : directory.listFiles())
        {
            if (file.isFile())
                length += file.length();
            else
                length += folderSize(file);
        }
        return length;
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
}
