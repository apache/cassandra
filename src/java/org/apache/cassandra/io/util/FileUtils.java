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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.Comparator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.CLibrary;

public class FileUtils
{
    private static final Logger logger = LoggerFactory.getLogger(FileUtils.class);
    private static final double KB = 1024d;
    private static final double MB = 1024*1024d;
    private static final double GB = 1024*1024*1024d;
    private static final double TB = 1024*1024*1024*1024d;

    private static final DecimalFormat df = new DecimalFormat("#.##");

    private static final Method cleanerMethod;

    static
    {
        Method m;
        try
        {
            m = Class.forName("sun.nio.ch.DirectBuffer").getMethod("cleaner");
        }
        catch (Exception e)
        {
            // Perhaps a non-sun-derived JVM - contributions welcome
            logger.info("Cannot initialize un-mmaper.  (Are you using a non-SUN JVM?)  Compacted data files will not be removed promptly.  Consider using a SUN JVM or using standard disk access mode");
            m = null;
        }
        cleanerMethod = m;
    }

    public static void createHardLink(File from, File to)
    {
        if (to.exists())
            throw new RuntimeException("Tried to create duplicate hard link to " + to);
        if (!from.exists())
            throw new RuntimeException("Tried to hard link to file that does not exist " + from);

        try
        {
            CLibrary.createHardLink(from, to);
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
            logger.debug("Deleting " + file.getName());
        if (!file.delete())
            throw new FSWriteError(new IOException("Failed to delete " + file.getAbsolutePath()), file);
    }

    public static void renameWithOutConfirm(String from, String to)
    {
        new File(from).renameTo(new File(to));
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
        if (!from.renameTo(to))
            throw new RuntimeException(String.format("Failed to rename %s to %s", from.getPath(), to.getPath()));
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
            logger.warn("Failed closing " + c, e);
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
                logger.warn("Failed closing stream " + c, ex);
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
        return cleanerMethod != null;
    }

    public static void clean(MappedByteBuffer buffer)
    {
        try
        {
            Object cleaner = cleanerMethod.invoke(buffer);
            cleaner.getClass().getMethod("clean").invoke(cleaner);
        }
        catch (IllegalAccessException e)
        {
            throw new RuntimeException(e);
        }
        catch (InvocationTargetException e)
        {
            throw new RuntimeException(e);
        }
        catch (NoSuchMethodException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static class FileComparator implements Comparator<File>
    {
        public int compare(File f, File f2)
        {
            return (int)(f.lastModified() - f2.lastModified());
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

    public static void delete(File[] files)
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
        StorageService.tasks.execute(runnable);
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

    public static void skipBytesFully(DataInput in, long bytes) throws IOException
    {
        long n = 0;
        while (n < bytes)
        {
            int m = (int) Math.min(Integer.MAX_VALUE, bytes - n);
            int skipped = in.skipBytes(m);
            if (skipped == 0)
                throw new EOFException("EOF after " + n + " bytes out of " + bytes);
            n += skipped;
        }
    }
}
