/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.utils;

import java.io.*;
import java.lang.reflect.Field;
import java.nio.MappedByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.config.Config;

import com.google.common.base.Preconditions;

/*
 * A wrapper around various mechanisms for syncing files that makes it possible it intercept
 * and skip syncing. Useful for unit tests in certain environments where syncs can have outliers
 * bad enough to causes tests to run 10s of seconds longer.
 */
public class SyncUtil
{
    public static boolean SKIP_SYNC = Boolean.getBoolean(Config.PROPERTY_PREFIX + "skip_sync");

    private static final Field mbbFDField;
    private static final Field fdClosedField;
    private static final Field fdUseCountField;

    static
    {
        Field mbbFDFieldTemp = null;
        try
        {
            mbbFDFieldTemp = MappedByteBuffer.class.getDeclaredField("fd");
            mbbFDFieldTemp.setAccessible(true);
        }
        catch (NoSuchFieldException e)
        {
        }
        mbbFDField = mbbFDFieldTemp;

        //Java 8
        Field fdClosedFieldTemp = null;
        try
        {
            fdClosedFieldTemp = FileDescriptor.class.getDeclaredField("closed");
            fdClosedFieldTemp.setAccessible(true);
        }
        catch (NoSuchFieldException e)
        {
        }
        fdClosedField = fdClosedFieldTemp;

        //Java 7
        Field fdUseCountTemp = null;
        try
        {
            fdUseCountTemp = FileDescriptor.class.getDeclaredField("useCount");
            fdUseCountTemp.setAccessible(true);
        }
        catch (NoSuchFieldException e)
        {
        }
        fdUseCountField = fdUseCountTemp;
    }

    public static MappedByteBuffer force(MappedByteBuffer buf)
    {
        Preconditions.checkNotNull(buf);
        if (SKIP_SYNC)
        {
            Object fd = null;
            try
            {
                if (mbbFDField != null)
                {
                    fd = mbbFDField.get(buf);
                }
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
            //This is what MappedByteBuffer.force() throws if a you call force() on an umapped buffer
            if (mbbFDField != null && fd == null)
                throw new UnsupportedOperationException();
            return buf;
        }
        else
        {
            return buf.force();
        }
    }

    public static void sync(FileDescriptor fd) throws SyncFailedException
    {
        Preconditions.checkNotNull(fd);
        if (SKIP_SYNC)
        {
            boolean closed = false;
            try
            {
                if (fdClosedField != null)
                    closed = fdClosedField.getBoolean(fd);
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }

            int useCount = 1;
            try
            {
                if (fdUseCountField != null)
                    useCount = ((AtomicInteger)fdUseCountField.get(fd)).get();
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
            if (closed || !fd.valid() || useCount < 0)
                throw new SyncFailedException("Closed " + closed + " valid " + fd.valid() + " useCount " + useCount);
        }
        else
        {
            fd.sync();
        }
    }

    public static void force(FileChannel fc, boolean metaData) throws IOException
    {
        Preconditions.checkNotNull(fc);
        if (SKIP_SYNC)
        {
            if (!fc.isOpen())
                throw new ClosedChannelException();
        }
        else
        {
            fc.force(metaData);
        }
    }

    public static void sync(RandomAccessFile ras) throws IOException
    {
        Preconditions.checkNotNull(ras);
        sync(ras.getFD());
    }

    public static void sync(FileOutputStream fos) throws IOException
    {
        Preconditions.checkNotNull(fos);
        sync(fos.getFD());
    }

    public static void trySync(int fd)
    {
        if (SKIP_SYNC)
            return;
        else
            NativeLibrary.trySync(fd);
    }

    public static void trySyncDir(File dir)
    {
        if (SKIP_SYNC)
            return;

        int directoryFD = NativeLibrary.tryOpenDirectory(dir.getPath());
        try
        {
            trySync(directoryFD);
        }
        finally
        {
            NativeLibrary.tryCloseFD(directoryFD);
        }
    }
}
