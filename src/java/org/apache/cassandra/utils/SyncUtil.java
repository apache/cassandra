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
import java.nio.MappedByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;

import com.google.common.base.Preconditions;

import org.apache.cassandra.config.CassandraRelevantEnv;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.utils.memory.MemoryUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * A wrapper around various mechanisms for syncing files that makes it possible to intercept
 * and skip syncing. Useful for unit tests in certain environments where syncs can have outliers
 * bad enough to cause tests to run 10s of seconds longer.
 */
public class SyncUtil
{
    public static final boolean SKIP_SYNC;
    private static final Logger logger = LoggerFactory.getLogger(SyncUtil.class);

    static
    {
        //If skipping syncing is requested by any means then skip them.
        boolean skipSyncProperty = CassandraRelevantProperties.TEST_CASSANDRA_SKIP_SYNC.getBoolean();
        boolean skipSyncEnv = CassandraRelevantEnv.CASSANDRA_SKIP_SYNC.getBoolean();
        SKIP_SYNC = skipSyncProperty || skipSyncEnv;
        if (SKIP_SYNC)
        {
            logger.info("Skip fsync enabled due to property {} and environment {}", skipSyncProperty, skipSyncEnv);
        }
    }

    public static MappedByteBuffer force(MappedByteBuffer buf)
    {
        Preconditions.checkNotNull(buf);
        Object attachment = MemoryUtil.getAttachment(buf);
        if (attachment instanceof Runnable)
        {
            ((Runnable) attachment).run();
            return buf;
        }
        if (SKIP_SYNC)
        {
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
        if (!SKIP_SYNC)
            fd.sync();
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

    public static void sync(FileOutputStream fos) throws IOException
    {
        Preconditions.checkNotNull(fos);
        sync(fos.getFD());
    }

    public static void trySync(int fd)
    {
        if (SKIP_SYNC)
            return;

        NativeLibrary.trySync(fd);
    }

    public static void trySyncDir(File dir)
    {
        if (SKIP_SYNC)
            return;

        int directoryFD = NativeLibrary.tryOpenDirectory(dir.path());
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
