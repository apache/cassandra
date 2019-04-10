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

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.FSReadError;

public class DirectIOUtils
{
    public static final int BLOCK_SIZE;

    static
    {
        try
        {
            // BLOCK_SIZE is the max of the block sizes of the data file directories.
            int blockSize = 0;
            for (String datadir : DatabaseDescriptor.getAllDataFileLocations())
            {
                if (datadir == null)
                    throw new ConfigurationException("data_file_directories must not contain empty entry", false);

                FileStore fs = Files.getFileStore(Paths.get(datadir));

                int n = (int) fs.getBlockSize();
                if (n > blockSize)
                    blockSize = n;
            }
            BLOCK_SIZE = blockSize;
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Allocates a block aligned direct byte buffer. The size of the returned
     * buffer is the nearest multiple of BLOCK_SIZE to the requested size.
     *
     * @param size the requested size of a byte buffer.
     * @return aligned byte buffer.
     */
    public static ByteBuffer allocate(int size)
    {
        try
        {
            int n = (size + BLOCK_SIZE - 1) / BLOCK_SIZE + 1;
            return ByteBuffer.allocateDirect(n * BLOCK_SIZE).alignedSlice(BLOCK_SIZE);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Reads a sequence of bytes from the file channel into the given byte buffer.
     *
     * @param channel the channel to read from.
     * @param dst the destination byte buffer.
     * @param position the position of the channel to start reading from.
     * @return the number of bytes read.
     * @throws IOException
     */
    public static int read(FileChannel channel, ByteBuffer dst, long position) throws IOException
    {
        int lim = dst.limit();
        int r = (int) (position & (BLOCK_SIZE - 1));
        int len = lim + r;
        dst.limit((len & (BLOCK_SIZE - 1)) == 0 ? len : (len & -BLOCK_SIZE) + BLOCK_SIZE);

        int n = channel.read(dst, position & -BLOCK_SIZE);
        n -= r;
        n = n < lim ? n : lim;

        dst.position(r).limit(r + n);

        return n;
    }
}
